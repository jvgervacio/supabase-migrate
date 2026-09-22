#!/usr/bin/env python3
"""Unified migration to Supabase.

  MySQL      -> Supabase Postgres   (pgloader)
  Amazon S3  -> Supabase Storage    (boto3)

Reads every setting from one env file. Run --dry-run first: it checks
connectivity, counts what would move, and writes nothing.

  python3 migrate.py --env migration.env --dry-run
  python3 migrate.py --env migration.env
  python3 migrate.py --env migration.env --only storage
  python3 migrate.py --env migration.env --verify-only

Storage transfers resume: completed keys are recorded and skipped on a rerun.
"""

import argparse
import csv
import datetime
import json
import mimetypes
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------

DB_KEYS = ["MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE",
           "SUPABASE_DB_HOST", "SUPABASE_DB_PORT", "SUPABASE_DB_USER",
           "SUPABASE_DB_PASSWORD", "SUPABASE_DB_NAME"]

STORAGE_KEYS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "SOURCE_BUCKET",
                "SUPABASE_S3_ENDPOINT", "SUPABASE_REGION", "SUPABASE_ACCESS_KEY_ID",
                "SUPABASE_SECRET_ACCESS_KEY", "TARGET_BUCKET"]

DEFAULTS = {
    "MYSQL_HOST": "127.0.0.1", "MYSQL_PORT": "3306",
    "SUPABASE_DB_PORT": "5432", "SUPABASE_DB_NAME": "postgres",
    "AWS_REGION": "us-east-1", "SUPABASE_REGION": "us-east-1",
    "SOURCE_PREFIX": "", "TARGET_PREFIX": "", "TARGET_SCHEMA": "",
    "AWS_SESSION_TOKEN": "", "MAX_FILE_MB": "50", "WORKERS": "8",
}

SECRETS = []          # populated from the env file, masked in all output
DEBUG = False         # --debug: full tracebacks on the console
_LOG = None           # transcript file handle
_LOG_PATH = None
_LOG_LOCK = threading.Lock()


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def mask(text):
    for s in SECRETS:
        if s and len(s) > 3:
            text = text.replace(s, "***")
    return text


def open_log(path):
    """Tee everything to a file so a failed run can be debugged afterwards.
    say() masks before writing, so no secret reaches the transcript."""
    global _LOG, _LOG_PATH
    _LOG_PATH = path
    _LOG = open(path, "a", encoding="utf-8")
    _LOG.write(f"\n{'=' * 68}\nrun {datetime.datetime.now().isoformat(timespec='seconds')}"
               f"  argv: {' '.join(sys.argv[1:])}\n{'=' * 68}\n")
    _LOG.flush()


def say(msg=""):
    text = mask(str(msg))
    print(text, flush=True)
    # Logging must never break reporting -- a closed handle or a full disk
    # should not swallow the error we are in the middle of printing.
    if _LOG:
        try:
            with _LOG_LOCK:
                _LOG.write(text + "\n")
                _LOG.flush()
        except (ValueError, OSError):
            pass


def head(title):
    say("\n" + "=" * 68)
    say(title)
    say("=" * 68)


def human(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024 or unit == "PB":
            return f"{n:,.0f} B" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024.0


def duration(secs):
    secs = int(max(secs, 0))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else (f"{m}m{s:02d}s" if m else f"{s}s")


class Progress:
    """Ticks on a timer, not a counter, so a 12-file run shows movement and a
    12-million-file run does not flood the log."""

    def __init__(self, total_items, total_bytes, every=2.0):
        self.t0 = self.last = time.time()
        self.total_items = total_items
        self.total_bytes = total_bytes
        self.every = every

    def tick(self, items, done_bytes, extra="", force=False):
        now = time.time()
        if not force and now - self.last < self.every:
            return
        self.last = now
        el = now - self.t0
        pct = 100.0 * items / self.total_items if self.total_items else 0.0
        rate = done_bytes / el / 1024 / 1024 if el > 0 else 0.0
        eta = ""
        if done_bytes > 0 and self.total_bytes > 0 and el > 0:
            eta = f"  eta {duration((self.total_bytes - done_bytes) / (done_bytes / el))}"
        say(f"    {items:>7,}/{self.total_items:,} {pct:5.1f}%  "
            f"{human(done_bytes):>10}  {rate:6.1f} MB/s{eta}  {extra}")


class Fatal(Exception):
    pass


# --------------------------------------------------------------------------
# Env file
# --------------------------------------------------------------------------

def load_env(path):
    """Parse KEY=VALUE lines. Supports # comments, blank lines and 'export '."""
    if not os.path.exists(path):
        raise Fatal(f"env file not found: {path}")

    env = dict(DEFAULTS)
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                raise Fatal(f"{path}:{lineno}: expected KEY=VALUE, got {raw.strip()!r}")
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            # Strip one matching pair of surrounding quotes.
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            env[key] = val

    for k in ("MYSQL_PASSWORD", "SUPABASE_DB_PASSWORD", "AWS_SECRET_ACCESS_KEY",
              "SUPABASE_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if env.get(k) and env[k] not in SECRETS:
            SECRETS.append(env[k])
    return env


# Empty is legitimate for these -- a passwordless MySQL user, no prefix, etc.
MAY_BE_EMPTY = {"MYSQL_PASSWORD", "SOURCE_PREFIX", "TARGET_PREFIX",
                "TARGET_SCHEMA", "AWS_SESSION_TOKEN"}


def require(env, keys, what):
    missing = [k for k in keys if not env.get(k) and k not in MAY_BE_EMPTY]
    todo = [k for k in keys if "<" in env.get(k, "")]
    if missing:
        raise Fatal(f"{what}: missing from the env file: {', '.join(missing)}")
    if todo:
        raise Fatal(f"{what}: still template placeholders: {', '.join(todo)}")


def as_num(env, key, cast=int):
    """Numeric setting or a clean error -- not a traceback from int('abc')."""
    raw = str(env.get(key, "")).strip()
    try:
        return cast(raw)
    except (TypeError, ValueError):
        raise Fatal(f"{key} must be a number, got {raw!r}")


def port_open(host, port, timeout=6):
    """(ok, reason). Fails fast instead of waiting out a 2 minute TCP timeout."""
    try:
        socket.create_connection((host, int(port)), timeout=timeout).close()
        return True, "open"
    except socket.timeout:
        return False, "timed out -- firewall is dropping packets"
    except ConnectionRefusedError:
        return False, "refused -- nothing listening on that port"
    except socket.gaierror as e:
        return False, f"DNS lookup failed ({e})"
    except OSError as e:
        return False, f"{type(e).__name__}: {e}"


# ==========================================================================
# Part 1 -- MySQL to Supabase Postgres
# ==========================================================================

def build_load_file(env, path):
    """Write a pgloader command file. Passwords are percent-encoded here, so
    a '@' or '/' in a password does not corrupt the connection URL."""
    my = "mysql://{u}:{p}@{h}:{port}/{db}".format(
        u=quote(env["MYSQL_USER"], safe=""), p=quote(env["MYSQL_PASSWORD"], safe=""),
        h=env["MYSQL_HOST"], port=env["MYSQL_PORT"], db=env["MYSQL_DATABASE"])
    pg = "postgresql://{u}:{p}@{h}:{port}/{db}".format(
        u=quote(env["SUPABASE_DB_USER"], safe=""),
        p=quote(env["SUPABASE_DB_PASSWORD"], safe=""),
        h=env["SUPABASE_DB_HOST"], port=env["SUPABASE_DB_PORT"],
        db=env["SUPABASE_DB_NAME"])

    schema = env.get("TARGET_SCHEMA", "").strip()
    if schema and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise Fatal(f"TARGET_SCHEMA must be a plain identifier, got {schema!r}")

    body = [
        "LOAD DATABASE",
        f"     FROM {my}",
        f"     INTO {pg}",
        "",
        "WITH create tables, create indexes, reset sequences, foreign keys",
        "",
        "SET work_mem to '256MB', statement_timeout to '0'",
        "",
    ]
    if schema:
        body += [f"ALTER TABLE NAMES MATCHING ~/.*/ SET SCHEMA '{schema}'", "",
                 "BEFORE LOAD DO", f'$$ CREATE SCHEMA IF NOT EXISTS "{schema}"; $$', ""]
    body.append(";")

    text = "\n".join(body) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(path, 0o600)
    return text


def pgloader_cmd(state_dir, docker_image=None, binary="pgloader"):
    """A pgloader binary (named, or on PATH), or a Docker run with the state
    dir mounted at /data.

    --network host is what lets 127.0.0.1:3306 inside the container reach
    MySQL on the host."""
    load_path = os.path.join(state_dir, "migrate.load")
    if not docker_image:
        return [binary, load_path]
    return ["docker", "run", "--rm", "--network", "host",
            "-v", f"{os.path.abspath(state_dir)}:/data",
            docker_image, "pgloader", "/data/migrate.load"]


def preflight_db(env, docker_image=None, binary="pgloader"):
    ok = True

    host, port = env["MYSQL_HOST"], env["MYSQL_PORT"]
    good, why = port_open(host, port)
    say(f"  mysql {host}:{port} -- {why}")
    if not good:
        ok = False
        if host not in ("127.0.0.1", "localhost", "::1"):
            say(f"      {host} is remote. MySQL usually binds to 127.0.0.1 and the")
            say("      firewall blocks 3306. Run this script ON the database server")
            say("      with MYSQL_HOST=127.0.0.1, or open an SSH tunnel first.")

    host, port = env["SUPABASE_DB_HOST"], env["SUPABASE_DB_PORT"]
    good, why = port_open(host, port)
    say(f"  supabase postgres {host}:{port} -- {why}")
    if not good:
        ok = False
    if str(port) == "6543":
        ok = False
        say("      Port 6543 is transaction mode: no session SET, no prepared")
        say("      statements. pgloader needs both. Use SUPABASE_DB_PORT=5432.")

    if docker_image:
        if not shutil.which("docker"):
            ok = False
            say("  docker -- not installed, but --pgloader-docker was requested")
        else:
            say(f"  pgloader -- via Docker image {docker_image}")
    elif not (shutil.which(binary) or os.path.isfile(binary)):
        ok = False
        say(f"  pgloader -- '{binary}' not found. Either install it, build it")
        say("      from source and pass --pgloader <path>, or use --pgloader-docker.")
    else:
        try:
            ver = subprocess.run([binary, "--version"], capture_output=True, text=True)
            text = (ver.stdout or "") + (ver.stderr or "")
        except OSError as e:
            ok = False
            say(f"  pgloader -- cannot run '{binary}': {e}")
            text = ""
        if text.strip():
            say(f"  pgloader -- {text.strip().splitlines()[0]}  [{binary}]")
        # 3.6.1/3.6.2 ship a Postgres library predating SCRAM-SHA-256, which
        # Supabase requires. It fails late and cryptically, so flag it now.
        m = re.search(r'"(\d+)\.(\d+)\.(\d+)"', text)
        if m and tuple(int(g) for g in m.groups()) < (3, 6, 3):
            ok = False
            say("      This version predates SCRAM-SHA-256 and WILL fail against")
            say("      Supabase. Build 3.6.9 from source and pass --pgloader <path>,")
            say("      or use --pgloader-docker.")

    return ok


def migrate_db(env, state_dir, dry_run, docker_image=None, binary="pgloader"):
    head("DATABASE  MySQL -> Supabase Postgres")
    require(env, DB_KEYS, "database")

    if not preflight_db(env, docker_image, binary):
        raise Fatal("database preflight failed -- fix the errors above")

    load_path = os.path.join(state_dir, "migrate.load")
    text = build_load_file(env, load_path)
    say("\n" + mask(text))

    if dry_run:
        say(f"DRY RUN -- command file written to {load_path}, pgloader not run.")
        return True

    # Print the real command: it is the only unambiguous signal of whether the
    # local binary or the Docker image is about to run.
    say(f"Running: {' '.join(pgloader_cmd(state_dir, docker_image, binary))}")
    say("-- output streams live below.\n")
    t0 = time.time()
    lines = []
    cmd = pgloader_cmd(state_dir, docker_image, binary)
    try:
        # Streamed, not captured: a long migration must show progress as it
        # happens rather than printing everything once it is over.
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, bufsize=1)
        except FileNotFoundError:
            raise Fatal(f"{cmd[0]} is not installed or not on PATH. Install it with:\n"
                        "    sudo apt-get update && sudo apt-get install -y pgloader\n"
                        "or rerun with --pgloader-docker to use the official image.")
        for line in proc.stdout:
            line = line.rstrip("\n")
            lines.append(line)
            say(f"  | {line}")
        proc.wait()
    finally:
        # The file holds both passwords in plaintext -- never leave it behind.
        try:
            os.remove(load_path)
        except OSError:
            pass

    out = "\n".join(lines)
    say(f"\npgloader exit {proc.returncode} after {duration(time.time() - t0)}")

    # pgloader exits 0 in cases where it clearly did not migrate: a source it
    # could not reach, or a run whose summary reports per-table errors.
    errors = len(re.findall(r"^\S+\s+ERROR", out, re.M))
    unreachable = "Failed to connect" in out
    failed = proc.returncode != 0 or unreachable or errors > 0

    if "fell through ECASE" in out:
        # Postgres auth type 10 is SCRAM-SHA-256. pgloader 3.6.1/3.6.2 handle
        # only (0 2 3 4 5 6 7 8), and Supabase requires SCRAM.
        say("  This pgloader is too old for Supabase. That ECASE error is Postgres")
        say("  auth type 10 -- SCRAM-SHA-256 -- which its bundled library cannot do.")
        say("  Ubuntu ships 3.6.1/3.6.2; SCRAM needs a newer build. Rerun with:")
        say("      python3 migrate.py --env <envfile> --only db --pgloader-docker")
    elif unreachable:
        say("  pgloader could not reach one of the databases.")
    if errors:
        say(f"  {errors} ERROR line(s) in the pgloader log -- review them above.")
    say("Database migration FAILED." if failed else "Database migration finished.")
    return not failed


# ==========================================================================
# Part 2 -- Amazon S3 to Supabase Storage (boto3 only)
# ==========================================================================

def s3_clients(env):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise Fatal("boto3 is not installed. Install it with:\n"
                    "    python3 -m pip install --upgrade boto3 botocore")

    akid = env["AWS_ACCESS_KEY_ID"]
    asec = env["AWS_SECRET_ACCESS_KEY"]
    sb_id = env["SUPABASE_ACCESS_KEY_ID"]
    sb_sec = env["SUPABASE_SECRET_ACCESS_KEY"]
    bad_keys = []          # names of credentials whose shape is already wrong

    for name, val, size in [("AWS_ACCESS_KEY_ID", akid, 20),
                            ("AWS_SECRET_ACCESS_KEY", asec, 40),
                            ("SUPABASE_ACCESS_KEY_ID", sb_id, 32),
                            ("SUPABASE_SECRET_ACCESS_KEY", sb_sec, 64)]:
        if len(val) != size:
            say(f"  warn: {name} is {len(val)} chars, expected {size}")
            bad_keys.append(name)

    # Copying from the Supabase dashboard easily grabs both fields at once and
    # leaves the key id stuck on the end of the secret. It fails later as an
    # opaque SignatureDoesNotMatch, so name it here instead.
    for sec_name, sec, id_name, kid in [
            ("SUPABASE_SECRET_ACCESS_KEY", sb_sec, "SUPABASE_ACCESS_KEY_ID", sb_id),
            ("AWS_SECRET_ACCESS_KEY", asec, "AWS_ACCESS_KEY_ID", akid)]:
        if kid and len(sec) > len(kid) and sec.endswith(kid):
            say(f"  warn: {sec_name} ends with the whole of {id_name} -- the key id")
            say(f"        was appended during the paste. Keep only the first "
                f"{len(sec) - len(kid)} characters.")

    if akid.startswith("ASIA") and not env.get("AWS_SESSION_TOKEN"):
        say("  warn: ASIA... is a temporary key -- AWS_SESSION_TOKEN is also needed")

    src = boto3.client(
        "s3", aws_access_key_id=akid, aws_secret_access_key=asec,
        aws_session_token=env.get("AWS_SESSION_TOKEN") or None,
        region_name=env["AWS_REGION"],
        config=Config(signature_version="s3v4",
                      retries={"max_attempts": 10, "mode": "adaptive"},
                      max_pool_connections=32))

    # path addressing: Supabase serves no virtual-hosted buckets.
    # when_required: botocore >=1.36 adds a CRC32 header Supabase rejects.
    base = dict(signature_version="s3v4", s3={"addressing_style": "path"},
                retries={"max_attempts": 10, "mode": "adaptive"},
                max_pool_connections=32)
    try:
        cfg = Config(request_checksum_calculation="when_required",
                     response_checksum_validation="when_required", **base)
    except TypeError:
        cfg = Config(**base)

    dst = boto3.client(
        "s3", endpoint_url=env["SUPABASE_S3_ENDPOINT"],
        aws_access_key_id=env["SUPABASE_ACCESS_KEY_ID"],
        aws_secret_access_key=env["SUPABASE_SECRET_ACCESS_KEY"],
        region_name=env["SUPABASE_REGION"], config=cfg)
    return src, dst, bad_keys


def explain_s3_error(env, e):
    """Turn a botocore ClientError into the specific thing that is wrong."""
    resp = getattr(e, "response", {}) or {}
    code = resp.get("Error", {}).get("Code", "")
    region = resp.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-bucket-region")
    akid = env["AWS_ACCESS_KEY_ID"]

    if code == "SignatureDoesNotMatch":
        return ("AWS_SECRET_ACCESS_KEY does not match AWS_ACCESS_KEY_ID. This is the\n"
                "      secret, not permissions -- an IAM policy will not help.\n"
                f"      Key id {akid[:4]}...{akid[-4:]} ({len(akid)} chars), "
                f"secret {len(env['AWS_SECRET_ACCESS_KEY'])} chars (expected 40).")
    if code in ("InvalidAccessKeyId", "InvalidClientTokenId"):
        return "AWS_ACCESS_KEY_ID does not exist -- deleted, deactivated, or a typo."
    if code in ("ExpiredToken", "ExpiredTokenException", "InvalidToken"):
        return "Temporary credentials expired -- refresh them and set AWS_SESSION_TOKEN."
    if code == "NoSuchBucket":
        return f"Bucket '{env['SOURCE_BUCKET']}' does not exist in this account."
    if region and region != env["AWS_REGION"]:
        return f"Bucket is in {region}, not {env['AWS_REGION']} -- fix AWS_REGION."
    if code in ("AccessDenied", "403", "AllAccessDisabled"):
        return ("The key is valid but lacks s3:ListBucket / s3:GetObject on this\n"
                "      bucket. Attach an IAM policy granting both:\n"
                f"        s3:ListBucket on arn:aws:s3:::{env['SOURCE_BUCKET']}\n"
                f"        s3:GetObject  on arn:aws:s3:::{env['SOURCE_BUCKET']}/*")
    return f"Unexpected error code {code!r}."


def dest_key(env, key):
    k = key.lstrip("/")
    prefix = env.get("SOURCE_PREFIX", "").lstrip("/")
    if prefix and k.startswith(prefix):
        k = k[len(prefix):].lstrip("/")
    target = env.get("TARGET_PREFIX", "").strip().strip("/")
    return f"{target}/{k}" if target else k


def preflight_storage(env, src, dst, bad_keys=()):
    from botocore.exceptions import ClientError, EndpointConnectionError
    ok = True
    prefix = env.get("SOURCE_PREFIX", "").lstrip("/")
    try:
        probe = src.list_objects_v2(Bucket=env["SOURCE_BUCKET"], Prefix=prefix, MaxKeys=1)
        say(f"  s3://{env['SOURCE_BUCKET']}/{prefix} -- can list")
        keys = [o["Key"] for o in probe.get("Contents", [])]
        if keys:
            src.head_object(Bucket=env["SOURCE_BUCKET"], Key=keys[0])
            say("  s3 objects -- can read")
        else:
            say("  warn: no objects under that prefix")
    except (ClientError, EndpointConnectionError) as e:
        ok = False
        say(f"  FAIL source: {e}")
        say(f"      {explain_s3_error(env, e)}")

    try:
        dst.list_buckets()
        say(f"  {env['SUPABASE_S3_ENDPOINT']} -- connected")
    except (ClientError, EndpointConnectionError) as e:
        ok = False
        say(f"  FAIL destination: {e}")
        code = (getattr(e, "response", {}) or {}).get("Error", {}).get("Code", "")
        if bad_keys:
            # The shape warnings above are almost certainly the cause -- say so
            # rather than making the reader connect two separate messages.
            say(f"      Fix the key warning(s) above first: {', '.join(bad_keys)}.")
            say("      A malformed secret cannot produce a valid signature, so this")
            say("      failure is expected until the key is corrected.")
        elif not code:
            # Supabase answers a bad signature with a non-S3 body, which botocore
            # surfaces as an empty code rather than SignatureDoesNotMatch.
            say("      Empty error code: the endpoint returned something that is not")
            say("      an S3 error. Usually a rejected signature -- re-copy")
            say("      SUPABASE_ACCESS_KEY_ID (32 chars) and")
            say("      SUPABASE_SECRET_ACCESS_KEY (64 chars) as separate fields.")
        else:
            say("      Check the endpoint ends in /storage/v1/s3, the region matches")
            say("      the project, and the S3 access keys are current.")
    return ok


def inventory(env, src, manifest_path):
    """List everything that would move. Metadata only -- no object is downloaded."""
    max_bytes = int(as_num(env, "MAX_FILE_MB", float) * 1024 * 1024)
    cold_classes = {"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"}
    prefix = env.get("SOURCE_PREFIX", "").lstrip("/")
    EXAMPLES = 5          # blocker examples kept; counts stay exact

    count, total, markers = 0, 0, 0
    by_class, hist = Counter(), defaultdict(int)
    oversize, cold = [], []
    n_oversize = n_cold = 0

    say(f"  listing s3://{env['SOURCE_BUCKET']}/{prefix} ...")
    last_tick = time.time()
    # Rows stream straight to the manifest; a million-object bucket would not
    # fit in memory if they were accumulated first.
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["key", "dest_key", "size", "storage_class"])
        writer.writeheader()

        for page in src.get_paginator("list_objects_v2").paginate(
                Bucket=env["SOURCE_BUCKET"], Prefix=prefix):
            for obj in page.get("Contents", []):
                key, size = obj["Key"], obj["Size"]
                cls = obj.get("StorageClass", "STANDARD")
                if key.endswith("/") and size == 0:      # console folder marker
                    markers += 1
                    continue
                count += 1
                total += size
                by_class[cls] += 1
                for label, limit in [("0 B", 1), ("< 1 MB", 1024 ** 2),
                                     ("1-6 MB", 6 * 1024 ** 2),
                                     ("6-50 MB", 50 * 1024 ** 2),
                                     ("50 MB-1 GB", 1024 ** 3)]:
                    if size < limit:
                        hist[label] += 1
                        break
                else:
                    hist["> 1 GB"] += 1
                if size > max_bytes:
                    n_oversize += 1
                    if len(oversize) < EXAMPLES:
                        oversize.append((key, size))
                if cls in cold_classes:
                    n_cold += 1
                    if len(cold) < EXAMPLES:
                        cold.append((key, cls))
                writer.writerow({"key": key, "dest_key": dest_key(env, key),
                                 "size": size, "storage_class": cls})
                if count % 1000 == 0 and time.time() - last_tick > 2.0:
                    last_tick = time.time()
                    say(f"    ... {count:,} objects, {human(total)} so far")

    say(f"\n  files to move   {count:,}")
    say(f"  total size      {human(total)}")
    say(f"  folder markers  {markers:,} skipped")
    if hist:
        say("\n  size distribution")
        for label in ["0 B", "< 1 MB", "1-6 MB", "6-50 MB", "50 MB-1 GB", "> 1 GB"]:
            if hist[label]:
                say(f"    {label:<14} {hist[label]:>9,}")
    if len(by_class) > 1:
        say("\n  storage class")
        for cls, cnt in by_class.most_common():
            say(f"    {cls:<14} {cnt:>9,}")

    say("\n  blockers")
    if n_oversize:
        say(f"    {n_oversize:,} file(s) over {env['MAX_FILE_MB']} MB will FAIL:")
        for key, size in sorted(oversize, key=lambda x: -x[1]):
            say(f"      {human(size):>11}  {key[:56]}")
        say("      Raise the bucket file size limit in Storage > Buckets.")
    else:
        say(f"    none over {env['MAX_FILE_MB']} MB")
    if n_cold:
        say(f"    {n_cold:,} file(s) in Glacier -- restore in S3 first:")
        for key, cls in cold:
            say(f"      {cls:<13}  {key[:56]}")
    else:
        say("    none archived")

    say(f"\n  manifest -> {manifest_path} ({count:,} rows)")
    return count


def ensure_bucket(env, dst):
    names = [b["Name"] for b in dst.list_buckets().get("Buckets", [])]
    if env["TARGET_BUCKET"] in names:
        say(f"  bucket '{env['TARGET_BUCKET']}' exists")
    else:
        dst.create_bucket(Bucket=env["TARGET_BUCKET"])
        say(f"  created private bucket '{env['TARGET_BUCKET']}'")
        say("  make it public under Storage > Buckets if the files need public reads")


def copy_objects(env, src, dst, manifest_path, state_path, workers):
    from boto3.s3.transfer import TransferConfig

    cfg = TransferConfig(multipart_threshold=8 * 1024 * 1024,
                         multipart_chunksize=16 * 1024 * 1024, max_concurrency=2)

    done = set()
    if os.path.exists(state_path):
        with open(state_path, encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["key"])
                except Exception:
                    pass

    with open(manifest_path, encoding="utf-8") as f:
        plan = list(csv.DictReader(f))
    pending = [r for r in plan if r["key"] not in done]
    say(f"  planned {len(plan):,} | done {len(done):,} | pending {len(pending):,} "
        f"({human(sum(int(r['size']) for r in pending))})")
    if not pending:
        return 0

    lock = threading.Lock()
    counts = {"copied": 0, "skipped": 0, "failed": 0, "bytes": 0}
    failures = []
    bucket, target = env["SOURCE_BUCKET"], env["TARGET_BUCKET"]

    # Held open: reopening per object serialises every worker on a file open.
    statefile = open(state_path, "a", encoding="utf-8")

    def one(row):
        key, dkey, size = row["key"], row["dest_key"], int(row["size"])
        try:
            if dst.head_object(Bucket=target, Key=dkey)["ContentLength"] == size:
                with lock:
                    counts["skipped"] += 1
                return
        except Exception:
            pass          # absent, or the check failed -- either way, copy it

        ctype = mimetypes.guess_type(key)[0] or "application/octet-stream"
        for attempt in range(1, 4):
            try:
                body = src.get_object(Bucket=bucket, Key=key)["Body"]
                try:
                    dst.upload_fileobj(body, target, dkey,
                                       ExtraArgs={"ContentType": ctype}, Config=cfg)
                finally:
                    body.close()
                with lock:
                    counts["copied"] += 1
                    counts["bytes"] += size
                    statefile.write(json.dumps({"key": key, "size": size}) + "\n")
                    statefile.flush()      # survive a kill -9 mid-run
                return
            except Exception as e:
                if attempt == 3:
                    with lock:
                        counts["failed"] += 1
                        failures.append((key, repr(e), traceback.format_exc()))
                    if DEBUG:
                        say(f"    FAILED {key}\n{traceback.format_exc()}")
                else:
                    time.sleep(2 ** attempt)

    pending_bytes = sum(int(r["size"]) for r in pending)
    prog = Progress(len(pending), pending_bytes)
    say(f"  copying with {workers} workers ...")
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(one, r) for r in pending]
            for i, fut in enumerate(as_completed(futures), 1):
                # one() never raises, but a worker dying should not lose the run.
                try:
                    fut.result()
                except Exception as e:
                    with lock:
                        counts["failed"] += 1
                        failures.append(("<worker>", repr(e), traceback.format_exc()))
                prog.tick(i, counts["bytes"],
                          extra=f"ok={counts['copied']:,} skip={counts['skipped']:,} "
                                f"fail={counts['failed']:,}",
                          force=(i == len(futures)))
    finally:
        statefile.close()

    say(f"\n  copied {counts['copied']:,} ({human(counts['bytes'])})  "
        f"skipped {counts['skipped']:,}  failed {counts['failed']:,}")

    if failures:
        fail_log = os.path.join(os.path.dirname(state_path) or ".", "failures.log")
        with open(fail_log, "w", encoding="utf-8") as fh:
            for key, err, tb in failures:
                fh.write(f"--- {key}\n{mask(tb)}\n")
        say(f"\n  first {min(5, len(failures))} of {len(failures):,} failure(s):")
        for key, err, _ in failures[:5]:
            say(f"    {key[:60]}")
            say(f"      {err[:110]}")
        say(f"\n  full tracebacks -> {fail_log}")
        say(f"  rerun to retry the {counts['failed']:,} failure(s); "
            f"add --debug to see stacks live")
    return counts["failed"]


def verify_storage(env, src, dst):
    def listing(client, bucket, prefix):
        out = {}
        for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if not (o["Key"].endswith("/") and o["Size"] == 0):
                    out[o["Key"]] = o["Size"]
        return out

    source = listing(src, env["SOURCE_BUCKET"], env.get("SOURCE_PREFIX", "").lstrip("/"))
    target = listing(dst, env["TARGET_BUCKET"], env.get("TARGET_PREFIX", "").strip().strip("/"))
    expected = {dest_key(env, k): v for k, v in source.items()}

    missing = {k: v for k, v in expected.items() if k not in target}
    wrong = {k: (v, target[k]) for k, v in expected.items()
             if k in target and target[k] != v}

    say(f"  source       {len(source):,} files   {human(sum(source.values()))}")
    say(f"  destination  {len(target):,} files   {human(sum(target.values()))}")
    say(f"  missing      {len(missing):,}")
    say(f"  wrong size   {len(wrong):,}")
    for k in list(missing)[:10]:
        say(f"    missing  {human(missing[k]):>10}  {k[:50]}")
    for k, (a, b) in list(wrong.items())[:10]:
        say(f"    size     {human(a):>10} != {human(b):>10}  {k[:40]}")

    clean = not missing and not wrong
    say("  all files present with matching sizes" if clean
        else "  rerun without --verify-only to fill the gaps")
    return clean


def migrate_storage(env, state_dir, dry_run, verify_only, workers):
    head("STORAGE  Amazon S3 -> Supabase Storage")
    require(env, STORAGE_KEYS, "storage")

    src, dst, bad_keys = s3_clients(env)
    if not preflight_storage(env, src, dst, bad_keys):
        raise Fatal("storage preflight failed -- fix the errors above")

    if verify_only:
        say("")
        return verify_storage(env, src, dst)

    manifest = os.path.join(state_dir, "s3_inventory.csv")
    state = os.path.join(state_dir, "s3_migrated.jsonl")

    say("")
    count = inventory(env, src, manifest)
    if not count:
        say("\n  nothing to migrate")
        return True
    if dry_run:
        say("\nDRY RUN -- no bucket created, nothing uploaded.")
        return True

    say("")
    ensure_bucket(env, dst)
    say("")
    failed = copy_objects(env, src, dst, manifest, state, workers)
    say("")
    clean = verify_storage(env, src, dst)
    return failed == 0 and clean


# ==========================================================================

def main():
    ap = argparse.ArgumentParser(
        prog="migrate.py",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Migrate into Supabase, driven by one env file.\n\n"
            "  MySQL      ->  Supabase Postgres   (via pgloader)\n"
            "  Amazon S3  ->  Supabase Storage    (via boto3)\n\n"
            "Both sources are only ever read. Nothing is deleted, anywhere."),
        epilog="""\
examples:
  # always start here: checks both connections, counts what would move,
  # and writes nothing to either destination
  migrate.py --env migration.env --dry-run

  # run one half at a time
  migrate.py --env migration.env --only storage
  migrate.py --env migration.env --only db

  # run both, then confirm every file arrived
  migrate.py --env migration.env
  migrate.py --env migration.env --verify-only

  # Ubuntu 20.04: its pgloader is too old for Supabase's SCRAM auth
  migrate.py --env migration.env --only db --pgloader-docker

  # a failed run: retry, watching stacks as they happen
  migrate.py --env migration.env --only storage --debug

resuming:
  Every completed storage key is recorded, so a rerun skips what is already
  done and retries only what failed. Ctrl-C is safe -- progress is kept.

files written to --state-dir:
  migrate.log        full transcript of every run, secrets masked
  s3_inventory.csv   every object that will move, written by --dry-run onward
  s3_migrated.jsonl  resume log
  failures.log       full tracebacks, written only if a transfer fails

exit codes:
  0  success                     1  finished, but with failures
  2  bad settings or preflight   3  unexpected error (a bug)
  130 interrupted

settings:
  Every value lives in the env file -- see migration.env.example for the
  template and what each key means.""")

    ap.add_argument("--env", required=True, metavar="FILE",
                    help="env file holding every setting.\n"
                         "Copy migration.env.example and fill it in.")
    ap.add_argument("--only", choices=["db", "storage", "all"], default="all",
                    help="which half to run (default: all)\n"
                         "  db       MySQL      ->  Supabase Postgres\n"
                         "  storage  Amazon S3  ->  Supabase Storage\n"
                         "  all      both, one after the other")
    ap.add_argument("--dry-run", action="store_true",
                    help="check both connections and count every file, then\n"
                         "stop. Creates no bucket, uploads nothing, runs no\n"
                         "pgloader. Run this first.")
    ap.add_argument("--verify-only", action="store_true",
                    help="skip the transfer; just compare S3 against Supabase\n"
                         "Storage and report anything missing or the wrong\n"
                         "size. Storage only -- not valid with --only db.")
    ap.add_argument("--state-dir", default="migration_state", metavar="DIR",
                    help="where the transcript, manifest, resume log and\n"
                         "failure log are kept (default: migration_state)")
    ap.add_argument("--workers", type=int, metavar="N",
                    help="parallel storage transfers, overriding WORKERS in\n"
                         "the env file (default: 8). Lower it if Supabase\n"
                         "starts returning 429.")
    ap.add_argument("--pgloader", default="pgloader", metavar="PATH",
                    help="path to the pgloader binary (default: pgloader on\n"
                         "PATH). Use this after building 3.6.9 from source:\n"
                         "  --pgloader ~/pgloader-3.6.9/build/bin/pgloader")
    ap.add_argument("--pgloader-docker", nargs="?", const="dimitri/pgloader:latest",
                    metavar="IMAGE",
                    help="run pgloader from a Docker image instead of the local\n"
                         "binary (default image: dimitri/pgloader:latest).\n"
                         "Needed on Ubuntu 20.04, whose pgloader 3.6.1/3.6.2\n"
                         "cannot do the SCRAM-SHA-256 auth Supabase requires.")
    ap.add_argument("--debug", action="store_true",
                    help="print full tracebacks to the console as failures\n"
                         "happen, not only to failures.log")
    args = ap.parse_args()

    global DEBUG
    DEBUG = args.debug

    try:
        if args.verify_only and args.only == "db":
            raise Fatal("--verify-only applies to storage; --only db has nothing to verify")

        env = load_env(args.env)
        os.makedirs(args.state_dir, exist_ok=True)
        open_log(os.path.join(args.state_dir, "migrate.log"))
        workers = args.workers or as_num(env, "WORKERS")
        if workers < 1:
            raise Fatal(f"WORKERS must be at least 1, got {workers}")

        mode = "VERIFY" if args.verify_only else ("DRY RUN" if args.dry_run else "LIVE")
        head(f"SUPABASE MIGRATION  [{mode}]")
        say(f"env file   {args.env}")
        say(f"state dir  {args.state_dir}")
        say(f"scope      {args.only}")

        results, fatals = {}, []

        def phase(name, fn):
            """Run one half. A Fatal here fails only this half: with --only all,
            a missing pgloader must not stop the storage transfer from running.
            It is still recorded, so the run exits 2 (bad settings / preflight)
            rather than 1 (ran, with failures)."""
            try:
                results[name] = fn()
            except Fatal as e:
                results[name] = False
                fatals.append(name)
                say(f"\nERROR ({name}): {e}")
                if DEBUG:
                    say(traceback.format_exc())
        if args.only in ("db", "all") and not args.verify_only:
            phase("database",
                  lambda: migrate_db(env, args.state_dir, args.dry_run,
                                     args.pgloader_docker, args.pgloader))
        if args.only in ("storage", "all"):
            phase("storage",
                  lambda: migrate_storage(env, args.state_dir, args.dry_run,
                                          args.verify_only, workers))

        head("SUMMARY")
        if not results:
            say("  nothing ran -- check --only and --verify-only")
            return 1
        for name, good in results.items():
            say(f"  {name:<10} {'ok' if good else 'FAILED'}")
        if args.dry_run:
            say("\nDry run only -- nothing was written. Rerun without --dry-run.")
        if _LOG_PATH:
            say(f"\nFull transcript: {_LOG_PATH}")
        if fatals:
            return 2          # settings or preflight problem, per --help
        return 0 if all(results.values()) else 1

    except Fatal as e:
        # Expected, actionable problems: the message is the whole story.
        say(f"\nERROR: {e}")
        if DEBUG:
            say(traceback.format_exc())
        return 2
    except KeyboardInterrupt:
        say("\nInterrupted. Storage progress is saved; rerun to resume.")
        return 130
    except Exception:
        # A bug, not a configuration problem -- show the stack and keep it.
        say("\nUNEXPECTED ERROR -- this is a bug, not a settings problem.")
        say(mask(traceback.format_exc()))
        if _LOG_PATH:
            say(f"Stack also written to {_LOG_PATH}")
        return 3
    finally:
        global _LOG
        if _LOG:
            try:
                _LOG.close()
            except OSError:
                pass
            _LOG = None          # never leave a closed handle behind


if __name__ == "__main__":
    sys.exit(main())
