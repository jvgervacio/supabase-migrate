# supabase-migrate

Move a MySQL database and an Amazon S3 bucket into Supabase, driven by a single
env file.

```
MySQL      ->  Supabase Postgres   (pgloader)
Amazon S3  ->  Supabase Storage    (boto3)
```

Run either half or both. **Both sources are only ever read, and nothing is
deleted anywhere** — the tool copies, verifies, and stops.

---

## Why this exists

Both migrations are individually straightforward and individually full of traps.
This script front-loads the traps into a preflight check so they surface in
seconds rather than partway through a transfer:

- pgloader fails against Supabase's **port 6543** (transaction mode has no
  session `SET` and no prepared statements). Preflight rejects it and tells you
  to use 5432.
- MySQL on a managed host binds to `127.0.0.1` and the firewall drops 3306, so a
  remote connection hangs for two minutes before failing. Preflight probes the
  port with a 6-second timeout and explains the result.
- botocore ≥ 1.36 attaches a CRC32 header that Supabase Storage rejects; the S3
  client disables it.
- Supabase Storage is path-addressed, so a default boto3 client reports
  `NoSuchBucket` for a bucket that plainly exists.
- A password containing `@`, `:` or `/` corrupts a pgloader connection URL.
  Passwords are percent-encoded automatically — paste them raw.
- Copying Supabase's S3 keys easily appends the access key id to the secret.
  The key-shape check names that specific mistake instead of letting it fail
  later as an opaque `SignatureDoesNotMatch`.

---

## Requirements

- **Python 3.6+** — everything else the script uses is standard library
- **`boto3`** — storage half only:

  ```bash
  pip3 install -r requirements.txt
  ```

- **`pgloader`** — database half only, a system package rather than a pip install:

  ```bash
  sudo apt-get install -y pgloader
  ```

  Ubuntu 20.04 ships 3.6.2, which does not support MySQL 8's default
  `utf8mb4_0900_ai_ci` collation. Check with
  `mysql -e "SELECT @@collation_database"`; if that is what you have, install a
  newer build from `apt.postgresql.org` instead.

Run it **on the database server**. MySQL usually accepts connections only from
`127.0.0.1`, which sidesteps the firewall, the `bind-address` setting and the
user grant in one move.

---

## Usage

```bash
cp migration.env.example migration.env
chmod 600 migration.env
nano migration.env          # replace every <...> value
```

Always start with a dry run. It checks both connections, counts every object and
writes nothing:

```bash
python3 migrate.py --env migration.env --dry-run
```

Then run it:

```bash
python3 migrate.py --env migration.env              # both halves
python3 migrate.py --env migration.env --only db
python3 migrate.py --env migration.env --only storage
python3 migrate.py --env migration.env --verify-only
```

Use `tmux` for the real run so an SSH drop doesn't kill it:

```bash
tmux new -s migrate 'python3 migrate.py --env migration.env'
```

`--help` documents every flag, the exit codes and the files written.

---

## What each step does

| Step | Action | Writes? |
|---|---|---|
| Preflight | Probes both ports, validates credential shapes, checks for pgloader | No |
| Inventory | Counts every S3 object, flags oversize and Glacier files, writes a manifest | Manifest only |
| Database | Generates a pgloader command file and streams its output live | Target only |
| Storage | Streams each object S3 → Supabase, parallel and resumable | Target only |
| Verify | Re-lists both sides, reports anything missing or the wrong size | No |

### Resuming

Every completed storage key is recorded, so rerunning skips what is done and
retries only what failed. `Ctrl-C` is safe.

### Output

Live progress with percentage, throughput and ETA:

```
   4,812/12,004  40.1%    1.2 GB    38.4 MB/s  eta 9m12s  ok=4810 skip=2 fail=0
```

Everything is teed to `migration_state/migrate.log` with secrets masked. Failed
transfers get full tracebacks in `migration_state/failures.log`; `--debug`
prints them live.

---

## Files written to `--state-dir`

| File | Contents |
|---|---|
| `migrate.log` | Full transcript of every run, secrets masked |
| `s3_inventory.csv` | Every object that will move |
| `s3_migrated.jsonl` | Resume log |
| `failures.log` | Tracebacks, only if a transfer fails |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Finished, but with failures |
| `2` | Bad settings or preflight failure |
| `3` | Unexpected error (a bug) |
| `130` | Interrupted |

---

## Permissions

The source needs read access only. Granting exactly that makes "copy, don't
modify" a property of your infrastructure rather than something you take on
trust.

**AWS** — attach to the IAM user, replacing the bucket name in both places.
Note the two ARN forms: `ListBucket` targets the bucket, `GetObject` targets the
objects inside it.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::YOUR_BUCKET" },
    { "Effect": "Allow", "Action": "s3:GetObject",  "Resource": "arn:aws:s3:::YOUR_BUCKET/*" }
  ]
}
```

**MySQL** — a read-only user:

```sql
CREATE USER 'migrate_ro'@'localhost' IDENTIFIED BY 'a-strong-password';
GRANT SELECT, SHOW VIEW ON your_database.* TO 'migrate_ro'@'localhost';
FLUSH PRIVILEGES;
```

---

## Security

`migration.env` holds live credentials and is gitignored — commit only
`migration.env.example`. The generated pgloader command file contains both
passwords in plaintext; it is created `0600` and deleted after each live run.
Secrets are masked in all console output and in the transcript.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| MySQL connection times out | Firewall dropping 3306 | Run on the DB server with `MYSQL_HOST=127.0.0.1`, or use an SSH tunnel |
| pgloader fails on the Supabase side | Port 6543 | Use `SUPABASE_DB_PORT=5432` |
| `SignatureDoesNotMatch` | The secret does not match the key id | Re-copy it; check the length (AWS 40, Supabase 64) |
| `NoSuchBucket` on a bucket you can see | Virtual-host addressing | Already handled by the client config |
| `403 Forbidden` on the source | Missing IAM permissions | Attach the policy above |
| `413 Payload too large` | File over the bucket limit | Raise it in Storage → Buckets, or exclude the key |
| Transfers stall, `429` | Too much parallelism | Lower `--workers` to 4 |

---

## Also here

`Amazon_S3_to_Supabase_Storage.ipynb` — the storage migration as a Colab
notebook, for a one-off run without a server.

## License

MIT
