# O2 Cloud storage (experimental)

The native O2 Cloud backend is available as `--storage o2cloud`. The endpoint is
the name of an **existing folder at the root of the O2 Cloud account**. JuiceFS
will only resolve object keys below that folder; it will not create the endpoint
folder automatically.

Export a plaintext session JSON from the existing O2 Cloud tooling to a file
outside the JuiceFS repository. The file must contain `validationKey` and may
also contain cookies and an OAuth bundle for automatic session renewal. Keep it
private and set `O2CLOUD_SESSION_FILE` to its path. Alternatively, direct login
uses `ACCESS_KEY` and `SECRET_KEY`; do not put either value in a command line
that could be saved in shell history.

```sh
export O2CLOUD_SESSION_FILE=/private/path/secondary-session.json
export O2CLOUD_TPS=2
juicefs objbench --storage o2cloud --block-size 4M \
  --big-object-size 64M --small-objects 10 --threads 2 \
  --skip-functional-tests JUICEFS-O2-LAB
```

For a 16 MiB comparison, use the same command with `--block-size 16M`. The
benchmark creates a unique `__juicefs_benchmark_*` folder under the endpoint.
Use only a test account and a dedicated folder for destructive benchmarks.

The backend confirms each upload using the O2 media ID when the upload response
contains one. If the response is ambiguous, it searches for a newly created ID
before returning success; it never blindly resends the upload. Existing keys
can be put again only if their contents are identical. A changed key returns an
error to avoid silent replacement. Deletes use O2's soft-delete operation and
are confirmed by ID. Nonempty folders are not deleted.

`O2CLOUD_TPS` limits request rate across one storage client (default: 2).
`O2CLOUD_RETRIES` sets the number of retries for read-only API requests
(default: 3). Mutating requests are never blindly retried.
Upload confirmation can take about two minutes when O2's listings lag. For
mounts on a slow account, set `--put-timeout 300s`. Set `O2CLOUD_JOURNAL_DIR`
to an absolute, persistent directory outside the repository, dedicated to this
account and endpoint. The backend records each upload intent there before
sending data. After a restart, it refuses to resend an unconfirmed key until
it can verify an identical object. The journal contains object keys and sizes,
but no credentials. Keep it available to every process that may write to the
same endpoint. Without this directory, the guard only lasts until the client
exits. Stop and inspect the volume after an unconfirmed upload; do not discard
the journal to force a retry.
`O2CLOUD_API_URL` and `O2CLOUD_UPLOAD_URL` are intended only for local mock
tests. Uploads spool to private temporary files and support a single object up
to 1 GiB. Uploads above 200 MiB use O2's asynchronous upload mode. Multipart
upload, server-side copy, and restore are not implemented. Directory listings
currently traverse the endpoint folder before producing sorted pages, which
can be slow for large volumes.
