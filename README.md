# Bluesky Starter Pack / List Blocker

Block every account contained in a Bluesky **starter pack** or **user list**.

> [!CAUTION]
> **Blocking is destructive.** This script can create **thousands** of blocks
> in seconds. Bluesky does not offer bulk-unblock, so reversing a large run
> means manually hunting down and unblocking accounts one by one — a tedious
> and error-prone process.
>
> - **Soft blows matter too**: every block is a permanent sever. Starter packs
>   and lists are curated by humans and often include accounts you might
>   actually want to see.
> - **Abuse risk**: mass-blocking in bulk could be flagged as abusive behavior
>   by Bluesky, potentially putting your account at risk. Use responsibly.
> - **Before you block**: always **dry-run first** (`--dry-run --verbose`) and
>   inspect the list. You can't easily undo this.
>
> Built-in `--unblock` or `--mute` support may be added in a future release.

The script logs in with an app password, resolves each input (starter pack, list, or short link) into a list, loads members, merges them into a unique set (by DID), skips:

- your own account
- accounts you already block

...and then creates block records for the remaining accounts.

## Requirements

- Python 3.12+
- `atproto>=0.0.65`

## Install

The project ships a `uv.lock`, so the recommended install is:

```bash
uv sync
```

Otherwise, install the dependency directly:

```bash
python3 -m pip install "atproto>=0.0.65"
```

## Setup

1. In Bluesky: **Settings -> Privacy and security -> App passwords** - create an app password.
2. Set the app password in the environment:

   ```bash
   export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
   ```

3. Set your Bluesky handle in the environment:

   ```bash
   export BSKY_HANDLE="your.handle.bsky.social"
   ```

## Usage

Run a **dry run** first (no blocks are created):

```bash
python3 bsky.py --input "<starter-pack-or-list-link>" --dry-run
```

If you want a line-per-account listing of what would be blocked, add `--verbose`:

```bash
python3 bsky.py --input "<starter-pack-or-list-link>" --dry-run --verbose
```

If the output looks right, run without `--dry-run` (`--no-dry-run` is the explicit form):

```bash
python3 bsky.py --input "<starter-pack-or-list-link>" --no-dry-run
```

You'll be prompted to confirm before any blocks are created. Use `-y`/`--yes` to skip the prompt (required for non-interactive runs):

```bash
python3 bsky.py --input "<starter-pack-or-list-link>" --no-dry-run --yes
```

Use `--quiet` to suppress per-account lines; only operational messages and the final summary are printed:

```bash
python3 bsky.py --input "<starter-pack-or-list-link>" --no-dry-run --quiet
```

Use more than one source by putting one input per line in a file:

```bash
python3 bsky.py --file inputs.txt --dry-run --verbose
```

## Flags

| Flag | Description |
| --- | --- |
| `-i`/`--input` | Single starter-pack/list URL or AT URI (mutually exclusive with `--file`). |
| `-f`/`--file` | UTF-8 text file with one URL/AT URI per line (max 10 MiB). |
| `--delay` | Seconds to sleep between successful blocks (default `0.5`). |
| `--dry-run`/`--no-dry-run` | Print what would be blocked, or actually block. Default: dry-run is off (use `--no-dry-run` to make the intent explicit). |
| `-y`/`--yes` | Skip the confirmation prompt. Required for non-TTY destructive runs. |
| `--quiet` | Suppress per-account output; only the summary and operational messages print. |
| `-v`/`--verbose` | Show verbose per-account detail (e.g. dry-run `DRY BLOCK` lines and error text). |

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Every member processed without failures. |
| 1 | At least one block attempt failed. |
| 2 | The destructive confirmation prompt was declined, or stdin was not a TTY without `--yes`. |
| 3 | The PDS-reported block-list cap was reached and the run was aborted. Unblock some accounts to continue. |

## Supported input formats

Inputs can be starter packs, lists, or short links (URLs may omit the `https://` scheme):

- Starter pack AT URI: `at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>`
- List AT URI: `at://<did-or-handle>/app.bsky.graph.list/<rkey>`
- Starter pack URLs:
  - `https://bsky.app/start/<did-or-handle>/<rkey>`
  - `https://bsky.app/starter-pack/<did-or-handle>/<rkey>`
- List URL:
  - `https://bsky.app/profile/<did-or-handle>/lists/<rkey>`
- Short links:
  - `https://bsky.app/starter-pack-short/<code>`
  - `https://go.bsky.app/<code>`

The script will pause and retry on rate limits and some transient network/server errors, and re-authenticate once per run if the access token expires mid-run.

## Notes

- Bluesky caps the number of accounts a single account can block (currently a few thousand). The script detects this and aborts with exit code 3 so you don't loop forever. Unblock some accounts, then re-run.
- Per-account output is suppressed by `--quiet`. Operational messages (rate-limit pauses, re-authentication notices, cap-reached aborts) always print.
- Progress is logged to stderr every 25 successful blocks (or would-blocks in dry-run). In `--quiet` mode both per-account output and progress are suppressed. Only operational messages and the final summary print.

## License

See [LICENSE](LICENSE).
