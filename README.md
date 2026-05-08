# Bluesky Starter Pack / List Blocker

Block every account contained in a Bluesky **starter pack** or **user list**.

The script logs in with an app password, resolves each input (starter pack, list, or short link) into a list, loads members, merges them into a unique set (by DID), skips:

- your own account
- accounts you already block

...and then creates block records for the remaining accounts.

## Requirements

- Python 3.12+
- `atproto>=0.0.65`

## Install

```bash
python3 -m pip install "atproto>=0.0.65"
```

## Setup

1. In Bluesky: **Settings -> Privacy and security -> App passwords** - create an app password.
2. Prefer passing it via environment (avoids shell history and process lists):

   ```bash
   export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
   ```

## Usage

Run a **dry run** first (no blocks are created):

```bash
python3 bsky.py --handle your.handle.bsky.social --input "<starter-pack-or-list-link>" --dry-run
```

If you want a line-per-account listing of what would be blocked, add `--verbose`:

```bash
python3 bsky.py --handle your.handle.bsky.social --input "<starter-pack-or-list-link>" --dry-run --verbose
```

If the output looks right, run without `--dry-run`:

```bash
python3 bsky.py --handle your.handle.bsky.social --input "<starter-pack-or-list-link>"
```

Use more than one source by putting one input per line in a file:

```bash
python3 bsky.py --handle your.handle.bsky.social --file inputs.txt --dry-run --verbose
```

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

The script will pause and retry on rate limits and some transient network/server errors.

## License

See [LICENSE](LICENSE).
