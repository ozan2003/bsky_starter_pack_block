#!/usr/bin/env python3
"""Block every account listed in one or more Bluesky starter packs or lists.

This script logs in to Bluesky with an app password, resolves each supported
input to a list URI, loads every account across those lists (merging
unique members by account), skips your own account and accounts you already
block, then creates block records for the remaining accounts.

Supported inputs:
    - ``at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>``
    - ``at://<did-or-handle>/app.bsky.graph.list/<rkey>``
    - ``http(s)://bsky.app/start/<did-or-handle>/<rkey>``
    - ``http(s)://bsky.app/starter-pack/<did-or-handle>/<rkey>``
    - ``http(s)://bsky.app/profile/<did-or-handle>/lists/<rkey>``
    - ``http(s)://bsky.app/starter-pack-short/<code>``
    - ``http(s)://go.bsky.app/<code>``

Usage:
    First create an app password in Bluesky under Settings -> Privacy and
    Security -> App Passwords. Then set that password and your handle in the environment:

    ```
    export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
    export BSKY_HANDLE="your.handle.bsky.social"
    ```

    Run a dry run before blocking:

    ``python3 bsky.py --input <url-or-at-uri> --dry-run``

    Or load pack inputs from a file (one input per line):

    ``python3 bsky.py --file inputs.txt --dry-run``

    ``--input`` and ``--file`` are mutually exclusive.

    If the dry run looks correct, run without ``--dry-run``:

    ``python3 bsky.py --input <url-or-at-uri>``

    You can pass ``--delay`` to control the pause between block operations.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from importlib import metadata as importlib_metadata
from math import isinf, isnan
from pathlib import Path
from random import uniform
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from atproto import Client, models
from atproto.exceptions import (
    AtProtocolError,
    BadRequestError,
    NetworkError,
    RequestException,
    UnauthorizedError,
)

try:
    __version__ = importlib_metadata.version("bsky-starter-pack-block")
except importlib_metadata.PackageNotFoundError:
    __version__ = "0.0.0+unknown"

STARTER_PACK_COLLECTION = "app.bsky.graph.starterpack"
LIST_COLLECTION = "app.bsky.graph.list"
SUPPORTED_STARTER_PACK_PATHS = {"start", "starter-pack"}
PROFILE_PATH_SEGMENT = "profile"
LISTS_PATH_SEGMENT = "lists"
BSKY_APP_HOSTS = {"bsky.app", "www.bsky.app"}
BSKY_SHORT_LINK_HOST = "go.bsky.app"
STARTER_PACK_SHORT_PATH = "starter-pack-short"

# Socket timeout for urllib when resolving short links (go.bsky.app redirects).
SHORT_LINK_TIMEOUT = dt.timedelta(seconds=10.0)

# Maximum size of the input file in bytes; anything larger is rejected before
# opening so a 5GB paste cannot OOM the process.
MAX_INPUT_FILE_SIZE = 10 * 1024 * 1024  # 10 MiB

# Maximum length of a single AT URI segment (identifier, rkey). Keeps a
# hostile 1MB identifier from making the parser do useless work.
MAX_AT_URI_SEGMENT_LENGTH = 512

# A progress line is emitted to stderr every N successful blocks (or, in
# dry-run, every N would-blocks). Set to 0 to disable progress output.
PROGRESS_EVERY = 25

# Maximum records per page for listRepos/listRecords-style reads.
LIST_PAGE_SIZE = 100

# Same pagination limit when enumerating existing blocks to skip already-blocked DIDs.
BLOCKS_PAGE_SIZE = 100

# Successful blocks are followed by ``time.sleep(delay)``, CLI default when ``--delay`` omitted.
DEFAULT_DELAY = dt.timedelta(seconds=0.5)

# Attempts per account in ``block_users`` use capped exponential backoff + jitter.
MAX_BLOCK_ATTEMPTS = 5  # Total attempts (initial + retries) per account.
# Wait ``min(MAX, BASE * 2**(attempt - 1))`` seconds before each retry.
BASE_BACKOFF = dt.timedelta(seconds=1.0)
# Upper bound so backoff does not grow past a lot.
MAX_BACKOFF = dt.timedelta(seconds=8.0)

# Uniform random extra delay on each backoff (seconds), reduces synchronized retries.
JITTER = (dt.timedelta(seconds=0.0), dt.timedelta(seconds=0.6))

# Error for non-existent packs or invalid DIDs.
HTTP_STATUS_BAD_REQUEST = 400
# HTTP response codes treated as retryable alongside network errors (see ``is_transient_error``).
HTTP_STATUS_TOO_MANY_REQUESTS = 429
# Any status greater than or equal to this is treated as a transient server error.
HTTP_STATUS_SERVER_ERROR_MIN = 500

# Rate-limit pause: small buffer added to the computed wait so the retry lands after the window resets.
RATE_LIMIT_BUFFER = dt.timedelta(seconds=2.0)
# If the server asks to wait longer than this, abort instead of blocking the terminal.
RATE_LIMIT_MAX_WAIT = dt.timedelta(hours=3)
# XRPC error code returned by the PDS when the user has hit the maximum
# per-account block list size. Used to abort the run with a clear message
# instead of retrying forever.
BLOCK_LIST_CAP_ERROR_CODE = "BlockedAccountCountLimitExceeded"
# User agent sent when resolving short links. Identifies the script by name
# and version so the short-link service can apply appropriate rate limits.
USER_AGENT = f"bsky-starter-pack-block/{__version__}"

# Useful for comparisons.
ZERO_DURATION = dt.timedelta(0)


class BlockListCapError(Exception):
    """Raised when the PDS rejects a block because the user hit the per-account cap."""

    def __init__(self, did: str, handle: str) -> None:
        super().__init__(
            f"Block list cap reached while blocking {handle} ({did})"
        )
        self.did = did
        self.handle = handle


@dataclass(slots=True)
class Member:
    """Starter pack member selected for possible blocking.

    Attributes:
        did: Account DID used as the stable block target.
        handle: Current account handle, used only for human-readable output.
    """

    did: str
    handle: str


@dataclass(slots=True)
class BlockSummary:
    """Counters collected while processing starter pack members.

    Attributes:
        discovered: Number of unique starter pack members loaded.
        skipped_self: Number of members skipped because they are the signed-in
            account.
        skipped_already_blocked: Number of members skipped because a block
            already exists.
        skipped_invalid: Number of members skipped because the block request
            returned a permanent client error (e.g. deleted account).
        would_block: Number of members that would be blocked in dry-run mode.
        blocked: Number of block records created successfully.
        failed: Number of members that could not be blocked.
        retries: Number of retry attempts used for transient block failures.
        reauths: Number of re-authentications triggered by an expired session.
    """

    discovered: int = 0
    skipped_self: int = 0
    skipped_already_blocked: int = 0
    skipped_invalid: int = 0
    would_block: int = 0
    blocked: int = 0
    failed: int = 0
    retries: int = 0
    reauths: int = 0


@dataclass(frozen=True, slots=True)
class PackReference:
    """Canonical starter pack/list reference before DID normalization.

    Attributes:
        identifier: Starter-pack/list creator DID or handle.
        rkey: Record key.
        collection: AT Protocol collection for the reference.
    """

    identifier: str
    rkey: str
    collection: str


@dataclass(frozen=True, slots=True)
class ShortStarterPackLink:
    """Bluesky short link that must be resolved before API use.

    Attributes:
        url: Canonical short-link service URL.
    """

    url: str


type PackInput = PackReference | ShortStarterPackLink


class SourceKind(StrEnum):
    """Kind of source input."""

    STARTER_PACK = "starter_pack"
    LIST = "list"


class BlockOutcome(StrEnum):
    """Outcome of a single-account block attempt."""

    BLOCKED = "blocked"
    SKIPPED_INVALID = "skipped_invalid"
    FAILED = "failed"


def confirm_destructive(count: int) -> bool:
    """Prompt the user before blocking ``count`` accounts.

    Returns ``True`` if the user typed ``y``/``yes`` (case-insensitive).
    Returns ``False`` for any other response.

    Raises:
        SystemExit: With code ``2`` if stdin is not a TTY (e.g. cron / CI),
            so a destructive run is never launched silently.
    """

    if not sys.stdin.isatty():
        print(
            f"ERROR: refusing to block {count} accounts without a TTY."
            " Pass --yes to confirm non-interactively.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    print(f"About to block {count} accounts. Continue? [y/N]")
    response = input().strip().lower()
    return response in {"y", "yes"}


@dataclass(frozen=True, slots=True)
class ResolvedListTarget:
    """Resolved list target used by the fetch/block pipeline.

    Attributes:
        list_uri: Canonical list AT URI ready for ``app.bsky.graph.getList``.
        source_kind: Whether the input was a direct list or a starter pack.
    """

    list_uri: str
    source_kind: SourceKind


def parse_delay(value: str) -> dt.timedelta:
    """Parse a CLI delay value.

    Args:
        value: Raw command-line value for ``--delay``.

    Returns:
        A finite, non-negative delay.

    Raises:
        argparse.ArgumentTypeError: If the delay is negative, infinite, or NaN.
        ValueError: If ``value`` cannot be parsed as a float.
    """

    delay = float(value)
    if delay < 0:
        msg = "delay must be greater than or equal to 0"
        raise argparse.ArgumentTypeError(msg)
    if isinf(delay) or isnan(delay):
        msg = "delay must be a finite number"
        raise argparse.ArgumentTypeError(msg)
    return dt.timedelta(seconds=delay)


def parse_args() -> argparse.Namespace:
    """Parse command-line options.

    Returns:
        Parsed command-line arguments for login, one or more starter-pack/list
        inputs, throttling, and dry-run mode.
    """

    parser = argparse.ArgumentParser(
        description="Block all users from one or more Bluesky starter packs or lists",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Needed enviroment variables:\n"
            "    - BSKY_APP_PASSWORD: Bluesky app password\n"
            "    - BSKY_HANDLE: Bluesky handle\n"
        ),
    )

    pack_input_group = parser.add_mutually_exclusive_group(required=True)

    pack_input_group.add_argument(
        "-i",
        "--input",
        type=str,
        help="Single starter-pack/list URL or AT URI",
    )

    pack_input_group.add_argument(
        "-f",
        "--file",
        type=str,
        help="Path to a UTF-8 text file with one starter-pack/list URL or AT URI per line",
    )

    parser.add_argument(
        "--delay",
        type=parse_delay,
        default=DEFAULT_DELAY,
        help="Delay between blocks (seconds)",
    )

    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Print users without blocking (use --no-dry-run to actually block)",
    )

    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Skip the confirmation prompt before blocking",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress per-account lines, only print the summary",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print verbose output",
        default=False,
    )

    return parser.parse_args()


def load_inputs_from_file(path: str) -> list[str]:
    """Load starter-pack/list inputs from a UTF-8 text file.

    The file is stat-checked first and rejected if it exceeds
    :data:`MAX_INPUT_FILE_SIZE`, so a multi-gigabyte paste cannot OOM the
    process before the read begins.

    Note that this function does not normalize or validate the inputs.
    Refer to ``normalize_input_uri`` for more information.

    Args:
        path: File path containing one starter-pack/list input per line.

    Returns:
        Non-empty input lines with surrounding whitespace removed.

    Raises:
        ValueError: If the file is missing, exceeds the size cap, cannot be
            read, or contains no usable pack lines.
    """

    file_path = Path(path)
    try:
        size = file_path.stat().st_size
    except OSError as error:
        msg = f"Could not stat source file {file_path}: {error}"
        raise ValueError(msg) from error

    if size > MAX_INPUT_FILE_SIZE:
        msg = (
            f"Source file {file_path} is {size} bytes, exceeding the "
            f"{MAX_INPUT_FILE_SIZE}-byte cap. Refusing to read."
        )
        raise ValueError(msg)

    try:
        with file_path.open(encoding="utf-8") as fp:
            inputs = [stripped for line in fp if (stripped := line.strip())]
    except OSError as error:
        msg = f"Could not read source file {file_path}: {error}"
        raise ValueError(msg) from error

    if inputs:
        return inputs

    msg = f"Source file {file_path} does not contain any starter-pack/list inputs"
    raise ValueError(msg)


def resolve_handle() -> str:
    """Resolve the Bluesky handle from the environment."""
    handle = os.getenv("BSKY_HANDLE")
    if not handle:
        msg = "Missing Bluesky handle. Set BSKY_HANDLE."
        raise ValueError(msg)
    return handle


def resolve_app_password() -> str:
    """Resolve the Bluesky app password from the environment.

    Returns:
        The app password to use for login.

    Raises:
        ValueError: If no password was provided and ``BSKY_APP_PASSWORD`` is not
            set.
    """

    env_password = os.getenv("BSKY_APP_PASSWORD")
    if not env_password:
        msg = "Missing app password. Set BSKY_APP_PASSWORD."
        raise ValueError(msg)
    return env_password


def supported_input_format_error() -> str:
    """Build the supported input error message.

    Returns:
        A message listing every supported starter-pack/list input format.
    """

    return (
        "Input must be one of: "
        "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>, "
        "at://<did-or-handle>/app.bsky.graph.list/<rkey>, "
        "http(s)://bsky.app/start/<did-or-handle>/<rkey>, "
        "http(s)://bsky.app/starter-pack/<did-or-handle>/<rkey>, "
        "http(s)://bsky.app/profile/<did-or-handle>/lists/<rkey>, "
        "http(s)://bsky.app/starter-pack-short/<code>, "
        "or http(s)://go.bsky.app/<code>"
    )


def parse_at_uri(raw: str) -> PackReference | None:
    """Parse a supported list/starter-pack AT URI.

    Args:
        raw: User-provided input after whitespace trimming.

    Returns:
        A canonical pack/list reference when ``raw`` is an AT URI, otherwise
        ``None``.

    Raises:
        ValueError: If ``raw`` is an AT URI but has the wrong shape, an
            unsupported collection, or an over-long segment.
    """

    if not raw.startswith("at://"):
        return None

    parts = [part for part in raw[len("at://") :].split("/") if part]
    if len(parts) != 3:
        msg = (
            f"AT URI must have exactly 3 segments (did-or-handle, collection,"
            f" rkey); got {len(parts)}: {raw!r}"
        )
        raise ValueError(msg)

    identifier, collection, rkey = parts
    if collection not in {STARTER_PACK_COLLECTION, LIST_COLLECTION}:
        msg = (
            f"AT URI must use collection {STARTER_PACK_COLLECTION} or"
            f" {LIST_COLLECTION}; got {collection!r}"
        )
        raise ValueError(msg)

    for label, value in (("identifier", identifier), ("rkey", rkey)):
        if len(value) > MAX_AT_URI_SEGMENT_LENGTH:
            msg = (
                f"AT URI {label} is {len(value)} chars, exceeding the "
                f"{MAX_AT_URI_SEGMENT_LENGTH}-char cap"
            )
            raise ValueError(msg)

    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=collection,
    )


def parse_starter_pack_path(path_source: str) -> PackReference | None:
    """Parse a canonical Bluesky starter pack URL path.

    Args:
        path_source: URL path or path-like input.

    Returns:
        A starter pack reference for ``/start/.../...`` or
        ``/starter-pack/.../...`` paths, otherwise ``None``.
    """

    parts = [part for part in path_source.split("/") if part]
    if len(parts) != 3 or parts[0] not in SUPPORTED_STARTER_PACK_PATHS:
        return None

    _, identifier, rkey = parts
    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=STARTER_PACK_COLLECTION,
    )


def parse_list_path(path_source: str) -> PackReference | None:
    """Parse a canonical Bluesky direct-list URL path.

    Args:
        path_source: URL path or path-like input.

    Returns:
        A list reference for ``/profile/.../lists/...`` paths, otherwise
        ``None``.
    """

    parts = [part for part in path_source.split("/") if part]
    if (
        len(parts) != 4
        or parts[0] != PROFILE_PATH_SEGMENT
        or parts[2] != LISTS_PATH_SEGMENT
    ):
        return None

    _, identifier, _, rkey = parts
    for label, value in (("identifier", identifier), ("rkey", rkey)):
        if len(value) > MAX_AT_URI_SEGMENT_LENGTH:
            msg = (
                f"List path {label} is {len(value)} chars, exceeding the "
                f"{MAX_AT_URI_SEGMENT_LENGTH}-char cap"
            )
            raise ValueError(msg)
    return PackReference(
        identifier=identifier,
        rkey=rkey,
        collection=LIST_COLLECTION,
    )


def parse_short_pack_path(
    host: str | None, path_source: str
) -> ShortStarterPackLink | None:
    """Parse a Bluesky starter pack short-link path.

    Args:
        host: Normalized URL host, if one was present.
        path_source: URL path or path-like input.

    Returns:
        A short-link reference for ``go.bsky.app/<code>`` or
        ``bsky.app/starter-pack-short/<code>``, otherwise ``None``.
    """

    parts = [part for part in path_source.split("/") if part]
    if not parts:
        return None

    if host == BSKY_SHORT_LINK_HOST and len(parts) == 1:
        return ShortStarterPackLink(
            url=f"https://{BSKY_SHORT_LINK_HOST}/{parts[0]}"
        )

    if (
        host in BSKY_APP_HOSTS
        and len(parts) == 2
        and parts[0] == STARTER_PACK_SHORT_PATH
    ):
        return ShortStarterPackLink(
            url=f"https://{BSKY_SHORT_LINK_HOST}/{parts[1]}"
        )

    return None


def parse_pack_input(pack_input: str) -> PackInput:
    """Parse any supported starter-pack/list input format.

    Args:
        pack_input: Starter-pack/list AT URI, canonical Bluesky URL, or short
            link.

    Returns:
        A canonical pack reference, or a short-link reference that still needs
        network resolution.

    Raises:
        ValueError: If the input is empty, uses an unsupported scheme or host,
            or does not match a supported format.
    """

    raw = pack_input.strip()
    if not raw:
        msg = "Input cannot be empty"
        raise ValueError(msg)

    # AT URIs are already the format required by the Bluesky API, except that
    # handles are still accepted here and normalized to DIDs later.
    at_reference = parse_at_uri(raw)
    if at_reference is not None:
        return at_reference

    parsed = urlparse(raw)
    host = parsed.hostname.lower() if parsed.hostname else None
    path_source = parsed.path if parsed.scheme else raw

    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        raise ValueError(supported_input_format_error())

    if host is not None and host not in {
        *BSKY_APP_HOSTS,
        BSKY_SHORT_LINK_HOST,
    }:
        msg = f"Unsupported Bluesky URL host: {host}"
        raise ValueError(msg)

    if not parsed.scheme:
        # Users often paste links without the scheme, e.g.
        # bsky.app/starter-pack/user.bsky.social/rkey. Treat those as Bluesky
        # paths only when they start with a known host.
        for app_host in BSKY_APP_HOSTS:
            if path_source.startswith(f"{app_host}/"):
                host = app_host
                path_source = "/" + path_source[len(app_host) + 1 :]
                break
        if path_source.startswith(f"{BSKY_SHORT_LINK_HOST}/"):
            host = BSKY_SHORT_LINK_HOST
            path_source = "/" + path_source[len(BSKY_SHORT_LINK_HOST) + 1 :]

    reference = parse_starter_pack_path(path_source)
    if reference is not None:
        return reference

    list_reference = parse_list_path(path_source)
    if list_reference is not None:
        return list_reference

    # Short links do not contain the DID/rkey pair. Return a marker object so
    # normalization can resolve the link before trying to build an AT URI.
    short_link = parse_short_pack_path(host, path_source)
    if short_link is not None:
        return short_link

    raise ValueError(supported_input_format_error())


def resolve_short_starter_pack_url(short_link: ShortStarterPackLink) -> str:
    """Resolve a Bluesky short link to its canonical URL.

    Args:
        short_link: Validated Bluesky short link.

    Returns:
        The canonical URL returned by the short-link service.

    Raises:
        ValueError: If the short-link URL is not an HTTPS ``go.bsky.app`` URL.
        RuntimeError: If the short link cannot be resolved or resolves to an
            unusable response.
    """

    parsed = urlparse(short_link.url)
    if parsed.scheme != "https" or parsed.hostname != BSKY_SHORT_LINK_HOST:
        msg = f"Unsupported short link URL: {short_link.url}"
        raise ValueError(msg)

    # Bluesky's short-link service returns JSON when requested with this Accept
    # header. That avoids scraping HTML and gives us the canonical bsky.app URL.
    request = Request(  # noqa: S310
        short_link.url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urlopen(  # noqa: S310
            request,
            timeout=SHORT_LINK_TIMEOUT.total_seconds(),
        ) as response:
            body = response.read()
            content_type = response.headers.get("content-type", "")
            if body and "application/json" in content_type:
                raw = json.loads(body.decode("utf-8"))
                if isinstance(raw, dict):
                    payload = cast(dict[str, object], raw)
                    url = payload.get("url")
                    if isinstance(url, str) and url:
                        return url

            # Keep a redirect fallback for clients or environments where the
            # service responds with a normal HTTP redirect instead of JSON.
            final_url = response.geturl()
            if isinstance(final_url, str) and final_url != short_link.url:
                return final_url
    except HTTPError as error:
        msg = (
            f"Could not resolve short link {short_link.url}: HTTP {error.code}"
        )
        raise RuntimeError(msg) from error
    except (OSError, TimeoutError, URLError, json.JSONDecodeError) as error:
        msg = f"Could not resolve short link {short_link.url}: {error}"
        raise RuntimeError(msg) from error

    msg = f"Short link did not resolve to a supported URL: {short_link.url}"
    raise RuntimeError(msg)


def resolve_identifier_to_did(
    client: Client,
    identifier: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> str:
    """Resolve a handle-like identifier to a DID.

    Args:
        client: Authenticated AT Protocol client.
        identifier: DID or Bluesky handle.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A DID suitable for building an AT URI.

    Raises:
        RuntimeError: If handle resolution succeeds but does not return a DID.
    """

    if identifier.startswith("did:"):
        return identifier

    response = call_with_rate_limit_retry(
        lambda: client.resolve_handle(identifier),
        context="resolve handle",
        reauth=reauth,
    )
    if response.did:
        return response.did

    msg = f"Could not resolve handle to DID: {identifier}"
    raise RuntimeError(msg)


def normalize_input_uri(
    client: Client,
    source_input: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> PackReference:
    """Normalize a supported input into a canonical ``PackReference``.

    Args:
        client: Authenticated AT Protocol client used for handle-to-DID
            resolution.
        source_input: Starter-pack/list AT URI, canonical Bluesky URL, or short
            link.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A ``PackReference`` with the resolved DID, collection, and rkey.

    Raises:
        RuntimeError: If short-link resolution loops too many times.
        ValueError: If the input format is unsupported.
    """

    current_input = source_input
    for _ in range(3):
        parsed_input = parse_pack_input(current_input)
        if isinstance(parsed_input, ShortStarterPackLink):
            # A short link resolves to another supported input format, usually
            # https://bsky.app/start/<did>/<rkey>, so loop back through the parser.
            current_input = resolve_short_starter_pack_url(parsed_input)
            continue

        # The graph API requires the creator DID in the AT URI; web links
        # may contain a handle, so resolve that as the final parse step.
        did = resolve_identifier_to_did(
            client, parsed_input.identifier, reauth=reauth
        )
        return PackReference(
            identifier=did,
            rkey=parsed_input.rkey,
            collection=parsed_input.collection,
        )

    msg = f"Short link resolution loop exceeded for {source_input}"
    raise RuntimeError(msg)


def normalize_starter_pack_uri(client: Client, pack_input: str) -> str:
    """Backward-compatible alias for ``normalize_input_uri``.

    Returns a canonical AT URI string rather than a ``PackReference``.
    """

    reference = normalize_input_uri(client, pack_input)
    return (
        f"at://{reference.identifier}/{reference.collection}/{reference.rkey}"
    )


def login(handle: str, app_password: str) -> tuple[Client, str]:
    """Log in to Bluesky and return the authenticated client.

    Args:
        handle: Bluesky handle or login identifier.
        app_password: Bluesky app password.

    Returns:
        A tuple containing the authenticated client and the signed-in account
        DID.

    Raises:
        RuntimeError: If login completes but the authenticated DID cannot be
            determined.
    """

    client = Client()
    profile = client.login(handle, app_password)

    did = profile.did
    if not did:
        did = getattr(getattr(client, "me", None), "did", None)

    if not isinstance(did, str) or not did:
        msg = "Unable to determine authenticated DID after login"
        raise RuntimeError(msg)

    return client, did


def _make_reauth_fn(
    client: Client,
    handle: str,
    app_password: str,
    summary: BlockSummary,
) -> Callable[[], bool]:
    """Build a one-shot re-authentication callback for the current run.

    The access token can be revoked or expire mid-run (especially on long
    runs). The SDK raises ``UnauthorizedError`` (HTTP 401) when it tries to
    use a stale session. This helper re-issues ``client.login`` with the
    already-resolved env credentials, then lets the caller retry the failed
    operation.

    The once-per-run cap is implemented via a closure flag so a flapping PDS
    can't trap us in a re-auth loop. After a successful re-auth, subsequent
    401s propagate normally and are recorded as failures.

    Args:
        client: Authenticated AT Protocol client.
        handle: Bluesky handle used at startup.
        app_password: App password used at startup.
        summary: Run summary; ``reauths`` is incremented on success.

    Returns:
        A zero-arg callable returning ``True`` if a re-authentication was
        performed (caller should retry) and ``False`` if a re-auth was
        already done earlier in this run.
    """

    reauth_used = [False]

    def reauth() -> bool:
        if reauth_used[0]:
            return False
        client.login(handle, app_password)
        reauth_used[0] = True
        summary.reauths += 1
        _emit(
            "INFO: re-authenticated (session expired)",
            quiet=False,
            force=True,
        )
        return True

    return reauth


def fetch_starter_pack_list_uri(
    client: Client,
    at_uri: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> str:
    """Fetch the backing list URI for a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        The AT URI of the list that contains the starter pack accounts.

    Raises:
        RuntimeError: If the starter pack response is missing the expected
            list data.
    """

    params = models.AppBskyGraphGetStarterPack.Params(starter_pack=at_uri)
    response = call_with_rate_limit_retry(
        lambda: client.app.bsky.graph.get_starter_pack(params),
        context="fetch starter pack",
        reauth=reauth,
    )

    starter_pack = response.starter_pack
    list_view = starter_pack.list
    if list_view is None:
        msg = "Starter pack does not expose a backing list"
        raise RuntimeError(msg)

    list_uri = list_view.uri
    if not list_uri:
        msg = "Starter pack list URI is missing"
        raise RuntimeError(msg)

    return list_uri


def resolve_input_to_list_target(
    client: Client,
    source_input: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> ResolvedListTarget:
    """Resolve any supported input into a list URI target.

    Args:
        client: Authenticated AT Protocol client.
        source_input: Raw user input identifying a starter pack or list.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        A list target with source metadata for logging.

    Raises:
        RuntimeError: If starter-pack list resolution fails or normalization
            does not produce a valid AT URI.
        ValueError: If the input format is unsupported.
    """

    reference = normalize_input_uri(client, source_input, reauth=reauth)
    at_uri = (
        f"at://{reference.identifier}/{reference.collection}/{reference.rkey}"
    )

    if reference.collection == STARTER_PACK_COLLECTION:
        list_uri = fetch_starter_pack_list_uri(client, at_uri, reauth=reauth)
        return ResolvedListTarget(
            list_uri=list_uri, source_kind=SourceKind.STARTER_PACK
        )

    return ResolvedListTarget(list_uri=at_uri, source_kind=SourceKind.LIST)


def fetch_list_members(
    client: Client,
    list_uri: str,
    *,
    reauth: Callable[[], bool] | None = None,
) -> list[Member]:
    """Load all unique account members from a list.

    Args:
        client: Authenticated AT Protocol client.
        list_uri: List AT URI.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        Unique list members keyed by DID.
    """

    members_by_did: dict[str, Member] = {}
    cursor: str | None = None

    while True:
        params = models.AppBskyGraphGetList.Params(
            list=list_uri,
            limit=LIST_PAGE_SIZE,
            cursor=cursor,
        )
        response = call_with_rate_limit_retry(
            lambda params=params: client.app.bsky.graph.get_list(params),
            context="fetch members page",
            reauth=reauth,
        )
        for item in response.items:
            subject = item.subject
            did = subject.did
            if not did:
                continue

            handle = subject.handle if subject.handle else "<unknown>"
            members_by_did.setdefault(did, Member(did=did, handle=handle))

        next_cursor = response.cursor
        if next_cursor:
            cursor = next_cursor
            continue
        break

    return list(members_by_did.values())


def merge_unique_members(
    merged: dict[str, Member], new_members: list[Member]
) -> None:
    """Add members into ``merged``, keeping the first handle seen per DID.

    Args:
        merged: Mapping from account DID to member, updated in place.
        new_members: Members to insert when their DID is not already present.
    """

    for member in new_members:
        merged.setdefault(member.did, member)


def fetch_blocked_dids(
    client: Client,
    *,
    reauth: Callable[[], bool] | None = None,
) -> set[str]:
    """Load all DIDs already blocked by the signed-in account.

    Args:
        client: Authenticated AT Protocol client.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        DIDs for accounts that are already blocked.
    """

    blocked_dids: set[str] = set()
    cursor: str | None = None

    while True:
        params = models.AppBskyGraphGetBlocks.Params(
            limit=BLOCKS_PAGE_SIZE,
            cursor=cursor,
        )
        response = call_with_rate_limit_retry(
            lambda params=params: client.app.bsky.graph.get_blocks(params),
            context="fetch blocks page",
            reauth=reauth,
        )
        for block in response.blocks:
            if block.did:
                blocked_dids.add(block.did)

        next_cursor = response.cursor
        if next_cursor:
            cursor = next_cursor
            continue
        break

    return blocked_dids


def current_time_iso(client: Client) -> str:
    """Return the timestamp format expected by Bluesky record creation.

    Args:
        client: Authenticated AT Protocol client. If the SDK exposes a
            timestamp helper, it is used first.

    Returns:
        Current UTC time as an RFC 3339 timestamp.
    """

    get_current_time_iso = getattr(client, "get_current_time_iso", None)
    if callable(get_current_time_iso):
        value = get_current_time_iso()
        if isinstance(value, str) and value:
            return value

    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def create_block_record(
    client: Client, did: str
) -> models.AppBskyGraphBlock.CreateRecordResponse:
    """Create a Bluesky block record for one account.

    The atproto SDK does not currently expose a high-level ``Client.block``
    helper, so the underlying ``com.atproto.repo.createRecord`` procedure is
    invoked directly with the ``app.bsky.graph.block`` collection.

    Args:
        client: Authenticated AT Protocol client.
        did: DID of the account to block.

    Returns:
        The create-record response with the new record's ``uri`` and ``cid``.

    Raises:
        RuntimeError: If the signed-in repo DID cannot be determined.
        AtProtocolError: If the PDS rejects the block (e.g. block-list cap).
    """

    repo_did = getattr(getattr(client, "me", None), "did", None)
    if not isinstance(repo_did, str) or not repo_did:
        msg = "Unable to determine repo DID for block operation"
        raise RuntimeError(msg)

    record = models.AppBskyGraphBlock.Record(
        subject=did,
        created_at=current_time_iso(client),
    )
    return client.app.bsky.graph.block.create(repo_did, record)


def extract_status_code(error: Exception) -> int | None:
    """Extract an HTTP status code from an SDK exception.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        The HTTP status code if available, otherwise ``None``.
    """

    response = getattr(error, "response", None)
    if response is None:
        return None

    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    return None


def extract_response_headers(error: Exception) -> dict[str, Any]:
    """Extract response headers from an SDK exception.

    The atproto SDK normalizes response headers to a lowercase-keyed
    ``dict[str, str]`` (see ``atproto_client.request._convert_headers_to_dict``),
    but the public type is ``dict[str, Any]``; this function preserves that type
    so callers do not have to cast.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        The response headers dict, or an empty dict when unavailable.
    """

    response = getattr(error, "response", None)
    if response is None:
        return {}

    headers = getattr(response, "headers", None)
    if isinstance(headers, dict):
        return headers
    return {}


def extract_rate_limit_wait(error: Exception) -> dt.timedelta | None:
    """Compute how long to sleep before the rate-limit window resets.

    Reads the ``ratelimit-reset`` header (Unix epoch seconds) first, then
    falls back to the standard ``retry-after`` header (delta seconds).  A
    small buffer is added so the retry lands safely after the window edge.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        Duration to wait (including buffer), or ``None`` when no usable
        rate-limit timing is available in the response.
    """

    headers = extract_response_headers(error)
    if not headers:
        return None

    reset_raw = headers.get("ratelimit-reset")
    if reset_raw is not None:
        try:
            reset_ts = float(reset_raw)
        except (TypeError, ValueError):
            pass
        else:
            wait_seconds = reset_ts - time.time()
            try:
                wait = dt.timedelta(seconds=wait_seconds) + RATE_LIMIT_BUFFER
            except (OverflowError, ValueError):
                pass
            else:
                return max(wait, RATE_LIMIT_BUFFER)

    retry_after_raw = headers.get("retry-after")
    if retry_after_raw is not None:
        try:
            retry_seconds = float(retry_after_raw)
        except (TypeError, ValueError):
            pass
        else:
            try:
                wait = dt.timedelta(seconds=retry_seconds) + RATE_LIMIT_BUFFER
            except (OverflowError, ValueError):
                pass
            else:
                return max(wait, RATE_LIMIT_BUFFER)

    return None


def describe_error(error: Exception) -> str:
    """Build a human-readable error message.

    Args:
        error: Exception to describe.

    Returns:
        The exception text, prefixed with an HTTP status code when available.
    """

    status_code = extract_status_code(error)
    if status_code is None:
        return str(error)
    return f"HTTP {status_code}: {error}"


def is_transient_error(error: BaseException) -> bool:
    """Decide whether a block failure should be retried.

    Uses SDK exception classes and the HTTP status code instead of fragile
    string matching. A failure is considered transient when:

    - it is a ``NetworkError`` (this includes ``InvokeTimeoutError``);
    - or it is a ``RequestException`` with a retryable status code (429, 5xx).

    All other exceptions (``BadRequestError``, ``UnauthorizedError``, unknown
    errors) are treated as permanent.

    Args:
        error: Exception raised during a block operation.

    Returns:
        ``True`` for transient failures; ``False`` otherwise.
    """

    if isinstance(error, NetworkError):
        return True

    if isinstance(error, RequestException):
        status_code = extract_status_code(error)
        if status_code == HTTP_STATUS_TOO_MANY_REQUESTS:
            return True
        if (
            isinstance(status_code, int)
            and status_code >= HTTP_STATUS_SERVER_ERROR_MIN
        ):
            return True

    return False


def is_block_list_cap_error(error: BaseException) -> bool:
    """Return ``True`` if ``error`` indicates the per-account block list cap.

    Checks the XRPC error code on the response payload first, then falls back
    to a substring match on the error message for SDK or server versions that
    do not surface the structured error.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        ``True`` when the error is the block-list-cap error, otherwise ``False``.
    """

    response = getattr(error, "response", None)
    content = (
        getattr(response, "content", None) if response is not None else None
    )

    error_code: str | None = None
    if isinstance(content, dict):
        code = content.get("error")
        if isinstance(code, str):
            error_code = code
    elif content is not None:
        code = getattr(content, "error", None)
        if isinstance(code, str):
            error_code = code

    if error_code == BLOCK_LIST_CAP_ERROR_CODE:
        return True

    message: str | None = None
    if isinstance(content, dict):
        msg = content.get("message")
        if isinstance(msg, str):
            message = msg
    elif content is not None:
        msg = getattr(content, "message", None)
        if isinstance(msg, str):
            message = msg
    if message is None:
        message = str(error)

    lowered = message.lower()
    return "block" in lowered and ("limit" in lowered or "exceeded" in lowered)


def is_bad_request_skip(error: Exception) -> bool:
    """Decide whether a 400 Bad Request should be silently skipped.

    The AT Protocol API returns HTTP 400 for permanently invalid targets
    (deleted accounts, malformed DIDs, etc.).  Retrying these is pointless,
    and they should not count as actionable failures.

    Args:
        error: Exception raised during a block operation.

    Returns:
        ``True`` when the error is an HTTP 400 that indicates an invalid
        or unreachable target.
    """

    return isinstance(error, BadRequestError) or (
        isinstance(error, RequestException)
        and extract_status_code(error) == HTTP_STATUS_BAD_REQUEST
    )


def is_input_unrecoverable(error: BaseException) -> bool:
    """Decide whether an input-resolution error should skip the input.

    Used in :func:`main` to recover from permanently-broken input sources
    (deleted starter pack, deleted list) without aborting the whole run.
    Treats the following as recoverable:

    - ``BadRequestError`` (HTTP 400: bad rkey, deleted record, etc.)
    - ``RequestException`` with status 404 (record not found / deleted)

    Args:
        error: Exception raised by the AT Protocol client.

    Returns:
        ``True`` when the input source is permanently broken and the
        caller should skip it and continue.
    """

    if isinstance(error, BadRequestError):
        return True
    if isinstance(error, RequestException):
        return extract_status_code(error) == 404
    return False


def call_with_rate_limit_retry[T](
    fn: Callable[[], T],
    *,
    context: str,
    reauth: Callable[[], bool] | None = None,
) -> T:
    """Call ``fn`` and transparently pause on HTTP 429 rate limits.

    On a 429 response the function reads ``ratelimit-reset`` (or
    ``retry-after``) from the response headers, sleeps until the window
    resets, and retries.  On an ``UnauthorizedError`` (HTTP 401) the optional
    ``reauth`` callback is invoked; if it returns ``True`` (a re-auth just
    succeeded) the function is retried once, otherwise the exception
    propagates.

    Args:
        fn: Zero-argument callable that performs a single API request.
        context: Human-readable label printed while pausing (e.g.
            ``"fetch members page"``).
        reauth: Optional callback that re-authenticates the client and
            returns ``True`` if the caller should retry. When ``None``,
            401s propagate as normal errors.

    Returns:
        The return value of ``fn`` on success.

    Raises:
        RuntimeError: If the rate-limit wait exceeds
            ``RATE_LIMIT_MAX_WAIT``.
    """

    while True:
        try:
            return fn()
        except Exception as error:
            if (
                isinstance(error, UnauthorizedError)
                and reauth is not None
                and reauth()
            ):
                continue
            status_code = extract_status_code(error)
            if status_code != HTTP_STATUS_TOO_MANY_REQUESTS:
                raise

            wait = extract_rate_limit_wait(error)
            if wait is None:
                raise

            resume_at = dt.datetime.fromtimestamp(
                time.time() + wait.total_seconds(),
                tz=dt.UTC,
            ).isoformat()
            if wait > RATE_LIMIT_MAX_WAIT:
                msg = (
                    f"Rate limit for {context} resets at {resume_at} "
                    f"({wait.total_seconds():.0f}s), exceeds max wait of "
                    f"{RATE_LIMIT_MAX_WAIT.total_seconds():.0f}s"
                )
                raise RuntimeError(msg) from error

            print(
                f"RATE LIMITED ({context}): pausing until {resume_at} ({wait.total_seconds():.0f}s)..."
            )
            time.sleep(wait.total_seconds())


@dataclass(slots=True)
class BlockResult:
    """Result of the blocking operations.

    Attributes:
        summary: Summary of the blocking operations.
        failures: Human-readable failure entries.
        skipped: Human-readable entries for accounts skipped due to permanent
            client errors.
        cap_reached: ``True`` when the PDS-reported block-list cap was hit and
            the run was aborted.
    """

    summary: BlockSummary
    failures: list[str]
    skipped: list[str]
    cap_reached: bool


def _record_block_failure(
    *,
    summary: BlockSummary,
    failures: list[str],
    user: Member,
    error: Exception,
    is_verbose: bool,
    is_quiet: bool = False,
) -> None:
    """Record and print a failed block attempt.

    Args:
        summary: Mutable run summary updated with the failure count.
        failures: Mutable list receiving a human-readable failed account entry.
        user: Starter pack member whose block attempt failed.
        error: Exception raised by the block operation.
        is_verbose: When ``True``, include the described error in output.
        is_quiet: When ``True``, suppress the per-account line.
    """

    error_text = describe_error(error)
    summary.failed += 1
    failures.append(f"{user.handle} ({user.did})")
    if is_verbose:
        _emit(
            f"ERROR {user.handle} ({user.did}) -> {error_text}", quiet=is_quiet
        )
    else:
        _emit(f"ERROR {user.handle} ({user.did})", quiet=is_quiet)


def _pause_for_rate_limit_if_needed(
    *,
    error: Exception,
    user: Member,
    summary: BlockSummary,
    failures: list[str],
    is_quiet: bool = False,
) -> bool | None:
    """Pause for a rate-limit response when retry timing is available.

    Args:
        error: Exception raised by the block operation.
        user: Starter pack member being blocked.
        summary: Mutable run summary updated with retry or failure counts.
        failures: Mutable list receiving a failed account entry when the
            rate-limit wait exceeds ``RATE_LIMIT_MAX_WAIT``.
        is_quiet: When ``True``, suppress the per-user "aborting" error line.

    Returns:
        ``True`` when the caller should retry immediately after the pause,
        ``False`` when the wait was too long and the user was recorded as
        failed, or ``None`` when the error is not a usable rate-limit response.
    """

    status_code = extract_status_code(error)
    if status_code != HTTP_STATUS_TOO_MANY_REQUESTS:
        return None

    rate_limit_wait = extract_rate_limit_wait(error)
    if rate_limit_wait is None:
        return None

    resume_at = dt.datetime.fromtimestamp(
        time.time() + rate_limit_wait.total_seconds(),
        tz=dt.UTC,
    ).isoformat()
    if rate_limit_wait > RATE_LIMIT_MAX_WAIT:
        _emit(
            f"ERROR rate limit for {user.handle} ({user.did}) resets at {resume_at}"
            + f" ({rate_limit_wait.total_seconds():.0f}s), exceeds max wait of"
            + f" {RATE_LIMIT_MAX_WAIT.total_seconds():.0f}s — aborting",
            quiet=is_quiet,
            force=True,
        )
        summary.failed += 1
        failures.append(f"{user.handle} ({user.did})")
        return False

    _emit(
        f"RATE LIMITED: pausing until {resume_at} ({rate_limit_wait.total_seconds():.0f}s)...",
        quiet=is_quiet,
        force=True,
    )
    time.sleep(rate_limit_wait.total_seconds())
    summary.retries += 1
    return True


def _pause_before_block_retry(
    *,
    attempt: int,
    user: Member,
    summary: BlockSummary,
) -> None:
    """Sleep before retrying a transient block failure.

    Args:
        attempt: One-based retry attempt number.
        user: Starter pack member being blocked.
        summary: Mutable run summary updated with the retry count.
    """

    summary.retries += 1
    base_seconds = BASE_BACKOFF.total_seconds()
    max_seconds = MAX_BACKOFF.total_seconds()
    backoff_seconds = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
    backoff = dt.timedelta(seconds=backoff_seconds)
    jitter_seconds = uniform(*(sec.total_seconds() for sec in JITTER))
    wait = backoff + dt.timedelta(seconds=jitter_seconds)
    print(
        f"WARN transient error for {user.handle} ({user.did}); retry "
        + f"{attempt}/{MAX_BLOCK_ATTEMPTS} in {wait.total_seconds():.2f}s"
    )
    time.sleep(wait.total_seconds())


def _block_user_with_retries(
    *,
    client: Client,
    user: Member,
    summary: BlockSummary,
    failures: list[str],
    skipped: list[str],
    is_verbose: bool,
    is_quiet: bool = False,
    reauth: Callable[[], bool] | None = None,
) -> BlockOutcome:
    """Block one user, retrying transient failures.

    Outcomes:

    - ``BlockOutcome.BLOCKED`` — record created and a non-empty ``cid`` returned.
    - ``BlockOutcome.SKIPPED_INVALID`` — the PDS returned a permanent
      ``BadRequestError`` (e.g. deleted account, malformed DID). The retry
      budget is not spent on these.
    - ``BlockOutcome.FAILED`` — the retry budget was exhausted on transient
      errors, or a non-transient ``RequestException`` was raised.
    - Raises :class:`BlockListCapError` when the PDS reports the per-account
      block list cap. ``block_users`` catches this to abort the entire run.

    Args:
        client: Authenticated AT Protocol client.
        user: Starter pack member to block.
        summary: Mutable run summary updated with retries, skips, and failures.
        failures: Mutable list receiving failed account entries.
        skipped: Mutable list receiving skipped-invalid account entries.
        is_verbose: When ``True``, include detailed error text in output.
        is_quiet: When ``True``, suppress per-account output lines.
        reauth: Optional one-shot re-authentication callback for 401s.

    Returns:
        The outcome of the block attempt.
    """

    attempt = 0
    while attempt < MAX_BLOCK_ATTEMPTS:
        try:
            response = create_block_record(client, user.did)
        except UnauthorizedError as error:
            if reauth is not None and reauth():
                continue
            _record_block_failure(
                summary=summary,
                failures=failures,
                user=user,
                error=error,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return BlockOutcome.FAILED
        except BadRequestError as error:
            if is_block_list_cap_error(error):
                raise BlockListCapError(user.did, user.handle) from error
            summary.skipped_invalid += 1
            skipped.append(f"{user.handle} ({user.did})")
            if is_verbose:
                _emit(
                    f"SKIP invalid {user.handle} ({user.did}) -> {describe_error(error)}",
                    quiet=is_quiet,
                )
            else:
                _emit(
                    f"SKIP invalid {user.handle} ({user.did})",
                    quiet=is_quiet,
                )
            return BlockOutcome.SKIPPED_INVALID
        except AtProtocolError as error:
            if not is_transient_error(error):
                _record_block_failure(
                    summary=summary,
                    failures=failures,
                    user=user,
                    error=error,
                    is_verbose=is_verbose,
                    is_quiet=is_quiet,
                )
                return BlockOutcome.FAILED

            if attempt + 1 >= MAX_BLOCK_ATTEMPTS:
                _record_block_failure(
                    summary=summary,
                    failures=failures,
                    user=user,
                    error=error,
                    is_verbose=is_verbose,
                    is_quiet=is_quiet,
                )
                return BlockOutcome.FAILED

            rate_limit_result = _pause_for_rate_limit_if_needed(
                error=error,
                user=user,
                summary=summary,
                failures=failures,
                is_quiet=is_quiet,
            )
            if rate_limit_result is False:
                return BlockOutcome.FAILED
            if rate_limit_result is True:
                continue

            attempt += 1
            _pause_before_block_retry(
                attempt=attempt,
                user=user,
                summary=summary,
            )
            continue
        except Exception as error:  # noqa: BLE001
            _record_block_failure(
                summary=summary,
                failures=failures,
                user=user,
                error=error,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return BlockOutcome.FAILED

        if not getattr(response, "cid", None):
            _record_block_failure(
                summary=summary,
                failures=failures,
                user=user,
                error=RuntimeError("Block record created without a CID"),
                is_verbose=is_verbose,
                is_quiet=is_quiet,
            )
            return BlockOutcome.FAILED
        return BlockOutcome.BLOCKED

    return BlockOutcome.FAILED


def _emit(msg: str, *, quiet: bool, force: bool = False) -> None:
    """Print ``msg`` to stdout unless ``quiet`` is set.

    Operational lines (rate limits, warnings, errors, cap-reached aborts) call
    with ``force=True`` so they always print even in ``--quiet`` mode.
    Per-account lines (BLOCK / SKIP / DRY BLOCK) call with ``force=False``
    (the default) and are suppressed in ``--quiet`` mode.
    """

    if not quiet or force:
        print(msg)


def _print_progress(
    *,
    completed: int,
    total: int,
    retries: int,
    skipped_invalid: int,
    is_quiet: bool,
    dry_run: bool,
) -> None:
    """Write a periodic progress line to stderr.

    Suppressed when ``is_quiet`` is true, when :data:`PROGRESS_EVERY` is 0,
    or when the interval has not yet been hit. In dry-run the count tracks
    ``would_block`` rather than ``blocked`` so users can still see forward
    progress without any blocks actually happening.

    The line is written with a trailing newline so progress history is
    preserved in non-TTY contexts (CI, redirected stderr).

    Args:
        completed: Number of successful blocks (or would-blocks in dry-run).
        total: Total accounts eligible to be blocked.
        retries: Current retry count.
        skipped_invalid: Current skipped-invalid count.
        is_quiet: When ``True``, do nothing.
        dry_run: When ``True``, label the counter as "would".
    """

    if is_quiet or PROGRESS_EVERY <= 0 or total <= 0:
        return
    if completed == 0 or completed % PROGRESS_EVERY != 0:
        return

    label = "would" if dry_run else "blocked"
    line = (
        f"[{label} {completed}/{total} - retries {retries}"
        f" - skipped {skipped_invalid}]"
    )
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


def block_users(
    client: Client,
    *,
    users: Sequence[Member],
    self_did: str,
    blocked_dids: set[str],
    delay: dt.timedelta,
    dry_run: bool,
    is_verbose: bool,
    is_quiet: bool = False,
    reauth: Callable[[], bool] | None = None,
    summary: BlockSummary,
) -> BlockResult:
    """Block each eligible starter pack member.

    Args:
        client: Authenticated AT Protocol client.
        users: Starter pack members to evaluate.
        self_did: DID of the signed-in account, which is always skipped.
        blocked_dids: Mutable set of DIDs already blocked before processing.
            Successfully blocked DIDs are added to this set.
        delay: Seconds to sleep after each successful block.
        dry_run: When ``True``, print intended actions without creating block
            records.
        is_verbose: When ``True``, print verbose output.
        is_quiet: When ``True``, suppress per-account lines.
        reauth: Optional one-shot re-authentication callback for 401s.
        summary: Mutable run summary to update in place. ``discovered`` is
            set to ``len(users)`` at the start.

    Returns:
        A summary of the run, human-readable failure and skipped entries, and
        a flag indicating whether the block-list cap was reached.
    """

    summary.discovered = len(users)
    failures: list[str] = []
    skipped: list[str] = []
    cap_reached = False

    for user in users:
        did = user.did
        handle = user.handle

        if did == self_did:
            summary.skipped_self += 1
            _emit(f"SKIP self {handle} ({did})", quiet=is_quiet)
            continue

        if did in blocked_dids:
            summary.skipped_already_blocked += 1
            _emit(f"SKIP already blocked {handle} ({did})", quiet=is_quiet)
            continue

        if dry_run:
            summary.would_block += 1
            if is_verbose:
                _emit(f"DRY BLOCK {handle} ({did})", quiet=is_quiet)
            _print_progress(
                completed=summary.would_block,
                total=sum(
                    1
                    for u in users
                    if u.did != self_did and u.did not in blocked_dids
                ),
                retries=summary.retries,
                skipped_invalid=summary.skipped_invalid,
                is_quiet=is_quiet,
                dry_run=True,
            )
            continue

        try:
            outcome = _block_user_with_retries(
                client=client,
                user=user,
                summary=summary,
                failures=failures,
                skipped=skipped,
                is_verbose=is_verbose,
                is_quiet=is_quiet,
                reauth=reauth,
            )
        except BlockListCapError:
            summary.failed += 1
            failures.append(f"{user.handle} ({user.did})")
            cap_reached = True
            _emit(
                f"ERROR block list cap reached while blocking {handle} ({did})",
                quiet=is_quiet,
                force=True,
            )
            _emit(
                "Aborting: no further block attempts will be made.",
                quiet=is_quiet,
                force=True,
            )
            break

        if outcome is BlockOutcome.BLOCKED:
            blocked_dids.add(did)
            summary.blocked += 1
            _emit(f"BLOCK {handle} ({did})", quiet=is_quiet)
            _print_progress(
                completed=summary.blocked,
                total=summary.discovered,
                retries=summary.retries,
                skipped_invalid=summary.skipped_invalid,
                is_quiet=is_quiet,
                dry_run=False,
            )
            if delay > ZERO_DURATION:
                time.sleep(delay.total_seconds())

    return BlockResult(
        summary=summary,
        failures=failures,
        skipped=skipped,
        cap_reached=cap_reached,
    )


def print_summary(result: BlockResult, dry_run: bool) -> None:
    """Print the run summary and any failed entries.

    Args:
        result: Result of the blocking operations.
        dry_run: Whether the run was executed in dry-run mode.
    """

    summary = result.summary
    failures = result.failures
    skipped = result.skipped

    print("\nSummary")
    print(f"Members discovered: {summary.discovered}")
    print(f"Skipped self: {summary.skipped_self}")
    print(f"Skipped already blocked: {summary.skipped_already_blocked}")
    print(f"Skipped invalid: {summary.skipped_invalid}")

    if dry_run:
        print(f"Would block: {summary.would_block}")
    else:
        print(f"Blocked successfully: {summary.blocked}")

    print(f"Failures: {summary.failed}")
    print(f"Retries used: {summary.retries}")
    print(f"Re-authentications: {summary.reauths}")

    if result.cap_reached:
        print(
            "\nBlock list cap reached: further block attempts were skipped."
            " Bluesky limits the number of accounts a single account can block;"
            " unblock some accounts to continue."
        )

    if failures:
        print("\nFailed entries:")
        for failure in failures:
            print(f"- {failure}")

    if skipped:
        print("\nSkipped (invalid) entries:")
        for entry in skipped:
            print(f"- {entry}")


def main() -> None:
    """Run the command-line workflow.

    Exit codes:
        0 — every member processed without failures.
        1 — at least one block attempt failed.
        2 — user aborted the destructive confirmation, or stdin was not a TTY
            without ``--yes``.
        3 — the PDS block-list cap was reached and the run was aborted.

    Raises:
        SystemExit: When the exit code is non-zero.
    """

    args = parse_args()

    handle = resolve_handle()
    app_password = resolve_app_password()
    client, self_did = login(handle, app_password)

    summary = BlockSummary()
    reauth = _make_reauth_fn(client, handle, app_password, summary)

    source_inputs: list[str]
    if args.input is not None:
        source_inputs = [args.input]
    else:
        source_inputs = load_inputs_from_file(args.file)

    merged: dict[str, Member] = {}
    skipped_inputs: list[str] = []
    for s_input in source_inputs:
        try:
            target = resolve_input_to_list_target(
                client, s_input, reauth=reauth
            )
            print(f"INFO: Using {target.source_kind} {target.list_uri}")
            pack_members = fetch_list_members(
                client, target.list_uri, reauth=reauth
            )
        except Exception as error:
            if not is_input_unrecoverable(error):
                raise
            skipped_inputs.append(s_input)
            print(f"WARN: input {s_input} skipped: {describe_error(error)}")
            continue
        print(f"INFO:   Loaded {len(pack_members)} members from this input")
        merge_unique_members(merged, pack_members)
    users = list(merged.values())
    print(
        f"INFO: Loaded {len(users)} unique members across "
        f"{len(source_inputs)} input source(s)"
    )
    if skipped_inputs:
        print(
            f"INFO: Skipped {len(skipped_inputs)} input source(s) due to"
            " bad request errors"
        )

    blocked_dids = fetch_blocked_dids(client, reauth=reauth)

    if not args.dry_run and not args.yes:
        to_block = sum(
            1
            for user in users
            if user.did != self_did and user.did not in blocked_dids
        )
        if to_block > 0 and not confirm_destructive(to_block):
            print("Aborted by user.", file=sys.stderr)
            raise SystemExit(2)
        print(f"INFO: confirmed, blocking {to_block} accounts")

    result = block_users(
        client,
        users=users,
        self_did=self_did,
        blocked_dids=blocked_dids,
        delay=args.delay,
        dry_run=args.dry_run,
        is_verbose=args.verbose,
        is_quiet=args.quiet,
        reauth=reauth,
        summary=summary,
    )
    print_summary(result, args.dry_run)

    if result.cap_reached:
        raise SystemExit(3)
    if result.summary.failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
