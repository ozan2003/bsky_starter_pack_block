#!/usr/bin/env python3
"""Block every account listed in one or more Bluesky starter packs.

This script logs in to Bluesky with an app password, resolves each starter
pack input to a backing list, loads every account across those lists (merging
unique members by account), skips your own account and accounts you already
block, then creates block records for the remaining accounts.

Supported starter pack inputs:
    - ``at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>``
    - ``https://bsky.app/start/<did-or-handle>/<rkey>``
    - ``https://bsky.app/starter-pack/<did-or-handle>/<rkey>``
    - ``https://bsky.app/starter-pack-short/<code>``
    - ``https://go.bsky.app/<code>``

Usage:
    First create an app password in Bluesky under Settings -> Privacy and
    Security -> App Passwords. Prefer storing it in an environment variable so
    it does not appear in shell history or process listings:

    ``export BSKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"``

    Run a dry run before blocking:

    ``python3 bsky.py --handle user.bsky.social --pack <url-or-at-uri> --dry-run``

    Or load pack inputs from a file (one input per line):

    ``python3 bsky.py --handle user.bsky.social --pack-file packs.txt --dry-run``

    ``--pack`` and ``--pack-file`` are mutually exclusive.

    If the dry run looks correct, run without ``--dry-run``:

    ``python3 bsky.py --handle user.bsky.social --pack <url-or-at-uri>``

    You can pass ``--delay`` to control the pause between block operations.
    ``--app-password`` is supported, but the environment variable is safer.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isinf, isnan
from pathlib import Path
from random import choice, uniform
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from atproto import Client, models

STARTER_PACK_COLLECTION = "app.bsky.graph.starterpack"
SUPPORTED_STARTER_PACK_PATHS = {"start", "starter-pack"}
BSKY_APP_HOSTS = {"bsky.app", "www.bsky.app"}
BSKY_SHORT_LINK_HOST = "go.bsky.app"
STARTER_PACK_SHORT_PATH = "starter-pack-short"

# Socket timeout for urllib when resolving short links (go.bsky.app redirects).
SHORT_LINK_TIMEOUT_SECONDS = 10.0

# Maximum records per page for listRepos/listRecords-style reads.
LIST_PAGE_SIZE = 100

# Same pagination limit when enumerating existing blocks to skip already-blocked DIDs.
BLOCKS_PAGE_SIZE = 100

# Successful blocks are followed by ``time.sleep(delay)``, CLI default when ``--delay`` omitted.
DEFAULT_DELAY_SECONDS = 0.5

# Retries after transient failures in ``block_users`` use capped exponential backoff + jitter.
MAX_BLOCK_RETRIES = 4  # Highest ``attempt`` index allowed before giving up (matches ``while attempt <= ...``).
BASE_BACKOFF_SECONDS = 1.0  # Wait ``min(MAX, BASE * 2**(attempt - 1))`` seconds before each retry.
# Upper bound so backoff does not grow past a lot.
MAX_BACKOFF_SECONDS = 8.0

# Uniform random extra delay on each backoff (seconds), reduces synchronized retries.
JITTER_SECONDS = (0.0, 0.6)

# Error for non-existent packs or invalid DIDs.
HTTP_STATUS_BAD_REQUEST = 400
# HTTP response codes treated as retryable alongside network errors (see ``is_transient_error``).
HTTP_STATUS_TOO_MANY_REQUESTS = 429
# Any status greater than or equal to this is treated as a transient server error.
HTTP_STATUS_SERVER_ERROR_MIN = 500

# Rate-limit pause: small buffer added to the computed wait so the retry lands after the window resets.
RATE_LIMIT_BUFFER_SECONDS = 2.0
# If the server asks to wait longer than this, abort instead of blocking the terminal.
RATE_LIMIT_MAX_WAIT_SECONDS = 900.0  # 15 minutes
# User agent is initialized once at invocation.
USER_AGENT = choice(
    (
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/531.2 (KHTML, like Gecko) Chrome/35.0.862.0 Safari/531.2",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1 rv:5.0; ja-JP) AppleWebKit/535.6.1 (KHTML, like Gecko) Version/5.0.3 Safari/535.6.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/32.0.816.0 Safari/535.1",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_11_7) AppleWebKit/533.2 (KHTML, like Gecko) Chrome/58.0.898.0 Safari/533.2",
        "Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 5.2; Trident/5.0)",
        "Opera/9.24.(Windows NT 6.2; sa-IN) Presto/2.9.172 Version/10.00",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; rv:1.9.4.20) Gecko/6174-02-20 01:19:12.425873 Firefox/7.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:1.9.7.20) Gecko/8960-12-16 18:15:36.475525 Firefox/3.8",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5 rv:6.0; kl-GL) AppleWebKit/532.19.4 (KHTML, like Gecko) Version/5.0.1 Safari/532.19.4",
        "Mozilla/5.0 (Linux; Android 2.3.5) AppleWebKit/534.1 (KHTML, like Gecko) Chrome/56.0.885.0 Safari/534.1",
    )
)


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
        would_block: Number of members that would be blocked in dry-run mode.
        blocked: Number of block records created successfully.
        failed: Number of members that could not be blocked.
        retries: Number of retry attempts used for transient block failures.
    """

    discovered: int = 0
    skipped_self: int = 0
    skipped_already_blocked: int = 0
    would_block: int = 0
    blocked: int = 0
    failed: int = 0
    retries: int = 0


@dataclass(frozen=True, slots=True)
class PackReference:
    """Canonical starter pack reference before DID normalization.

    Attributes:
        identifier: Starter pack creator DID or handle.
        rkey: Starter pack record key.
    """

    identifier: str
    rkey: str


@dataclass(frozen=True, slots=True)
class ShortStarterPackLink:
    """Bluesky short link that must be resolved before API use.

    Attributes:
        url: Canonical short-link service URL.
    """

    url: str


type PackInput = PackReference | ShortStarterPackLink


def parse_delay(value: str) -> float:
    """Parse a CLI delay value.

    Args:
        value: Raw command-line value for ``--delay``.

    Returns:
        A finite, non-negative delay in seconds.

    Raises:
        argparse.ArgumentTypeError: If the delay is negative, infinite, or NaN.
        ValueError: If ``value`` cannot be parsed as a float.
    """

    delay = float(value)
    if delay < 0:
        msg = "--delay must be greater than or equal to 0"
        raise argparse.ArgumentTypeError(msg)
    if isinf(delay) or isnan(delay):
        msg = "--delay must be a finite number"
        raise argparse.ArgumentTypeError(msg)
    return delay


def parse_args() -> argparse.Namespace:
    """Parse command-line options.

    Returns:
        Parsed command-line arguments for login, one or more starter pack
        inputs, throttling, and dry-run mode.
    """

    parser = argparse.ArgumentParser(
        description="Block all users from one or more Bluesky starter packs",
    )

    parser.add_argument(
        "--handle",
        required=True,
        type=str,
        help="Bluesky handle (e.g. user.bsky.social)",
    )

    parser.add_argument(
        "--app-password",
        type=str,
        default=None,
        help="Bluesky app password (or set BSKY_APP_PASSWORD)",
    )

    pack_input_group = parser.add_mutually_exclusive_group(required=True)

    pack_input_group.add_argument(
        "--pack",
        type=str,
        help="Single starter pack URL or AT URI",
    )

    pack_input_group.add_argument(
        "--pack-file",
        type=str,
        help="Path to a UTF-8 text file with one starter pack URL or AT URI per line",
    )

    parser.add_argument(
        "--delay",
        type=parse_delay,
        default=DEFAULT_DELAY_SECONDS,
        help="Delay between blocks (seconds)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print users without blocking",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print verbose output",
        default=False,
    )

    return parser.parse_args()


def load_pack_inputs_from_file(path: str) -> list[str]:
    """Load starter pack inputs from a UTF-8 text file.

    Note that it won't do any normalization or validation of the inputs.
    Refer to ``normalize_starter_pack_uri`` for more information.

    Args:
        path: File path containing one starter pack input per line.

    Returns:
        Non-empty starter pack input lines with surrounding whitespace removed.

    Raises:
        ValueError: If the file cannot be read or contains no usable pack lines.
    """

    pack_file_path = Path(path)
    try:
        with pack_file_path.open(encoding="utf-8") as pack_file:
            pack_inputs = [
                stripped for line in pack_file if (stripped := line.strip())
            ]
    except OSError as error:
        msg = f"Could not read pack file {pack_file_path}: {error}"
        raise ValueError(msg) from error

    if pack_inputs:
        return pack_inputs

    msg = (
        f"Pack file {pack_file_path} does not contain any starter pack inputs"
    )
    raise ValueError(msg)


def resolve_app_password(cli_password: str | None) -> str:
    """Resolve the Bluesky app password from CLI input or environment.

    Args:
        cli_password: Password passed through ``--app-password``.

    Returns:
        The app password to use for login.

    Raises:
        ValueError: If no password was provided and ``BSKY_APP_PASSWORD`` is not
            set.
    """

    if cli_password:
        return cli_password

    env_password = os.getenv("BSKY_APP_PASSWORD")
    if env_password:
        return env_password

    msg = "Missing app password. Provide --app-password or set BSKY_APP_PASSWORD."
    raise ValueError(msg)


def starter_pack_format_error() -> str:
    """Build the supported starter pack input error message.

    Returns:
        A message listing every supported starter pack input format.
    """

    return (
        "Starter pack input must be one of: "
        "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>, "
        "https://bsky.app/start/<did-or-handle>/<rkey>, "
        "https://bsky.app/starter-pack/<did-or-handle>/<rkey>, "
        "https://bsky.app/starter-pack-short/<code>, "
        "or https://go.bsky.app/<code>"
    )


def parse_at_uri(raw: str) -> PackReference | None:
    """Parse a starter pack AT URI.

    Args:
        raw: User-provided input after whitespace trimming.

    Returns:
        A starter pack reference when ``raw`` is an AT URI, otherwise ``None``.

    Raises:
        ValueError: If ``raw`` is an AT URI but has the wrong shape or
            collection.
    """

    if not raw.startswith("at://"):
        return None

    parts = [part for part in raw[len("at://") :].split("/") if part]
    if len(parts) != 3:
        msg = (
            "AT URI must be in the format "
            "at://<did-or-handle>/app.bsky.graph.starterpack/<rkey>"
        )
        raise ValueError(msg)

    identifier, collection, rkey = parts
    if collection != STARTER_PACK_COLLECTION:
        msg = f"Starter pack AT URI must use collection {STARTER_PACK_COLLECTION}"
        raise ValueError(msg)

    return PackReference(identifier=identifier, rkey=rkey)


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
    return PackReference(identifier=identifier, rkey=rkey)


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
    """Parse any supported starter pack input format.

    Args:
        pack_input: Starter pack AT URI, canonical Bluesky URL, or short link.

    Returns:
        A canonical pack reference, or a short-link reference that still needs
        network resolution.

    Raises:
        ValueError: If the input is empty, uses an unsupported scheme or host,
            or does not match a supported starter pack format.
    """

    raw = pack_input.strip()
    if not raw:
        msg = "Starter pack input cannot be empty"
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
        raise ValueError(starter_pack_format_error())

    if host is not None and host not in {
        *BSKY_APP_HOSTS,
        BSKY_SHORT_LINK_HOST,
    }:
        msg = f"Unsupported starter pack URL host: {host}"
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

    # Short links do not contain the DID/rkey pair. Return a marker object so
    # normalization can resolve the link before trying to build an AT URI.
    short_link = parse_short_pack_path(host, path_source)
    if short_link is not None:
        return short_link

    raise ValueError(starter_pack_format_error())


def resolve_short_starter_pack_url(short_link: ShortStarterPackLink) -> str:
    """Resolve a Bluesky short link to its canonical starter pack URL.

    Args:
        short_link: Validated Bluesky starter pack short link.

    Returns:
        The canonical URL returned by the short-link service.

    Raises:
        ValueError: If the short-link URL is not an HTTPS ``go.bsky.app`` URL.
        RuntimeError: If the short link cannot be resolved or resolves to an
            unusable response.
    """

    parsed = urlparse(short_link.url)
    if parsed.scheme != "https" or parsed.hostname != BSKY_SHORT_LINK_HOST:
        msg = f"Unsupported starter pack short link URL: {short_link.url}"
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
        with urlopen(request, timeout=SHORT_LINK_TIMEOUT_SECONDS) as response:  # noqa: S310
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
        msg = f"Could not resolve starter pack short link {short_link.url}: HTTP {error.code}"
        raise RuntimeError(msg) from error
    except (OSError, TimeoutError, URLError, json.JSONDecodeError) as error:
        msg = f"Could not resolve starter pack short link {short_link.url}: {error}"
        raise RuntimeError(msg) from error

    msg = f"Short link did not resolve to a starter pack URL: {short_link.url}"
    raise RuntimeError(msg)


def resolve_identifier_to_did(client: Client, identifier: str) -> str:
    """Resolve a handle-like identifier to a DID.

    Args:
        client: Authenticated AT Protocol client.
        identifier: DID or Bluesky handle.

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
    )
    if response.did:
        return response.did

    msg = f"Could not resolve handle to DID: {identifier}"
    raise RuntimeError(msg)


def normalize_starter_pack_uri(client: Client, pack_input: str) -> str:
    """Normalize a starter pack input into the AT URI expected by the API.

    Args:
        client: Authenticated AT Protocol client used for handle-to-DID
            resolution.
        pack_input: Starter pack AT URI, canonical Bluesky URL, or short link.

    Returns:
        A canonical starter pack AT URI using the creator DID.

    Raises:
        RuntimeError: If short-link resolution loops too many times.
        ValueError: If the starter pack input format is unsupported.
    """

    current_input = pack_input
    for _ in range(3):
        parsed_input = parse_pack_input(current_input)
        if isinstance(parsed_input, PackReference):
            # The graph API requires the creator DID in the AT URI; web links
            # may contain a handle, so resolve that as the final parse step.
            did = resolve_identifier_to_did(client, parsed_input.identifier)
            return f"at://{did}/{STARTER_PACK_COLLECTION}/{parsed_input.rkey}"

        # A short link resolves to another supported input format, usually
        # https://bsky.app/start/<did>/<rkey>, so loop back through the parser.
        current_input = resolve_short_starter_pack_url(parsed_input)

    msg = f"Starter pack short link resolution loop exceeded for {pack_input}"
    raise RuntimeError(msg)


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


def fetch_starter_pack_list_uri(client: Client, at_uri: str) -> str:
    """Fetch the backing list URI for a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.

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


def fetch_members(client: Client, at_uri: str) -> list[Member]:
    """Load all unique account members from a starter pack.

    Args:
        client: Authenticated AT Protocol client.
        at_uri: Starter pack AT URI.

    Returns:
        Unique starter pack members keyed by DID.

    Raises:
        RuntimeError: If the backing list URI cannot be resolved
            (see ``fetch_starter_pack_list_uri``).
    """

    list_uri = fetch_starter_pack_list_uri(client, at_uri)
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


def fetch_blocked_dids(client: Client) -> set[str]:
    """Load all DIDs already blocked by the signed-in account.

    Args:
        client: Authenticated AT Protocol client.

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

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def create_block_record(client: Client, did: str) -> None:
    """Create a Bluesky block record for one account.

    Args:
        client: Authenticated AT Protocol client.
        did: DID of the account to block.

    Raises:
        RuntimeError: If the client does not expose the signed-in repo DID when
            the low-level block fallback is needed.
    """

    block_method = getattr(client, "block", None)
    if callable(block_method):
        block_method(did)
        return

    repo_did = getattr(getattr(client, "me", None), "did", None)
    if not isinstance(repo_did, str) or not repo_did:
        msg = "Unable to determine repo DID for block operation"
        raise RuntimeError(msg)

    record_data: dict[str, str] = {
        "subject": did,
        "created_at": current_time_iso(client),
    }
    record = models.AppBskyGraphBlock.Record.model_validate(record_data)
    client.app.bsky.graph.block.create(
        repo_did,
        record,
    )


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


def extract_response_headers(error: Exception) -> dict[str, str]:
    """Extract response headers from an SDK exception.

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
        return cast(dict[str, str], headers)
    return {}


def extract_rate_limit_wait(error: Exception) -> float | None:
    """Compute how long to sleep before the rate-limit window resets.

    Reads the ``ratelimit-reset`` header (Unix epoch seconds) first, then
    falls back to the standard ``retry-after`` header (delta seconds).  A
    small buffer is added so the retry lands safely after the window edge.

    Args:
        error: Exception raised by the AT Protocol client or network layer.

    Returns:
        Seconds to wait (including buffer), or ``None`` when no usable
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
            wait = reset_ts - time.time() + RATE_LIMIT_BUFFER_SECONDS
            return max(wait, RATE_LIMIT_BUFFER_SECONDS)

    retry_after_raw = headers.get("retry-after")
    if retry_after_raw is not None:
        try:
            retry_seconds = float(retry_after_raw)
        except (TypeError, ValueError):
            pass
        else:
            return max(
                retry_seconds + RATE_LIMIT_BUFFER_SECONDS,
                RATE_LIMIT_BUFFER_SECONDS,
            )

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


def is_transient_error(error: Exception) -> bool:
    """Decide whether a block failure should be retried.

    Args:
        error: Exception raised during a block operation.

    Returns:
        ``True`` for network errors, timeouts, rate limits, and server errors;
        otherwise ``False``.
    """

    error_name = type(error).__name__
    if error_name in {"NetworkError", "InvokeTimeoutError"}:
        return True

    status_code = extract_status_code(error)
    if status_code == HTTP_STATUS_TOO_MANY_REQUESTS:
        return True
    if (
        isinstance(status_code, int)
        and status_code >= HTTP_STATUS_SERVER_ERROR_MIN
    ):
        return True

    text = str(error).lower()
    return "rate limit" in text or "temporarily unavailable" in text


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

    return extract_status_code(error) == HTTP_STATUS_BAD_REQUEST


def call_with_rate_limit_retry[T](fn: Callable[[], T], *, context: str) -> T:
    """Call ``fn`` and transparently pause on HTTP 429 rate limits.

    On a 429 response the function reads ``ratelimit-reset`` (or
    ``retry-after``) from the response headers, sleeps until the window
    resets, and retries.  Non-429 exceptions propagate immediately.

    Args:
        fn: Zero-argument callable that performs a single API request.
        context: Human-readable label printed while pausing (e.g.
            ``"fetch members page"``).

    Returns:
        The return value of ``fn`` on success.

    Raises:
        RuntimeError: If the rate-limit wait exceeds
            ``RATE_LIMIT_MAX_WAIT_SECONDS``.
    """

    while True:
        try:
            return fn()
        except Exception as error:
            status_code = extract_status_code(error)
            if status_code != HTTP_STATUS_TOO_MANY_REQUESTS:
                raise

            wait = extract_rate_limit_wait(error)
            if wait is None:
                raise

            if wait > RATE_LIMIT_MAX_WAIT_SECONDS:
                resume_at = datetime.fromtimestamp(
                    time.time() + wait,
                    tz=UTC,
                ).isoformat()
                msg = (
                    f"Rate limit for {context} resets at {resume_at} "
                    f"({wait:.0f}s), exceeds max wait of "
                    f"{RATE_LIMIT_MAX_WAIT_SECONDS:.0f}s"
                )
                raise RuntimeError(msg) from error

            resume_at = datetime.fromtimestamp(
                time.time() + wait,
                tz=UTC,
            ).isoformat()
            print(
                f"RATE LIMITED ({context}): pausing until {resume_at} ({wait:.0f}s)..."
            )
            time.sleep(wait)


@dataclass(slots=True)
class BlockResult:
    """Result of the blocking operations.

    Attributes:
        summary: Summary of the blocking operations.
        failures: Human-readable failure entries.
    """

    summary: BlockSummary
    failures: list[str]


def block_users(
    client: Client,
    *,
    users: list[Member],
    self_did: str,
    blocked_dids: set[str],
    delay: float,
    dry_run: bool,
    is_verbose: bool,
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

    Returns:
        A summary of the run and human-readable failure entries.
    """

    summary = BlockSummary(discovered=len(users))
    failures: list[str] = []

    for user in users:
        did = user.did
        handle = user.handle

        if did == self_did:
            summary.skipped_self += 1
            print(f"SKIP self {handle} ({did})")
            continue

        if did in blocked_dids:
            summary.skipped_already_blocked += 1
            print(f"SKIP already blocked {handle} ({did})")
            continue

        if dry_run:
            summary.would_block += 1
            if is_verbose:
                print(f"DRY BLOCK {handle} ({did})")
            continue

        has_succeeded = False
        attempt = 0
        while attempt <= MAX_BLOCK_RETRIES:
            try:
                create_block_record(client, did)
                has_succeeded = True
                break
            except Exception as error:  # noqa: BLE001
                if (
                    not is_transient_error(error)
                    or attempt == MAX_BLOCK_RETRIES
                ):
                    error_text = describe_error(error)
                    summary.failed += 1
                    failures.append(f"{handle} ({did})")
                    if is_verbose:
                        print(f"ERROR {handle} ({did}) -> {error_text}")
                    else:
                        print(f"ERROR {handle} ({did})")
                    break

                status_code = extract_status_code(error)
                is_rate_limited = status_code == HTTP_STATUS_TOO_MANY_REQUESTS

                if is_rate_limited:
                    rate_limit_wait = extract_rate_limit_wait(error)
                    if rate_limit_wait is not None:
                        if rate_limit_wait > RATE_LIMIT_MAX_WAIT_SECONDS:
                            resume_at = datetime.fromtimestamp(
                                time.time() + rate_limit_wait,
                                tz=UTC,
                            ).isoformat()
                            print(
                                f"ERROR rate limit for {handle} ({did}) resets at {resume_at}"
                                + f" ({rate_limit_wait:.0f}s), exceeds max wait of"
                                + f" {RATE_LIMIT_MAX_WAIT_SECONDS:.0f}s — aborting"
                            )
                            summary.failed += 1
                            failures.append(f"{handle} ({did})")
                            break

                        resume_at = datetime.fromtimestamp(
                            time.time() + rate_limit_wait,
                            tz=UTC,
                        ).isoformat()
                        print(
                            f"RATE LIMITED: pausing until {resume_at} ({rate_limit_wait:.0f}s)..."
                        )
                        time.sleep(rate_limit_wait)
                        summary.retries += 1
                        continue

                attempt += 1
                summary.retries += 1
                backoff = min(
                    MAX_BACKOFF_SECONDS,
                    BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                )
                wait_seconds = backoff + uniform(*JITTER_SECONDS)
                print(
                    f"WARN transient error for {handle} ({did}); retry {attempt}/{MAX_BLOCK_RETRIES} in {wait_seconds:.2f}s"
                )
                time.sleep(wait_seconds)

        if has_succeeded:
            blocked_dids.add(did)
            summary.blocked += 1
            print(f"BLOCK {handle} ({did})")
            if delay > 0:
                time.sleep(delay)

    return BlockResult(summary=summary, failures=failures)


def print_summary(result: BlockResult, dry_run: bool) -> None:
    """Print the run summary and any failed entries.

    Args:
        result: Result of the blocking operations.
        dry_run: Whether the run was executed in dry-run mode.
    """

    summary = result.summary
    failures = result.failures

    print("\nSummary")
    print(f"Members discovered: {summary.discovered}")
    print(f"Skipped self: {summary.skipped_self}")
    print(f"Skipped already blocked: {summary.skipped_already_blocked}")

    if dry_run:
        print(f"Would block: {summary.would_block}")
    else:
        print(f"Blocked successfully: {summary.blocked}")

    print(f"Failures: {summary.failed}")
    print(f"Retries used: {summary.retries}")

    if failures:
        print("\nFailed entries:")
        for failure in failures:
            print(f"- {failure}")


def main() -> None:
    """Run the command-line workflow.

    Raises:
        SystemExit: If at least one block operation failed.
    """

    args = parse_args()

    app_password = resolve_app_password(args.app_password)
    client, self_did = login(args.handle, app_password)

    pack_inputs: list[str]
    if args.pack is not None:
        pack_inputs = [args.pack]
    else:
        pack_inputs = load_pack_inputs_from_file(args.pack_file)

    merged: dict[str, Member] = {}
    skipped_packs: list[str] = []
    for pack_input in pack_inputs:
        try:
            at_uri = normalize_starter_pack_uri(client, pack_input)
            print(f"Using starter pack {at_uri}")
            pack_members = fetch_members(client, at_uri)
        except Exception as error:
            if not is_bad_request_skip(error):
                raise
            skipped_packs.append(pack_input)
            print(f"SKIP starter pack {pack_input}: {describe_error(error)}")
            continue
        print(f"\t- Loaded {len(pack_members)} members from this pack")
        merge_unique_members(merged, pack_members)
    users = list(merged.values())
    print(
        f"Loaded {len(users)} unique members across {len(pack_inputs)} starter pack(s)"
    )
    if skipped_packs:
        print(
            f"Skipped {len(skipped_packs)} starter pack(s) due to bad request errors"
        )

    blocked_dids = fetch_blocked_dids(client)

    result = block_users(
        client,
        users=users,
        self_did=self_did,
        blocked_dids=blocked_dids,
        delay=args.delay,
        dry_run=args.dry_run,
        is_verbose=args.verbose,
    )
    print_summary(result, args.dry_run)

    if result.summary.failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
