# Regression tests

From the plugin directory, run:

```sh
lua5.1 tests/run.lua
luac5.1 -p PhotoDeck*.lua
git diff --check
```

The tests mock Lightroom and HTTP. They never access the network, use real
credentials or change a Lightroom catalog. The small upload fixture is not an
actual image; only its transfer bytes matter to these tests.

`tests/run.lua` is grouped into sections, from JSON and HTTP behavior to API
operations, uploads, Lightroom integration and localization. The same headings
appear in the output. Failures include a source traceback and bounded value
previews rather than dumping binary upload contents.

The mocked API uses `api.example.test`, independently of the plugin's configured
host. Time advances through a fake clock, so retry and throttle tests do not wait
in real time.

Localization checks compare the French and German dictionaries with the source
keys, rejecting missing, stale, duplicate or malformed entries and mismatched
placeholders and formatting markers.

Storage tests also cover multi-chunk binary streams, file-handle preparation and
cleanup outside native byte-getter callbacks, and multipart POST uploads through
`LrHttp.postMultipart`. The mock checks the stream's SDK `totalSize` argument.

## Lightroom smoke tests (Windows and macOS)

Mocked tests do not replace testing the native HTTP stack and catalog callbacks.
Before releasing, use disposable resources and test accounts to verify:

1. Login, wrong password, OTP, canceled OTP, expired session, logout and switching
   between accounts/sites. Confirm all routing/session cookies are reused.
2. Existing catalogs/presets: publish to multiple galleries, virtual copies,
   metadata-only republish, file replacement and recovery after remote deletion.
3. JPEG, TIFF, DNG, original RAW and video uploads, MIME/filename corrections,
   PhotoDeck watermark selection and upload failure/queue cancellation.
4. Gallery/folder import and settings, introduction clearing, rename, move,
   deletion, duplicate names and manual media ordering. Check localized and
   integration-mode public URLs.
5. Pagination, 3,000/3,001-item removals and partial batch failures. Lightroom
   acknowledges queued operations; background completion is not verified.
6. Trusted 307 handling and throttling, including long Retry-After values. Confirm
   the native stack neither forwards API credentials/cookies to storage nor
   automatically follows redirects to untrusted hosts with credentials.

For upload failures, the `-> storage` / `<- storage` log entries include the method,
transfer mode, file size, location identifier, HTTP status and sanitized SDK error
codes. Signed URLs, form signatures, response bodies and raw exception messages
are omitted. Use these entries when reporting a native upload error.

Do not put credentials, cookie values, signed storage URLs or payloads in logs.
