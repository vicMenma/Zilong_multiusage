"""
plugins/uploader.py — RE-EXPORT STUB  (BUG-05 fix)

WHY THIS FILE IS JUST A STUB:
  Pyrogram auto-imports every .py file inside the plugins/ directory via the
  `plugins={"root": "plugins"}` argument passed to Client().  The original
  plugins/uploader.py was a full 250-line copy of services/uploader.py.

  Having two complete, identical implementations loaded simultaneously causes:
    1. ~500 lines of dead code executed on every bot start.
    2. Non-deterministic import-order races: whichever module's upload_file()
       wins depends on Pyrogram's internal plugin-loading order.
    3. Any fix applied to services/uploader.py is silently shadowed by the
       stale copy here — future maintainers are confused about which version
       is actually running.

  This stub re-exports the canonical symbols from services/uploader.py so
  that any existing `from plugins.uploader import upload_file` still works,
  while ensuring there is exactly ONE implementation loaded.
"""
from services.uploader import upload_file, TG_MAX_BYTES  # noqa: F401
