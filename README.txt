============================================================
  ZILONG BOT — FIXES BUNDLE
============================================================

COMPLETE FILE REPLACEMENTS (drop these straight in):
  plugins/admin.py          — owner-only gate + all audit fixes
  plugins/hardsub.py        — start_hardsub_for_url + stop_propagation fix
  services/downloader.py    — critical timeout bug + import cleanup
  services/task_runner.py   — stats cache, _dirty removed, auto_panel guard

TARGETED PATCHES (see PATCHES.txt for exact before/after):
  plugins/url_handler.py    — magnet probe eviction, hardsub bg task, tmp cleanup
  main.py                   — hardcoded 5 → MAX_CONCURRENT
  services/telegraph.py     — token path survives EC2 reboots
  services/cloudconvert_hook.py — startup warning when no WEBHOOK_SECRET
  services/utils.py         — smart_clean_filename position guard

============================================================
  WHAT EACH FIX ADDRESSES
============================================================

🔴 CRITICAL
  [admin.py]       Owner-only gate — non-owners see "contact @kingkum_1"
  [hardsub.py]     start_hardsub_for_url() added — cc_buttons.py was importing
                   a function that never existed, crashing bot at startup
  [downloader.py]  timeout variable was computed but hardcoded TOTAL_TIMEOUT
                   (6 h) was always used — dead magnets now fail in 3 minutes

🟠 HIGH
  [hardsub.py]     Wrong file type during waiting_subtitle now sends an error
                   and stops propagation instead of falling through to media_router
  [url_handler.py] Magnet hardsub now runs as background asyncio task — the
                   callback no longer blocks while aria2c downloads
  [url_handler.py] _magnet_probe sessions now timestamped and evicted after
                   30 min so /tmp doesn't grow unbounded
  [url_handler.py] tmp dir cleaned up on error in ccv_resolution_cb
  [task_runner.py] render_panel no longer calls system_stats() — that slept
                   250ms inside the live panel loop. Stats are now updated by
                   a background task every 5 s and read synchronously.

🟡 MEDIUM
  [task_runner.py] TaskRecord._dirty (asyncio.Event) removed — was set but
                   never awaited anywhere, pure dead code
  [task_runner.py] auto_panel wakes existing live panels instead of always
                   spawning a new one (prevents race when 2 tasks finish together)
  [main.py]        Hardcoded 5 → MAX_CONCURRENT constant
  [utils.py]       smart_clean_filename now only strips noise tokens after
                   the 3rd token — prevents eating show names like "TS Online"
  [downloader.py]  urllib.parse moved to module level (was two local aliases)

🔵 LOW
  [telegraph.py]   Token stored in data/telegraph.token (survives EC2 reboots)
  [cloudconvert_hook.py]  Startup warning logged when CC_WEBHOOK_SECRET is empty

============================================================
  OWNER-ONLY GATE — HOW IT WORKS
============================================================

Two handlers in admin.py at group=-1 (highest priority):

  owner_only_gate   — fires on every private message before any other handler.
                      If sender is not OWNER_ID, replies with the contact message
                      and calls stop_propagation() so nothing else runs.

  owner_only_cb_gate — fires on every callback query. Non-owner gets an alert
                       popup and the query is stopped.

The existing ban_gate at group=2 is kept so you can pre-populate the ban list
for future multi-user mode without any code changes.
