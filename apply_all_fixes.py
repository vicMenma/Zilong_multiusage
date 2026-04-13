#!/usr/bin/env python3
"""
apply_all_fixes.py  —  Zilong Bot Audit v2 patch script
========================================================

Applies all targeted fixes from the second audit to a live repository checkout.
Each patch is idempotent: running this script twice is safe.

Usage:
    python apply_all_fixes.py --repo /path/to/Zilong_multiusage

Patches applied:
  BUG-02b  plugins/url_handler.py    ccv_resolution_cb: add finally: cleanup(tmp_conv)
  BUG-03   plugins/stream_extractor.py  se_mag_cb action="file": replace broken aria2p block
  BUG-06   colab_launcher.py         PATCH A regex: ─{10} → ─{10,}
  BUG-07   plugins/video.py          _IGNORED set: add missing commands
  BUG-08   plugins/resize.py         _do_resize / _do_compress: add try/finally cleanup
  BUG-09   plugins/start.py          3 exclusion lists: add missing commands
  BUG-10   plugins/nyaa_tracker.py   _magnet_cache: add TTL eviction

NOTE: BUG-01, BUG-04, BUG-05, BUG-11, BUG-12 are handled by complete file
      replacements in the zilong_audit_v2/ directory — copy those files instead.
"""
from __future__ import annotations

import argparse
import ast
import sys
import textwrap
from pathlib import Path


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _verify_syntax(content: str, label: str) -> bool:
    try:
        ast.parse(content)
        return True
    except SyntaxError as exc:
        print(f"  ❌ SYNTAX ERROR after patch {label}: {exc}")
        return False


def patch(path: Path, old: str, new: str, label: str) -> bool:
    """
    Replace exactly one occurrence of `old` with `new` in `path`.
    Returns True if the patch was applied (or was already applied).
    """
    src = _read(path)
    if old not in src:
        if new in src:
            print(f"  ✅ {label} — already applied, skipping")
            return True
        print(f"  ⚠️  {label} — search string NOT FOUND, skipping")
        return False

    patched = src.replace(old, new, 1)
    if not _verify_syntax(patched, label):
        return False

    _write(path, patched)
    print(f"  ✅ {label} — applied")
    return True


# ─────────────────────────────────────────────────────────────
# BUG-02b: plugins/url_handler.py — ccv_resolution_cb missing finally cleanup
# ─────────────────────────────────────────────────────────────

BUG_02B_OLD = '''\
    try:
        from services.cloudconvert_api import parse_api_keys, pick_best_key, submit_convert
        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
        else:
            key_info = "🔑 1 API key"

        await safe_edit(cb.message,
            f"☁️ <b>Submitting Convert job…</b>\\n"
            "──────────────────────\\n\\n"
            f"🎬 <code>{fname[:40]}</code>\\n"
            f"📐 → <b>{res_label}</b>\\n\\n"
            "<i>Checking API keys…</i>",
            parse_mode=enums.ParseMode.HTML)

        job_id = await submit_convert(
            api_key,
            video_path=video_path,
            video_url=None,
            output_name=output_name,
            scale_height=scale_height,
        )
        mode_s = "📤 File upload (download-first)"

        await safe_edit(cb.message,
            f"✅ <b>Convert Job Submitted!</b>\\n"
            "──────────────────────\\n\\n"
            f"🆔 <code>{job_id}</code>\\n"
            f"🎬 <code>{fname[:38]}</code>\\n"
            f"📐 → <b>{res_label}</b>\\n"
            f"📦 → <code>{output_name[:40]}</code>\\n"
            f"⚙️ {mode_s}\\n{key_info}\\n\\n"
            "⏳ <i>CloudConvert is processing…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[Convert] Failed: %s", exc, exc_info=True)
        cleanup(tmp_conv)
        await safe_edit(cb.message,
            f"❌ <b>Convert failed</b>\\n\\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )'''

BUG_02B_NEW = '''\
    try:
        from services.cloudconvert_api import parse_api_keys, pick_best_key, submit_convert
        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
        else:
            key_info = "🔑 1 API key"

        await safe_edit(cb.message,
            f"☁️ <b>Submitting Convert job…</b>\\n"
            "──────────────────────\\n\\n"
            f"🎬 <code>{fname[:40]}</code>\\n"
            f"📐 → <b>{res_label}</b>\\n\\n"
            "<i>Checking API keys…</i>",
            parse_mode=enums.ParseMode.HTML)

        job_id = await submit_convert(
            api_key,
            video_path=video_path,
            video_url=None,
            output_name=output_name,
            scale_height=scale_height,
        )
        mode_s = "📤 File upload (download-first)"

        await safe_edit(cb.message,
            f"✅ <b>Convert Job Submitted!</b>\\n"
            "──────────────────────\\n\\n"
            f"🆔 <code>{job_id}</code>\\n"
            f"🎬 <code>{fname[:38]}</code>\\n"
            f"📐 → <b>{res_label}</b>\\n"
            f"📦 → <code>{output_name[:40]}</code>\\n"
            f"⚙️ {mode_s}\\n{key_info}\\n\\n"
            "⏳ <i>CloudConvert is processing…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[Convert] Failed: %s", exc, exc_info=True)
        await safe_edit(cb.message,
            f"❌ <b>Convert failed</b>\\n\\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        # FIX BUG-02b: always clean up the downloaded video (1-2 GB) whether
        # the CC job submission succeeded or failed.  Previously cleanup() was
        # only called in the except branch, leaking the full file on success.
        cleanup(tmp_conv)'''


# ─────────────────────────────────────────────────────────────
# BUG-03: plugins/stream_extractor.py — se_mag_cb action="file" broken aria2p
# ─────────────────────────────────────────────────────────────

BUG_03_OLD = '''\
    if action == "file":
        file_idx = extra
        files    = _cache.get(f"mag_files|{tok}", [])
        selected = next((f for f in files if str(f.get("index")) == str(file_idx)), None)
        fname    = selected["path"] if selected else f"file_{file_idx}"

        st  = await cb.message.edit(
            f"🧲 Downloading <code>{fname[:50]}</code>…",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = make_tmp(cfg.download_dir, uid)
        start = time.time(); last = [start]

        async def _file_prog(done: int, total: int, speed: float, eta: int) -> None:
            now = time.time()
            if now - last[0] < 3.0: return
            last[0] = now

        try:
            api = aria2p.API(aria2p.Client(
                host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
            ))
            opts = {
                "dir":           tmp,
                "seed-time":     "0",
                "select-file":   str(file_idx),
                "follow-torrent":"mem",
            }
            dl = api.add_magnet(magnet, options=opts)
            start_dl = time.time()
            while True:
                await asyncio.sleep(3)
                try:
                    dl = api.get_download(dl.gid)
                except Exception:
                    continue
                if dl.error_message:
                    raise RuntimeError(dl.error_message)
                if dl.is_complete:
                    break
                done_b  = dl.completed_length or 0
                total_b = dl.total_length     or 0
                speed   = dl.download_speed   or 0.0
                eta_val = int((total_b - done_b) / speed) if speed else 0
                await _file_prog(done_b, total_b, speed, eta_val)
                if time.time() - start_dl > 3600 * 6:
                    raise TimeoutError("Torrent timeout (6h)")

            from services.utils import largest_file
            path = largest_file(tmp)
            if not path:
                raise FileNotFoundError("No output file found")

        except Exception as exc:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ Download failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)

        await upload_file(client, st, path)
        cleanup(tmp)'''

BUG_03_NEW = '''\
    if action == "file":
        # FIX BUG-03: Replaced broken aria2p select-file + follow-torrent=mem pattern.
        # Root cause: with follow-torrent=mem, aria2c fires is_complete=True after the
        # metadata phase (~30 MB), never downloading the actual file.  Every "select file
        # from magnet" delivered a silently truncated ~30 MB blob.
        # Fix: use smart_download (aria2c subprocess) which handles metadata transparently
        # and waits for the real file to finish before returning.
        file_idx = extra
        files    = _cache.get(f"mag_files|{tok}", [])
        selected = next((f for f in files if str(f.get("index")) == str(file_idx)), None)
        fname    = selected["path"] if selected else f"file_{file_idx}"

        st = await cb.message.edit(
            f"🧲 <b>Downloading</b> <code>{fname[:50]}</code>…\\n"
            "<i>Full download via aria2c subprocess — no 30 MB limit.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = make_tmp(cfg.download_dir, uid)
        try:
            from services.downloader import smart_download as _se_dl
            _dl_path = await _se_dl(magnet, tmp, user_id=uid, label=fname, msg=st)
            if os.path.isdir(_dl_path):
                _resolved = largest_file(_dl_path)
                if not _resolved:
                    raise FileNotFoundError("No output file found in torrent download")
                _dl_path = _resolved
            elif not os.path.isfile(_dl_path):
                raise FileNotFoundError("No output file found")
        except Exception as exc:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ Download failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)

        await upload_file(client, st, _dl_path)
        cleanup(tmp)'''


# ─────────────────────────────────────────────────────────────
# BUG-06: colab_launcher.py — PATCH A regex uses ─{10} instead of ─{10,}
# ─────────────────────────────────────────────────────────────

BUG_06_OLD = (
    r'r"async def _probe_magnet_file\(.*?(?=\n(?:async def |def |class |# ─{10}))",'
)

BUG_06_NEW = (
    r'r"async def _probe_magnet_file\(.*?(?=\n(?:async def |def |class |# ─{10,}))",'
)


# ─────────────────────────────────────────────────────────────
# BUG-07: plugins/video.py — _IGNORED set missing commands
# ─────────────────────────────────────────────────────────────

BUG_07_OLD = '''\
_IGNORED = {
    "start","help","settings","info","broadcast","stats","log","restart",
    "mergedone","admin","ban_user","unban_user","banned_list","status",
    "forward","createarchive","archiveddone","bulk_url","usettings",
    "show_thumb","del_thumb","json_formatter","stream",
    "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
    "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
}'''

BUG_07_NEW = '''\
_IGNORED = {
    # Core / admin
    "start","help","settings","info","broadcast","stats","log","restart",
    "mergedone","admin","ban_user","unban_user","banned_list","status",
    "forward","createarchive","archiveddone","bulk_url","usettings",
    "show_thumb","del_thumb","json_formatter","stream",
    # Nyaa tracker
    "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
    "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
    # FIX BUG-07: added missing commands so text_reply_handler never tries to
    # parse a slash command as a trim timestamp / split chunk / etc.
    "resize","compress","hardsub","botname","ccstatus","convert",
    "captiontemplate","usage","allow","deny","allowed","cancel",
}'''


# ─────────────────────────────────────────────────────────────
# BUG-08: plugins/resize.py — _do_resize missing finally cleanup
# ─────────────────────────────────────────────────────────────

BUG_08A_OLD = '''\
    fsize = os.path.getsize(out)
    log.info("[Resize] Done: %s → %dp  (%s)", fname, height, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"📐 <b>Resize done!</b>  {height}p\\n"
        f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    await upload_file(client, st, out, user_id=uid)
    cleanup(tmp)'''

BUG_08A_NEW = '''\
    fsize = os.path.getsize(out)
    log.info("[Resize] Done: %s → %dp  (%s)", fname, height, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"📐 <b>Resize done!</b>  {height}p\\n"
        f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    # FIX BUG-08: cleanup in finally so it runs even if upload_file raises
    # (FloodWait exhausted, network drop, etc.).  Previously cleanup() was only
    # reached on the happy path, leaking multi-GB temp dirs on upload failure.
    try:
        await upload_file(client, st, out, user_id=uid)
    finally:
        cleanup(tmp)'''


BUG_08B_OLD = '''\
    fsize = os.path.getsize(out)
    log.info("[Compress] Done: %s → %.0f MB actual %s",
             fname, target_mb, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"🗜️ <b>Compress done!</b>  {human_size(fsize)}\\n"
        f"<code>{out_fname}</code>\\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    await upload_file(client, st, out, user_id=uid)
    cleanup(tmp)'''

BUG_08B_NEW = '''\
    fsize = os.path.getsize(out)
    log.info("[Compress] Done: %s → %.0f MB actual %s",
             fname, target_mb, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"🗜️ <b>Compress done!</b>  {human_size(fsize)}\\n"
        f"<code>{out_fname}</code>\\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    # FIX BUG-08: cleanup in finally (mirrors _do_resize fix above)
    try:
        await upload_file(client, st, out, user_id=uid)
    finally:
        cleanup(tmp)'''


# ─────────────────────────────────────────────────────────────
# BUG-09: plugins/start.py — 3 exclusion lists missing commands
# ─────────────────────────────────────────────────────────────

# The missing commands in all 3 collectors:  resize, compress, captiontemplate,
# usage, allow, deny, allowed

BUG_09_SUFFIX_OLD = '''\
    & ~filters.command(
        ["start","help","settings","info","status","log","restart",
         "broadcast","admin","ban_user","unban_user","banned_list",
         "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","botname","ccstatus","convert",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=8,
)
async def prefix_suffix_collector'''

BUG_09_SUFFIX_NEW = '''\
    & ~filters.command(
        ["start","help","settings","info","status","log","restart",
         "broadcast","admin","ban_user","unban_user","banned_list",
         "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","botname","ccstatus","convert",
         # FIX BUG-09: added missing commands so /resize etc. typed while
         # waiting for a prefix value are not saved as the prefix string.
         "resize","compress","captiontemplate","usage","allow","deny","allowed",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=8,
)
async def prefix_suffix_collector'''

BUG_09_CHANNEL_OLD = '''\
    & ~filters.command(
        ["start","help","settings","info","status","log","restart","broadcast",
         "admin","ban_user","unban_user","banned_list","cancel",
         "show_thumb","del_thumb","json_formatter","bulk_url","hardsub",
         "botname","ccstatus","convert",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=9,
)
async def af_channel_collector'''

BUG_09_CHANNEL_NEW = '''\
    & ~filters.command(
        ["start","help","settings","info","status","log","restart","broadcast",
         "admin","ban_user","unban_user","banned_list","cancel",
         "show_thumb","del_thumb","json_formatter","bulk_url","hardsub",
         "botname","ccstatus","convert",
         # FIX BUG-09
         "resize","compress","captiontemplate","usage","allow","deny","allowed",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=9,
)
async def af_channel_collector'''

BUG_09_BOTNAME_OLD = '''\
    & ~filters.command([
        "start", "help", "settings", "info", "status", "log", "restart",
        "broadcast", "admin", "ban_user", "unban_user", "banned_list",
        "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
        "hardsub", "stream", "forward", "createarchive", "archiveddone",
        "mergedone", "botname", "ccstatus", "convert",
        "nyaa_add", "nyaa_list", "nyaa_remove", "nyaa_check",
        "nyaa_search", "nyaa_dump", "nyaa_toggle", "nyaa_edit",
    ]),
    group=10,
)
async def botname_collector'''

BUG_09_BOTNAME_NEW = '''\
    & ~filters.command([
        "start", "help", "settings", "info", "status", "log", "restart",
        "broadcast", "admin", "ban_user", "unban_user", "banned_list",
        "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
        "hardsub", "stream", "forward", "createarchive", "archiveddone",
        "mergedone", "botname", "ccstatus", "convert",
        # FIX BUG-09
        "resize", "compress", "captiontemplate", "usage", "allow", "deny", "allowed",
        "nyaa_add", "nyaa_list", "nyaa_remove", "nyaa_check",
        "nyaa_search", "nyaa_dump", "nyaa_toggle", "nyaa_edit",
    ]),
    group=10,
)
async def botname_collector'''


# ─────────────────────────────────────────────────────────────
# BUG-10: plugins/nyaa_tracker.py — _magnet_cache unbounded growth
# ─────────────────────────────────────────────────────────────

BUG_10_OLD = '''\
_search_cache: dict[str, dict] = {}
_CACHE_TTL = 1800
_magnet_cache: dict[str, str] = {}'''

BUG_10_NEW = '''\
_search_cache: dict[str, dict] = {}
_CACHE_TTL = 1800

# FIX BUG-10: _magnet_cache now stores (timestamp, magnet) tuples with a 2-hour TTL.
# The old bare dict grew without bound — long-running bots accumulated thousands of
# entries as every Nyaa search and poller match added entries that were never removed.
_MAGNET_CACHE_TTL = 7200  # 2 hours
_magnet_cache: dict[str, tuple[float, str]] = {}


def _magnet_cache_put(key: str, magnet: str) -> None:
    """Store a magnet with the current timestamp."""
    import time as _time
    # Evict expired entries on every write (amortised O(1) cleanup)
    now = _time.time()
    dead = [k for k, (ts, _) in _magnet_cache.items() if now - ts > _MAGNET_CACHE_TTL]
    for k in dead:
        _magnet_cache.pop(k, None)
    _magnet_cache[key] = (now, magnet)


def _magnet_cache_get(key: str) -> str:
    """Return the magnet for key, or "" if expired / not found."""
    import time as _time
    entry = _magnet_cache.get(key)
    if not entry:
        return ""
    ts, magnet = entry
    if _time.time() - ts > _MAGNET_CACHE_TTL:
        _magnet_cache.pop(key, None)
        return ""
    return magnet'''


# Replace all _magnet_cache[...] = ... with _magnet_cache_put(...)
# and all _magnet_cache.get(...) with _magnet_cache_get(...)

BUG_10_SETITEM_OLD = '_magnet_cache[f"{key}_{i}"] = r.magnet'
BUG_10_SETITEM_NEW = '_magnet_cache_put(f"{key}_{i}", r.magnet)'

BUG_10_GET_OLD  = '_magnet_cache.get(f"{key}_{idx}", "")'
BUG_10_GET_NEW  = '_magnet_cache_get(f"{key}_{idx}")'

BUG_10_H12_SET_OLD = '_magnet_cache[h12] = magnet'
BUG_10_H12_GET_OLD = '_magnet_cache.get(h12, "")'
BUG_10_H12_SET_NEW = '_magnet_cache_put(h12, magnet)'
BUG_10_H12_GET_NEW = '_magnet_cache_get(h12)'


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Apply Zilong audit v2 patches")
    parser.add_argument("--repo", required=True, help="Path to Zilong_multiusage checkout")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    if not repo.is_dir():
        print(f"❌ Repo directory not found: {repo}")
        sys.exit(1)

    print(f"🔧 Applying patches to: {repo}\n")
    errors = 0

    # ── BUG-02b ──────────────────────────────────────────────
    p = repo / "plugins" / "url_handler.py"
    print(f"[BUG-02b] {p.relative_to(repo)}")
    if not patch(p, BUG_02B_OLD, BUG_02B_NEW, "ccv_resolution_cb finally cleanup"):
        errors += 1
    print()

    # ── BUG-03 ───────────────────────────────────────────────
    p = repo / "plugins" / "stream_extractor.py"
    print(f"[BUG-03] {p.relative_to(repo)}")
    if not patch(p, BUG_03_OLD, BUG_03_NEW, "se_mag_cb action=file → smart_download"):
        errors += 1
    print()

    # ── BUG-06 ───────────────────────────────────────────────
    p = repo / "colab_launcher.py"
    print(f"[BUG-06] {p.relative_to(repo)}")
    if not patch(p, BUG_06_OLD, BUG_06_NEW, "PATCH A regex ─{10} → ─{10,}"):
        errors += 1
    print()

    # ── BUG-07 ───────────────────────────────────────────────
    p = repo / "plugins" / "video.py"
    print(f"[BUG-07] {p.relative_to(repo)}")
    if not patch(p, BUG_07_OLD, BUG_07_NEW, "_IGNORED set missing commands"):
        errors += 1
    print()

    # ── BUG-08a (_do_resize) ─────────────────────────────────
    p = repo / "plugins" / "resize.py"
    print(f"[BUG-08] {p.relative_to(repo)}")
    ok_a = patch(p, BUG_08A_OLD, BUG_08A_NEW, "_do_resize finally cleanup")
    ok_b = patch(p, BUG_08B_OLD, BUG_08B_NEW, "_do_compress finally cleanup")
    if not (ok_a and ok_b):
        errors += 1
    print()

    # ── BUG-09 (3 collectors) ────────────────────────────────
    p = repo / "plugins" / "start.py"
    print(f"[BUG-09] {p.relative_to(repo)}")
    ok_a = patch(p, BUG_09_SUFFIX_OLD,  BUG_09_SUFFIX_NEW,  "prefix_suffix_collector exclusions")
    ok_b = patch(p, BUG_09_CHANNEL_OLD, BUG_09_CHANNEL_NEW, "af_channel_collector exclusions")
    ok_c = patch(p, BUG_09_BOTNAME_OLD, BUG_09_BOTNAME_NEW, "botname_collector exclusions")
    if not (ok_a and ok_b and ok_c):
        errors += 1
    print()

    # ── BUG-10 (_magnet_cache TTL) ───────────────────────────
    p = repo / "plugins" / "nyaa_tracker.py"
    print(f"[BUG-10] {p.relative_to(repo)}")
    ok_a = patch(p, BUG_10_OLD,         BUG_10_NEW,         "_magnet_cache → TTL dict + helpers")
    # Replace all usages of the old dict API with the new helper functions
    ok_b = True
    src = _read(p)
    # Replace set operations
    new_src = src
    for old_s, new_s in [
        (BUG_10_SETITEM_OLD, BUG_10_SETITEM_NEW),
        (BUG_10_GET_OLD,     BUG_10_GET_NEW),
    ]:
        if old_s in new_src:
            new_src = new_src.replace(old_s, new_s)
            print(f"  ✅ Usage patch: {old_s[:60]!r} → ok")
        else:
            if new_s in new_src:
                print(f"  ✅ Usage patch already applied: {old_s[:60]!r}")
            else:
                print(f"  ⚠️  Usage patch not found: {old_s[:60]!r}")

    # h12 set/get — there are multiple occurrences; replace all
    count_h12_set = new_src.count(BUG_10_H12_SET_OLD)
    count_h12_get = new_src.count(BUG_10_H12_GET_OLD)
    if count_h12_set:
        new_src = new_src.replace(BUG_10_H12_SET_OLD, BUG_10_H12_SET_NEW)
        print(f"  ✅ h12 set patch: replaced {count_h12_set} occurrence(s)")
    if count_h12_get:
        new_src = new_src.replace(BUG_10_H12_GET_OLD, BUG_10_H12_GET_NEW)
        print(f"  ✅ h12 get patch: replaced {count_h12_get} occurrence(s)")

    if new_src != src:
        if _verify_syntax(new_src, "BUG-10 usage patches"):
            _write(p, new_src)
        else:
            ok_b = False

    if not (ok_a and ok_b):
        errors += 1
    print()

    # ── Summary ──────────────────────────────────────────────
    print("═" * 50)
    if errors == 0:
        print("✅ All patches applied successfully!")
    else:
        print(f"⚠️  {errors} patch group(s) had issues — review output above.")
    print()
    print("REMINDER: copy complete replacement files from zilong_audit_v2/:")
    print("  services/cloudconvert_hook.py  (BUG-01 + BUG-12)")
    print("  core/session.py                (BUG-11)")
    print("  plugins/hardsub.py             (BUG-04)")
    print("  plugins/uploader.py            (BUG-05 — stub)")


if __name__ == "__main__":
    main()
