"""
plugins/hardsub.py  —  PATCHED v2 (preset selector added)

WHAT CHANGED vs previous version
────────────────────────────────
FIX HS-PRESET: user can now pick FFmpeg preset (fast | medium | slow)
  at the same "waiting_subtitle" step as CRF.

  Rationale:
    • 'fast'    — ~2× speed, slightly larger file, slightly lower quality
    • 'medium'  — default, balanced
    • 'slow'    — ~2× slower, ~15-20% smaller file, better quality
    • 'veryslow'— for perfectionists, very slow

  State now carries both `crf` (18/20/23/26) and `preset`.
  Default: preset=medium, crf=20.

Additional changes:
  • _subtitle_kb rebuilt to add preset row
  • Callback `hs_pre|<uid>|<preset>` added
  • _submit_one_job / _submit_one_fc now pass preset through
  • CC API (cloudconvert_api.submit_hardsub) already accepts preset;
    this commit passes it through.
"""
from __future__ import annotations

import logging
import os
import re
import urllib.parse as _urlparse

import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.cc_sanitize import build_cc_output_name
from services.utils import human_size, make_tmp, cleanup, safe_edit

# Local FFmpeg availability — checked once at import
import shutil as _shutil
_FFMPEG_OK = bool(_shutil.which("ffmpeg"))

log = logging.getLogger(__name__)

_SUB_EXTS = {".ass", ".srt", ".vtt", ".ssa", ".sub", ".txt"}

# ── Per-user state ────────────────────────────────────────────
_STATE: dict[int, dict] = {}


def _user_state(uid: int) -> dict | None:
    return _STATE.get(uid)


def _clear(uid: int) -> None:
    s = _STATE.pop(uid, None)
    if s and s.get("tmp"):
        cleanup(s["tmp"])


# ── Public entry-point used by url_handler.py ─────────────────

async def start_hardsub_for_url(
    client: "Client",
    st,
    uid: int,
    url: str,
    fname: str,
) -> None:
    _clear(uid)
    tmp = make_tmp(cfg.download_dir, uid)

    await safe_edit(
        st,
        f"⬇️ <b>Downloading video for Hardsub…</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:45]}</code>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        from services.downloader import smart_download
        path = await smart_download(url, tmp, user_id=uid, label=fname, msg=st)
        if os.path.isdir(path):
            from services.utils import largest_file
            resolved = largest_file(path)
            if resolved:
                path = resolved
        if not os.path.isfile(path):
            raise FileNotFoundError("No output file found after download")
        fname = os.path.basename(path)
        fsize = os.path.getsize(path)
    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(
            st, f"❌ <b>Download failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    _STATE[uid] = {
        "step":      "waiting_subtitle",
        "tmp":       tmp,
        "videos":    [{"path": path, "url": None, "fname": fname}],
        "sub_path":  None,
        "sub_fname": None,
        "crf":       23,
        "preset":    "medium",
        "platform":  _default_platform(),
    }

    await _show_subtitle_prompt(st, uid, fname, fsize)


# ── Labels ────────────────────────────────────────────────────

def _crf_label(crf: int) -> str:
    labels = {18: "CRF 18 🔵 HQ", 20: "CRF 20 🟢 HQ",
              23: "CRF 23 🟢 Default", 26: "CRF 26 🟠 Low"}
    return labels.get(crf, f"CRF {crf}")


def _preset_label(p: str) -> str:
    return {"fast": "⚡ Fast", "medium": "🟢 Medium",
            "slow": "🐢 Slow", "veryslow": "🦥 VerySlow"}.get(p, p)


def _platform_label(platform: str) -> str:
    return {
        "cc":    "☁️ CloudConvert",
        "fc":    "🆓 FreeConvert",
        "local": "🖥 Local FFmpeg",
    }.get(platform, platform)


def _has_cc()    -> bool: return bool(os.environ.get("CC_API_KEY", "").strip())
def _has_fc()    -> bool: return bool(os.environ.get("FC_API_KEY", "").strip())
def _has_local() -> bool: return _FFMPEG_OK


def _default_platform() -> str:
    if _has_cc():    return "cc"
    if _has_fc():    return "fc"
    if _has_local(): return "local"
    return "cc"


async def _show_subtitle_prompt(st, uid: int, fname: str, fsize: int = 0) -> None:
    state        = _STATE.get(uid, {})
    cur_crf      = state.get("crf", 23)
    cur_preset   = state.get("preset", "medium")
    cur_platform = state.get("platform", _default_platform())
    size_s = f"  <code>{human_size(fsize)}</code>" if fsize else ""

    await safe_edit(
        st,
        f"🔥 <b>Hardsub — Ready</b>\n"
        "──────────────────────\n\n"
        f"✅ <code>{fname[:45]}</code>{size_s}\n\n"
        f"⚙️ <b>Quality:</b> {_crf_label(cur_crf)}\n"
        f"🎛 <b>Preset:</b>  {_preset_label(cur_preset)}\n"
        f"🌐 <b>Platform:</b> {_platform_label(cur_platform)}\n\n"
        "Now send the <b>subtitle</b>:\n"
        "• A <b>file</b> (.ass / .srt / .vtt)\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_subtitle_kb(uid),
    )


def _subtitle_kb(uid: int) -> InlineKeyboardMarkup:
    state        = _STATE.get(uid, {})
    cur_crf      = state.get("crf", 23)
    cur_preset   = state.get("preset", "medium")
    cur_platform = state.get("platform", _default_platform())

    def _crf_btn(lbl, crf):
        tick = " ✓" if crf == cur_crf else ""
        return InlineKeyboardButton(f"{lbl}{tick}", callback_data=f"hs_crf|{uid}|{crf}")

    def _pre_btn(p):
        tick = " ✓" if p == cur_preset else ""
        return InlineKeyboardButton(
            f"{_preset_label(p)}{tick}", callback_data=f"hs_pre|{uid}|{p}",
        )

    def _plat_btn(lbl, plat):
        tick = " ✓" if plat == cur_platform else ""
        return InlineKeyboardButton(f"{lbl}{tick}", callback_data=f"hs_plat|{uid}|{plat}")

    rows = [
        [_crf_btn("🔵 18", 18), _crf_btn("🟢 20", 20),
         _crf_btn("🟡 23", 23), _crf_btn("🟠 26", 26)],
        [_pre_btn("fast"), _pre_btn("medium"),
         _pre_btn("slow"), _pre_btn("veryslow")],
    ]

    # Platform row — show if 2+ backends available
    plat_row = []
    if _has_cc():    plat_row.append(_plat_btn("☁️ CC",    "cc"))
    if _has_fc():    plat_row.append(_plat_btn("🆓 FC",    "fc"))
    if _has_local(): plat_row.append(_plat_btn("🖥 Local", "local"))
    if len(plat_row) >= 2:
        rows.append(plat_row)

    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"hs_cancel|{uid}")])
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# Callbacks
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^hs_crf\|"))
async def hardsub_crf_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3: return await cb.answer("Invalid.", show_alert=True)
    _, uid_s, crf_s = parts[:3]
    uid = int(uid_s) if uid_s.isdigit() else cb.from_user.id
    state = _user_state(uid)
    if not state: return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    try: state["crf"] = int(crf_s)
    except ValueError: return
    videos = state.get("videos", [])
    fname  = videos[-1]["fname"] if videos else "video"
    path   = videos[-1].get("path") if videos else None
    fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
    await _show_subtitle_prompt(cb.message, uid, fname, fsize)


@Client.on_callback_query(filters.regex(r"^hs_pre\|"))
async def hardsub_preset_cb(client: Client, cb: CallbackQuery):
    """NEW — preset picker callback."""
    parts = cb.data.split("|")
    if len(parts) < 3: return await cb.answer("Invalid.", show_alert=True)
    _, uid_s, preset = parts[:3]
    uid = int(uid_s) if uid_s.isdigit() else cb.from_user.id
    state = _user_state(uid)
    if not state: return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    if preset not in ("fast", "medium", "slow", "veryslow"):
        return
    state["preset"] = preset
    videos = state.get("videos", [])
    fname  = videos[-1]["fname"] if videos else "video"
    path   = videos[-1].get("path") if videos else None
    fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
    await _show_subtitle_prompt(cb.message, uid, fname, fsize)


@Client.on_callback_query(filters.regex(r"^hs_plat\|"))
async def hardsub_platform_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3: return await cb.answer("Invalid.", show_alert=True)
    _, uid_s, plat = parts[:3]
    uid = int(uid_s) if uid_s.isdigit() else cb.from_user.id
    state = _user_state(uid)
    if not state: return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    if plat not in ("cc", "fc", "local"): return
    state["platform"] = plat
    videos = state.get("videos", [])
    fname  = videos[-1]["fname"] if videos else "video"
    path   = videos[-1].get("path") if videos else None
    fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
    await _show_subtitle_prompt(cb.message, uid, fname, fsize)


# ─────────────────────────────────────────────────────────────
# Submission
# ─────────────────────────────────────────────────────────────

async def _submit_one_job(
    api_key: str,
    video:   dict,
    sub_path:  str,
    sub_fname: str,
    uid:       int,
    crf:       int  = 23,
    preset:    str  = "medium",
    platform:  str  = "cc",
) -> tuple[str, str, bool]:
    video_fname = video.get("fname", "video.mkv")
    output_name = build_cc_output_name(video_fname, suffix="VOSTFR")

    log.info("[Hardsub] output=%s  platform=%s  CRF=%d  preset=%s",
             output_name, platform, crf, preset)

    if platform == "fc":
        return await _submit_one_fc(api_key, video, sub_path, sub_fname,
                                    uid, crf, preset, output_name, video_fname)

    # CloudConvert
    from services.cloudconvert_api import submit_hardsub
    from services.cc_job_store import cc_job_store, CCJob
    from services.task_runner import tracker, TaskRecord
    from services.utils import human_size
    import time as _hs_time

    # Register an upload task so /status shows CC upload progress
    ul_tid = tracker.new_tid()
    _vid_size = os.path.getsize(video.get("path", "")) if video.get("path") else 0
    _sub_size = os.path.getsize(sub_path) if os.path.isfile(sub_path) else 0
    ul_rec = TaskRecord(
        tid=ul_tid, user_id=uid,
        label=f"CC↑ {video_fname}",
        fname=video_fname,
        mode="ul", engine="http",
        state="☁️ Uploading to CC",
        total=_vid_size + _sub_size,
    )
    await tracker.register(ul_rec)
    _hs_ul_start = _hs_time.time()

    async def _hs_upload_progress(phase: str, done: int, total: int) -> None:
        phase_label = "📄 Sub" if phase == "sub" else "🎬 Video"
        ul_done  = (done if phase == "sub" else _sub_size + done)
        ul_total = _sub_size + _vid_size
        elapsed  = _hs_time.time() - _hs_ul_start
        speed    = ul_done / elapsed if elapsed else 0.0
        eta      = int((ul_total - ul_done) / speed) if (speed and ul_total > ul_done) else 0
        await tracker.update(
            ul_tid,
            state=f"☁️ {phase_label} {human_size(done)}/{human_size(total)}",
            done=ul_done, total=ul_total,
            speed=speed, eta=eta, elapsed=elapsed,
        )

    try:
        job_id = await submit_hardsub(
            api_key,
            video_path=video.get("path"),
            video_url=video.get("url"),
            subtitle_path=sub_path,
            output_name=output_name,
            scale_height=0,
            crf=crf,
            preset=preset,   # NEW — passed through
            upload_progress_cb=_hs_upload_progress,
        )
        await tracker.finish(ul_tid, success=True)
        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid, fname=video_fname,
            sub_fname=sub_fname, output_name=output_name,
            status="processing",
        ))
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception: pass
        return video_fname, job_id, True
    except Exception as exc:
        await tracker.finish(ul_tid, success=False, msg=str(exc)[:60])
        log.error("[Hardsub-CC] %s failed: %s", video_fname, exc)
        return video_fname, str(exc)[:80], False


async def _submit_one_fc(
    api_key:     str,
    video:       dict,
    sub_path:    str,
    sub_fname:   str,
    uid:         int,
    crf:         int,
    preset:      str,
    output_name: str,
    video_fname: str,
) -> tuple[str, str, bool]:
    from services.freeconvert_api import submit_hardsub as fc_submit
    from services.fc_job_store import fc_job_store, FCJob
    try:
        # webhook_url auto-derived from tunnel inside fc_submit now
        job_id = await fc_submit(
            api_key,
            video_path=video.get("path"),
            video_url=video.get("url"),
            subtitle_path=sub_path,
            output_name=output_name,
            crf=crf,
            preset=preset,   # NEW
        )
        await fc_job_store.add(FCJob(
            job_id=job_id, uid=uid, fname=video_fname,
            sub_fname=sub_fname, output_name=output_name,
            status="processing", job_type="hardsub",
            api_key=api_key,
        ))
        return video_fname, job_id, True
    except Exception as exc:
        log.error("[Hardsub-FC] %s failed: %s", video_fname, exc)
        return video_fname, str(exc)[:80], False


async def _submit_batch_local(
    st, state: dict, uid: int,
    videos: list, sub_path: str, sub_fname: str,
    crf: int, preset: str,
) -> None:
    """Inline local-FFmpeg hardsub. Re-encodes audio to AAC 128k (matches CC preset)."""
    import time as _time
    from services.ffmpeg import hardsub_video
    from services.uploader import upload_file
    from core.session import get_client

    client = get_client()

    count = len(videos)
    out_files: list[str] = []
    errs: list[str] = []

    await safe_edit(
        st,
        f"🖥 <b>Local hardsub — {count} video{'s' if count > 1 else ''}</b>\n"
        "──────────────────────\n\n"
        f"💬 <code>{sub_fname[:42]}</code>\n"
        f"⚙️ {_crf_label(crf)}  ·  🎛 {_preset_label(preset)}\n"
        f"🌐 {_platform_label('local')}\n\n"
        "⏳ Processing…",
        parse_mode=enums.ParseMode.HTML,
    )

    for i, video in enumerate(videos):
        in_path = video.get("path")
        if not in_path or not os.path.isfile(in_path):
            errs.append(f"{i+1}. missing input")
            continue

        fname = video.get("fname", os.path.basename(in_path))
        stem, _ = os.path.splitext(fname)
        out_name = build_cc_output_name(fname, suffix="VOSTFR")
        out_path = os.path.join(state["tmp"], out_name)

        t0 = _time.monotonic()
        await safe_edit(
            st,
            f"🖥 <b>Encoding {i+1}/{count}</b>\n"
            "──────────────────────\n\n"
            f"📁 <code>{fname[:45]}</code>\n"
            f"⚙️ {_crf_label(crf)}  ·  🎛 {_preset_label(preset)}\n"
            "⏳ This can take a while for long videos…",
            parse_mode=enums.ParseMode.HTML,
        )

        try:
            await hardsub_video(in_path, sub_path, out_path, crf=crf, preset=preset)
        except Exception as exc:
            log.error("[Hardsub-Local] %s failed: %s", fname, exc)
            errs.append(f"{i+1}. <code>{str(exc)[:80]}</code>")
            continue

        if not os.path.isfile(out_path) or os.path.getsize(out_path) == 0:
            errs.append(f"{i+1}. empty output")
            continue

        dt = _time.monotonic() - t0
        log.info("[Hardsub-Local] %s done in %.1fs", fname, dt)

        # Upload (upload_file handles PanelUpdater + FloodWait internally)
        try:
            await upload_file(client, st, out_path, user_id=uid, is_last=(i == count - 1))
            out_files.append(out_path)
        except Exception as exc:
            log.error("[Hardsub-Local] upload %s failed: %s", out_name, exc)
            errs.append(f"{i+1}. upload: <code>{str(exc)[:60]}</code>")

    ok_count = len(out_files)
    ok_line = f"✅ {ok_count}/{count} uploaded"
    err_block = ("\n\n❌ Errors:\n" + "\n".join(f"  {e}" for e in errs)) if errs else ""

    await safe_edit(
        st,
        f"{'✅' if ok_count == count else '⚠️'} <b>Local hardsub complete</b>\n"
        "──────────────────────\n\n"
        f"{ok_line}{err_block}",
        parse_mode=enums.ParseMode.HTML,
    )

    # Cleanup outputs after upload
    for p in out_files:
        try: os.remove(p)
        except Exception: pass

    _clear(uid)


async def _submit_batch(st, state: dict, uid: int) -> None:
    videos    = state.get("videos", [])
    sub_path  = state["sub_path"]
    sub_fname = state.get("sub_fname", "subtitle.ass")
    crf       = state.get("crf", 23)
    preset    = state.get("preset", "medium")
    platform  = state.get("platform", _default_platform())
    count     = len(videos)

    # Local FFmpeg branches off — different flow (inline processing, not submit-and-wait)
    if platform == "local":
        return await _submit_batch_local(st, state, uid, videos, sub_path, sub_fname, crf, preset)

    vid_list = "\n".join(
        f"  {i+1}. <code>{v['fname'][:40]}</code>" for i, v in enumerate(videos)
    )

    await safe_edit(
        st,
        f"☁️ <b>Submitting {count} hardsub job{'s' if count > 1 else ''}…</b>\n"
        "──────────────────────\n\n"
        f"{vid_list}\n\n"
        f"💬 <code>{sub_fname[:42]}</code>\n"
        f"⚙️ {_crf_label(crf)}  ·  🎛 {_preset_label(preset)}\n"
        f"🌐 {_platform_label(platform)}\n",
        parse_mode=enums.ParseMode.HTML,
    )

    if platform == "fc":
        api_key = os.environ.get("FC_API_KEY", "").strip()
        key_src = "FC_API_KEY"
    else:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        key_src = "CC_API_KEY"

    if not api_key:
        await safe_edit(st, f"❌ {key_src} not set. Add it to .env.",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)
        return

    key_info = ""
    if platform == "cc":
        try:
            from services.cloudconvert_api import parse_api_keys, pick_best_key
            keys = parse_api_keys(api_key)
            if len(keys) > 1:
                selected, credits = await pick_best_key(keys)
                key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits)"
            else:
                key_info = "🔑 1 CC API key"
        except Exception as exc:
            await safe_edit(st, f"❌ <b>CC keys exhausted</b>\n\n<code>{str(exc)[:200]}</code>",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)
            return
    else:
        try:
            from services.freeconvert_api import parse_fc_keys, pick_best_fc_key
            fc_keys = parse_fc_keys(api_key)
            if len(fc_keys) > 1:
                best_key, mins = await pick_best_fc_key(fc_keys)
                key_info = f"🔑 Key {fc_keys.index(best_key)+1}/{len(fc_keys)}"
                api_key = best_key
            else:
                key_info = "🔑 1 FC API key"
        except Exception as exc:
            await safe_edit(st, f"❌ <b>FC keys exhausted</b>\n\n<code>{str(exc)[:200]}</code>",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)
            return

    results: list[str] = []
    ok_count = 0
    for i, video in enumerate(videos):
        vname, result, success = await _submit_one_job(
            api_key, video, sub_path, sub_fname, uid,
            crf=crf, preset=preset, platform=platform,
        )
        if success:
            results.append(f"✅ {i+1}. <code>{vname[:35]}</code> → <code>{result}</code>")
            ok_count += 1
        else:
            results.append(f"❌ {i+1}. <code>{vname[:35]}</code> — {result}")

    wh_note = (
        "⬆️ <i>Webhook active — result uploads automatically.</i>"
        if platform == "fc"
        else "⏳ <i>Auto-uploads via webhook / poller when ready.</i>"
    )
    await safe_edit(
        st,
        f"{'✅' if ok_count == count else '⚠️'} <b>Hardsub — {ok_count}/{count} submitted</b>\n"
        "──────────────────────\n\n"
        f"{chr(10).join(results)}\n\n"
        f"💬 <code>{sub_fname[:38]}</code>\n"
        f"⚙️ {_crf_label(crf)}  ·  🎛 {_preset_label(preset)}  ·  🌐 {_platform_label(platform)}\n"
        f"{key_info}\n\n"
        f"{wh_note}",
        parse_mode=enums.ParseMode.HTML,
    )
    _clear(uid)


# ─────────────────────────────────────────────────────────────
# Video / flow callbacks (unchanged logic, kept for completeness)
# ─────────────────────────────────────────────────────────────

async def _video_added(msg_or_st, state: dict, uid: int, fname: str) -> None:
    videos = state.get("videos", [])
    count  = len(videos)
    vid_list = "\n".join(
        f"  {i+1}. <code>{v['fname'][:40]}</code>" for i, v in enumerate(videos)
    )
    await safe_edit(
        msg_or_st,
        f"✅ <b>Video {count} added!</b>\n"
        "──────────────────────\n\n"
        f"{vid_list}\n\n"
        "Send another video or tap <b>Done</b>.",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add another",      callback_data=f"hs_more|{uid}"),
             InlineKeyboardButton(f"🟢 Done ({count})",  callback_data=f"hs_done|{uid}")],
            [InlineKeyboardButton("❌ Cancel",            callback_data=f"hs_cancel|{uid}")],
        ]),
    )


@Client.on_message(filters.private & filters.command("hardsub"))
async def cmd_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")
    api_key = os.environ.get("CC_API_KEY", "").strip() or os.environ.get("FC_API_KEY", "").strip()
    if not api_key and not _has_local():
        return await msg.reply(
            "❌ <b>No hardsub backend available</b>\n\n"
            "Add <code>CC_API_KEY</code> or <code>FC_API_KEY</code> to .env, "
            "or install <code>ffmpeg</code> for local hardsub.",
            parse_mode=enums.ParseMode.HTML,
        )
    _clear(uid)
    tmp = make_tmp(cfg.download_dir, uid)
    _STATE[uid] = {
        "step": "waiting_video", "tmp": tmp, "videos": [],
        "sub_path": None, "sub_fname": None,
        "crf": 23, "preset": "medium", "platform": _default_platform(),
    }
    await msg.reply(
        "🔥 <b>Hardsub</b>\n"
        "──────────────────────\n\n"
        "Send the <b>video</b> (file / URL / magnet).\n"
        "You can send multiple — all will get the same subtitle.\n\n"
        "⚙️ Quality + preset selectable before processing.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.command("cancel"), group=4)
async def cmd_cancel_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid in _STATE:
        _clear(uid)
        await msg.reply("❌ Hardsub cancelled.")
        msg.stop_propagation()


@Client.on_callback_query(filters.regex(r"^hs_(more|done|cancel)\|"))
async def hardsub_flow_cb(client: Client, cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[0].split("_")[1]
    uid    = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else cb.from_user.id
    state  = _user_state(uid)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    if action == "cancel":
        _clear(uid)
        await cb.message.delete()
        return
    if action == "more":
        state["step"] = "waiting_video"
        await cb.message.edit(
            f"📦 <b>{len(state['videos'])} video(s) queued</b>\n\nSend the next video:",
            parse_mode=enums.ParseMode.HTML,
        )
        return
    if action == "done":
        if not state["videos"]:
            return await cb.answer("No videos added yet!", show_alert=True)
        state["step"] = "waiting_subtitle"
        fname  = state["videos"][0]["fname"]
        path   = state["videos"][0].get("path")
        fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
        await _show_subtitle_prompt(cb.message, uid, fname, fsize)


@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=1,
)
async def hardsub_video_file(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_video":
        return
    media = msg.video or msg.document
    if not media: return
    fname = getattr(media, "file_name", None) or "video.mkv"
    ext   = os.path.splitext(fname)[1].lower()
    _VIDEO_EXTS = {".mp4",".mkv",".avi",".mov",".webm",".flv",".ts",".m2ts",".wmv",".m4v"}
    if ext not in _VIDEO_EXTS and not msg.video:
        return
    fsize = getattr(media, "file_size", 0) or 0
    st    = await msg.reply(f"⬇️ Downloading <code>{fname[:40]}</code>…",
                            parse_mode=enums.ParseMode.HTML)
    try:
        from services.tg_download import tg_download
        path = await tg_download(
            client, media.file_id,
            os.path.join(state["tmp"], fname), st,
            fname=fname, fsize=fsize, user_id=uid,
        )
        state["videos"].append({"path": path, "url": None, "fname": os.path.basename(path)})
        await _video_added(st, state, uid, fname)
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
    msg.stop_propagation()


@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start","help","settings","info","status","log","restart",
         "broadcast","admin","ban_user","unban_user","banned_list",
         "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","stream","forward","createarchive","archiveddone","mergedone",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
         "resize","compress","usage","allow","deny","allowed",
         "botname","ccstatus","convert","captiontemplate"]
    ),
    group=1,
)
async def hardsub_url_handler(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _user_state(uid)
    if not state: return
    if state["step"] not in ("waiting_video", "waiting_subtitle"): return
    text   = msg.text.strip()
    if not re.match(r"^(https?://\S+|magnet:\?\S+)$", text, re.I):
        return

    if state["step"] == "waiting_subtitle":
        await _handle_subtitle_url(msg, state, text, uid)
        msg.stop_propagation()
        return

    from services.downloader import classify, smart_download
    from services.utils import largest_file
    kind = classify(text)
    st = await msg.reply(f"⬇️ Downloading via {kind}…", parse_mode=enums.ParseMode.HTML)
    tmp = state["tmp"]
    try:
        path = await smart_download(text, tmp, user_id=uid, label="Hardsub DL")
        if os.path.isdir(path):
            resolved = largest_file(path)
            if resolved: path = resolved
        if not os.path.isfile(path):
            raise FileNotFoundError("No output file found")
        fname = os.path.basename(path)
        state["videos"].append({"path": path, "url": None, "fname": fname})
        await _video_added(st, state, uid, fname)
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
    msg.stop_propagation()


@Client.on_message(filters.private & filters.document, group=0)
async def hardsub_subtitle_file(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_subtitle": return
    media = msg.document
    if not media: return
    fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext = os.path.splitext(fname)[1].lower()
    if ext not in _SUB_EXTS:
        await msg.reply(f"❌ Unsupported: {ext}\nUse .ass/.srt/.vtt/.ssa/.sub/.txt")
        msg.stop_propagation()
        return
    tmp = state["tmp"]
    st  = await msg.reply("⬇️ Downloading subtitle…")
    try:
        sub_path = await client.download_media(media, file_name=os.path.join(tmp, fname))
        state["sub_path"]  = sub_path
        state["sub_fname"] = os.path.basename(sub_path)
    except Exception as exc:
        await safe_edit(st, f"❌ Subtitle download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid); msg.stop_propagation(); return
    await _submit_batch(st, state, uid)
    msg.stop_propagation()


async def _handle_subtitle_url(msg: Message, state: dict, url: str, uid: int) -> None:
    tmp = state["tmp"]
    parsed_path = _urlparse.urlparse(url).path
    raw_fname   = os.path.basename(parsed_path)
    fname       = _urlparse.unquote_plus(raw_fname) if raw_fname else "subtitle.ass"
    ext         = os.path.splitext(fname)[1].lower()
    if ext not in _SUB_EXTS:
        fname = (fname + ".ass") if fname else "subtitle.ass"
    fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

    st = await msg.reply(f"⬇️ Downloading subtitle from URL…", parse_mode=enums.ParseMode.HTML)
    try:
        sub_path = os.path.join(tmp, fname)
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers={"User-Agent": "Mozilla/5.0"},
                                 allow_redirects=True) as resp:
                resp.raise_for_status()
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    cd_fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
                    if cd_fname:
                        cd_fname = _urlparse.unquote_plus(cd_fname)
                        cd_ext   = os.path.splitext(cd_fname)[1].lower()
                        if cd_ext in _SUB_EXTS:
                            fname    = re.sub(r'[\\/:*?"<>|]', "_", cd_fname)
                            sub_path = os.path.join(tmp, fname)
                content = await resp.read()
        if len(content) > 10_000_000:
            await safe_edit(st, "❌ File too large — not a subtitle.")
            _clear(uid); return
        with open(sub_path, "wb") as f: f.write(content)
        state["sub_path"]  = sub_path
        state["sub_fname"] = fname
    except Exception as exc:
        await safe_edit(st, f"❌ Subtitle URL failed: <code>{str(exc)[:200]}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid); return
    await _submit_batch(st, state, uid)
