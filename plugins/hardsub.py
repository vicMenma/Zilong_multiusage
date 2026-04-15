"""
plugins/hardsub.py
CloudConvert / FreeConvert hardsubbing — batch multi-video support.

CHANGES:
  • CRF quality picker (CRF 18/20/23/26) at "waiting_subtitle" step.
  • Platform picker (☁️ CloudConvert  /  🆓 FreeConvert) at same step.
    - CloudConvert: CC_API_KEY env var; webhooks via Cloudflare tunnel.
    - FreeConvert:  FC_API_KEY env var; webhooks via /fc-webhook route.
    - If only one key is configured, platform is chosen automatically.
    - If both are set, user sees the platform buttons.
  • Default: CloudConvert / CRF 20 / preset "medium".
  • Preset changed from "ultrafast" → "medium" for better quality.

FIX BUG-04: hardsub_url_handler now handles kind == "scrape".
PATCH: _submit_one_job() uses build_cc_output_name() from cc_sanitize.
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
    """DOWNLOAD-FIRST: Download the video locally before asking for subtitle."""
    _clear(uid)
    tmp = make_tmp(cfg.download_dir, uid)

    await safe_edit(
        st,
        f"⬇️ <b>Downloading video for Hardsub…</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:45]}</code>\n\n"
        "<i>Download-first → CC file upload is reliable for any URL type.</i>",
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
        log.info("[Hardsub] Downloaded %s (%s) for hardsub", fname, human_size(fsize))

    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(
            st,
            f"❌ <b>Download failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    _STATE[uid] = {
        "step":      "waiting_subtitle",
        "tmp":       tmp,
        "videos":    [{"path": path, "url": None, "fname": fname}],
        "sub_path":  None,
        "sub_fname": None,
        "crf":       20,
        "platform":  _default_platform(),   # cc or fc
    }

    await _show_subtitle_prompt(st, uid, fname, fsize)


async def _show_subtitle_prompt(st, uid: int, fname: str, fsize: int = 0) -> None:
    """Show the subtitle request with CRF quality picker."""
    state    = _STATE.get(uid, {})
    cur_crf  = state.get("crf", 20)
    crf_lbl  = _crf_label(cur_crf)

    size_s = f"  <code>{human_size(fsize)}</code>" if fsize else ""

    await safe_edit(
        st,
        f"🔥 <b>Hardsub — Ready</b>\n"
        "──────────────────────\n\n"
        f"✅ <code>{fname[:45]}</code>{size_s}\n\n"
        f"⚙️ <b>Quality:</b> {crf_lbl}  "
        "<i>(tap to change)</i>\n\n"
        "Now send the <b>subtitle</b>:\n"
        "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_crf_kb(uid),
    )


def _crf_label(crf: int) -> str:
    labels = {18: "CRF 18 🔵 (HQ)", 20: "CRF 20 🟢 (Default)",
              23: "CRF 23 🟡 (Medium)", 26: "CRF 26 🟠 (Low)"}
    return labels.get(crf, f"CRF {crf}")


def _platform_label(platform: str) -> str:
    return "☁️ CloudConvert" if platform == "cc" else "🆓 FreeConvert"


def _has_cc() -> bool:
    return bool(os.environ.get("CC_API_KEY", "").strip())


def _has_fc() -> bool:
    return bool(os.environ.get("FC_API_KEY", "").strip())


def _default_platform() -> str:
    """Auto-select platform when only one key is configured."""
    if _has_cc():
        return "cc"
    if _has_fc():
        return "fc"
    return "cc"   # will fail at submission; error surfaced there


async def _show_subtitle_prompt(st, uid: int, fname: str, fsize: int = 0) -> None:
    """Show the subtitle request with CRF quality picker + platform picker."""
    state        = _STATE.get(uid, {})
    cur_crf      = state.get("crf", 20)
    cur_platform = state.get("platform", _default_platform())
    crf_lbl      = _crf_label(cur_crf)
    plat_lbl     = _platform_label(cur_platform)

    size_s = f"  <code>{human_size(fsize)}</code>" if fsize else ""

    await safe_edit(
        st,
        f"🔥 <b>Hardsub — Ready</b>\n"
        "──────────────────────\n\n"
        f"✅ <code>{fname[:45]}</code>{size_s}\n\n"
        f"⚙️ <b>Quality:</b> {crf_lbl}  <i>(tap to change)</i>\n"
        f"🌐 <b>Platform:</b> {plat_lbl}  <i>(tap to change)</i>\n\n"
        "Now send the <b>subtitle</b>:\n"
        "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_subtitle_kb(uid),
    )


def _subtitle_kb(uid: int) -> InlineKeyboardMarkup:
    """
    Combined keyboard shown at the subtitle prompt:
    top row = CRF picker, second row = platform picker (if both keys set).
    """
    state        = _STATE.get(uid, {})
    cur_crf      = state.get("crf", 20)
    cur_platform = state.get("platform", _default_platform())

    def _crf_btn(label: str, crf: int) -> InlineKeyboardButton:
        tick = " ✓" if crf == cur_crf else ""
        return InlineKeyboardButton(f"{label}{tick}", callback_data=f"hs_crf|{uid}|{crf}")

    def _plat_btn(label: str, plat: str) -> InlineKeyboardButton:
        tick = " ✓" if plat == cur_platform else ""
        return InlineKeyboardButton(f"{label}{tick}", callback_data=f"hs_plat|{uid}|{plat}")

    rows = [
        [_crf_btn("🔵 HQ (18)", 18), _crf_btn("🟢 Default (20)", 20)],
        [_crf_btn("🟡 Medium (23)", 23), _crf_btn("🟠 Low (26)", 26)],
    ]

    # Show platform picker only if both keys are configured
    if _has_cc() and _has_fc():
        rows.append([
            _plat_btn("☁️ CloudConvert", "cc"),
            _plat_btn("🆓 FreeConvert",  "fc"),
        ])

    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"hs_cancel|{uid}")])
    return InlineKeyboardMarkup(rows)


def _crf_kb(uid: int) -> InlineKeyboardMarkup:
    """Alias kept for backward-compatibility — delegates to _subtitle_kb."""
    return _subtitle_kb(uid)


# ── Keyboards ─────────────────────────────────────────────────

def _more_or_done_kb(uid: int, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add another video",      callback_data=f"hs_more|{uid}"),
         InlineKeyboardButton(f"🟢 Done ({count}) → Sub",  callback_data=f"hs_done|{uid}")],
        [InlineKeyboardButton("❌ Cancel",                  callback_data=f"hs_cancel|{uid}")],
    ])


# ─────────────────────────────────────────────────────────────
# CRF picker callback
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^hs_crf\|"))
async def hardsub_crf_cb(client: Client, cb: CallbackQuery):
    """Handle CRF quality selection during waiting_subtitle step."""
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid.", show_alert=True)
    _, uid_s, crf_s = parts[:3]
    uid = int(uid_s) if uid_s.isdigit() else cb.from_user.id
    state = _user_state(uid)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    try:
        new_crf = int(crf_s)
    except ValueError:
        return

    state["crf"] = new_crf
    log.info("[Hardsub] uid=%d CRF set to %d", uid, new_crf)

    # Refresh the subtitle prompt with updated CRF display
    videos   = state.get("videos", [])
    fname    = videos[-1]["fname"] if videos else "video"
    path     = videos[-1].get("path") if videos else None
    fsize    = os.path.getsize(path) if path and os.path.isfile(path) else 0

    await _show_subtitle_prompt(cb.message, uid, fname, fsize)


# ─────────────────────────────────────────────────────────────
# Platform picker callback  hs_plat|<uid>|<platform>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^hs_plat\|"))
async def hardsub_platform_cb(client: Client, cb: CallbackQuery):
    """Handle platform selection (CloudConvert vs FreeConvert)."""
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid.", show_alert=True)
    _, uid_s, plat = parts[:3]
    uid   = int(uid_s) if uid_s.isdigit() else cb.from_user.id
    state = _user_state(uid)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    if plat not in ("cc", "fc"):
        return

    state["platform"] = plat
    log.info("[Hardsub] uid=%d platform → %s", uid, plat)

    videos = state.get("videos", [])
    fname  = videos[-1]["fname"] if videos else "video"
    path   = videos[-1].get("path") if videos else None
    fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
    await _show_subtitle_prompt(cb.message, uid, fname, fsize)
# ─────────────────────────────────────────────────────────────

async def _submit_one_job(
    api_key:   str,
    video:     dict,
    sub_path:  str,
    sub_fname: str,
    uid:       int,
    crf:       int  = 20,
    platform:  str  = "cc",
) -> tuple[str, str, bool]:
    video_fname = video.get("fname", "video.mkv")
    output_name = build_cc_output_name(video_fname, suffix="VOSTFR")

    log.info("[Hardsub] output=%s  platform=%s  CRF=%d", output_name, platform, crf)

    if platform == "fc":
        return await _submit_one_fc(api_key, video, sub_path, sub_fname,
                                    uid, crf, output_name, video_fname)

    # ── CloudConvert (default) ────────────────────────────────
    from services.cloudconvert_api import submit_hardsub
    from services.cc_job_store import cc_job_store, CCJob

    try:
        job_id = await submit_hardsub(
            api_key,
            video_path=video.get("path"),
            video_url=video.get("url"),
            subtitle_path=sub_path,
            output_name=output_name,
            scale_height=0,
            crf=crf,
        )
        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid, fname=video_fname,
            sub_fname=sub_fname, output_name=output_name,
            status="processing",
        ))
        log.info("[Hardsub-CC] Registered job %s uid=%d", job_id, uid)
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception as _pe:
            log.warning("[Hardsub-CC] Could not start CC poller: %s", _pe)
        return video_fname, job_id, True
    except Exception as exc:
        log.error("[Hardsub-CC] Job failed for %s: %s", video_fname, exc)
        return video_fname, str(exc)[:80], False


async def _submit_one_fc(
    api_key:     str,
    video:       dict,
    sub_path:    str,
    sub_fname:   str,
    uid:         int,
    crf:         int,
    output_name: str,
    video_fname: str,
) -> tuple[str, str, bool]:
    """Submit one hardsub job to FreeConvert with webhook delivery."""
    from services.freeconvert_api import submit_hardsub as fc_submit
    from services.fc_job_store import fc_job_store, FCJob

    try:
        # Resolve webhook URL from running tunnel
        webhook_url: str | None = None
        try:
            from core.config import get_tunnel_url
            from services.freeconvert_api import fc_webhook_url
            _turl = get_tunnel_url()
            if _turl:
                webhook_url = fc_webhook_url(_turl)
        except Exception:
            pass

        job_id = await fc_submit(
            api_key,
            video_path=video.get("path"),
            video_url=video.get("url"),
            subtitle_path=sub_path,
            output_name=output_name,
            crf=crf,
            preset="medium",
            webhook_url=webhook_url,
        )

        await fc_job_store.add(FCJob(
            job_id=job_id, uid=uid, fname=video_fname,
            sub_fname=sub_fname, output_name=output_name,
            status="processing", job_type="hardsub",
            api_key=api_key,
        ))
        log.info("[Hardsub-FC] Registered job %s uid=%d  webhook=%s",
                 job_id, uid, webhook_url or "none")
        return video_fname, job_id, True
    except Exception as exc:
        log.error("[Hardsub-FC] Job failed for %s: %s", video_fname, exc)
        return video_fname, str(exc)[:80], False


# ─────────────────────────────────────────────────────────────
# Submit all videos (batch)
# ─────────────────────────────────────────────────────────────

async def _submit_batch(st, state: dict, uid: int) -> None:
    videos    = state.get("videos", [])
    sub_path  = state["sub_path"]
    sub_fname = state.get("sub_fname", "subtitle.ass")
    crf       = state.get("crf", 20)
    platform  = state.get("platform", _default_platform())
    count     = len(videos)

    vid_list = "\n".join(
        f"  {i+1}. <code>{v['fname'][:40]}</code>"
        for i, v in enumerate(videos)
    )
    crf_lbl  = _crf_label(crf)
    plat_lbl = _platform_label(platform)

    await safe_edit(
        st,
        f"☁️ <b>Submitting {count} hardsub job{'s' if count > 1 else ''}…</b>\n"
        "──────────────────────\n\n"
        f"{vid_list}\n\n"
        f"💬 <code>{sub_fname[:42]}</code>\n"
        f"⚙️ Quality: {crf_lbl}\n"
        f"🌐 Platform: {plat_lbl}\n\n"
        "<i>Checking API key and creating jobs…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    if platform == "fc":
        api_key = os.environ.get("FC_API_KEY", "").strip()
        key_src = "FC_API_KEY"
    else:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        key_src = "CC_API_KEY"

    if not api_key:
        await safe_edit(
            st,
            f"❌ <b>{key_src} not set</b>\n\n"
            f"Add <code>{key_src}=your_key</code> to .env or Colab secrets.",
            parse_mode=enums.ParseMode.HTML,
        )
        _clear(uid)
        return

    # Key credit check (CC only — FC credits checked per-job)
    key_info = ""
    if platform == "cc":
        try:
            from services.cloudconvert_api import parse_api_keys, pick_best_key
            keys = parse_api_keys(api_key)
            if len(keys) > 1:
                selected, credits = await pick_best_key(keys)
                key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
            else:
                key_info = "🔑 1 CC API key"
        except Exception as exc:
            await safe_edit(
                st,
                f"❌ <b>All CC API keys exhausted</b>\n\n<code>{str(exc)[:200]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            _clear(uid)
            return
    else:
        try:
            from services.freeconvert_api import parse_fc_keys, pick_best_fc_key
            fc_keys = parse_fc_keys(api_key)
            if len(fc_keys) > 1:
                best_key, minutes = await pick_best_fc_key(fc_keys)
                key_info = f"🔑 Key {fc_keys.index(best_key)+1}/{len(fc_keys)} ({minutes:.0f} min left)"
                api_key  = best_key
            else:
                key_info = "🔑 1 FC API key"
        except Exception as exc:
            await safe_edit(
                st,
                f"❌ <b>All FC API keys exhausted</b>\n\n<code>{str(exc)[:200]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            _clear(uid)
            return

    results: list[str] = []
    ok_count = 0

    for i, video in enumerate(videos):
        vname, result, success = await _submit_one_job(
            api_key, video, sub_path, sub_fname, uid, crf=crf, platform=platform,
        )
        if success:
            results.append(f"✅ {i+1}. <code>{vname[:35]}</code> → <code>{result}</code>")
            ok_count += 1
        else:
            results.append(f"❌ {i+1}. <code>{vname[:35]}</code> — {result}")

    result_text = "\n".join(results)
    wh_note = (
        "⬆️ <i>Webhook active — result uploads automatically.</i>"
        if platform == "fc"
        else "⏳ <i>CloudConvert is processing…\nThe webhook will auto-upload results.</i>"
    )
    await safe_edit(
        st,
        f"{'✅' if ok_count == count else '⚠️'} <b>Hardsub — {ok_count}/{count} submitted</b>\n"
        "──────────────────────\n\n"
        f"{result_text}\n\n"
        f"💬 <code>{sub_fname[:38]}</code>\n"
        f"⚙️ {crf_lbl}  ·  🌐 {plat_lbl}\n"
        f"{key_info}\n\n"
        f"{wh_note}",
        parse_mode=enums.ParseMode.HTML,
    )

    log.info("[Hardsub] Batch: %d/%d jobs submitted for uid=%d (CRF=%d)",
             ok_count, count, uid, crf)
    _clear(uid)


# ─────────────────────────────────────────────────────────────
# Helper: video added to batch
# ─────────────────────────────────────────────────────────────

async def _video_added(msg_or_st, state: dict, uid: int, fname: str) -> None:
    videos = state.get("videos", [])
    count  = len(videos)
    vid_list = "\n".join(
        f"  {i+1}. <code>{v['fname'][:40]}</code>"
        for i, v in enumerate(videos)
    )
    await safe_edit(
        msg_or_st,
        f"✅ <b>Video {count} added!</b>\n"
        "──────────────────────\n\n"
        f"{vid_list}\n\n"
        "Send <b>another video</b> or tap <b>Done</b> to continue to subtitle.",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_more_or_done_kb(uid, count),
    )


# ─────────────────────────────────────────────────────────────
# /hardsub command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("hardsub"))
async def cmd_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")

    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        return await msg.reply(
            "❌ <b>CloudConvert API key not set</b>\n\n"
            "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.\n\n"
            "Get a key at: cloudconvert.com → Dashboard → API → API Keys",
            parse_mode=enums.ParseMode.HTML,
        )

    _clear(uid)
    tmp = make_tmp(cfg.download_dir, uid)
    _STATE[uid] = {
        "step":      "waiting_video",
        "tmp":       tmp,
        "videos":    [],
        "sub_path":  None,
        "sub_fname": None,
        "crf":       20,
        "platform":  _default_platform(),
    }

    await msg.reply(
        "🔥 <b>CloudConvert Hardsub</b>\n"
        "──────────────────────\n\n"
        "Send me the <b>video</b>:\n"
        "• A <b>video file</b> (upload from Telegram)\n"
        "• A <b>direct URL</b> (HTTP link to .mkv/.mp4)\n"
        "• A <b>magnet link</b> (downloaded via aria2 first)\n\n"
        "📦 <i>You can send multiple videos — they'll all get\n"
        "the same subtitle burned in.</i>\n\n"
        "⚙️ <i>Quality can be set when I ask for the subtitle.</i>\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.command("cancel"), group=4)
async def cmd_cancel_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid in _STATE:
        _clear(uid)
        await msg.reply("❌ Hardsub cancelled.")
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Flow buttons: more / done / cancel
# ─────────────────────────────────────────────────────────────

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
            f"📦 <b>{len(state['videos'])} video(s) queued</b>\n\n"
            "Send the next <b>video</b> (file / URL / magnet):",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if action == "done":
        if not state["videos"]:
            return await cb.answer("No videos added yet!", show_alert=True)
        state["step"] = "waiting_subtitle"
        count  = len(state["videos"])
        fname  = state["videos"][0]["fname"]
        path   = state["videos"][0].get("path")
        fsize  = os.path.getsize(path) if path and os.path.isfile(path) else 0
        await _show_subtitle_prompt(cb.message, uid, fname, fsize)


# ─────────────────────────────────────────────────────────────
# Step 1: Receive video FILE
# ─────────────────────────────────────────────────────────────

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
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    ext   = os.path.splitext(fname)[1].lower()

    _VIDEO_EXTS = {
        ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
        ".ts", ".m2ts", ".wmv", ".m4v",
    }
    if ext not in _VIDEO_EXTS and not msg.video:
        return

    fsize = getattr(media, "file_size", 0) or 0
    st    = await msg.reply(
        f"⬇️ Downloading <code>{fname[:40]}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        from services.tg_download import tg_download
        path = await tg_download(
            client, media.file_id,
            os.path.join(state["tmp"], fname), st,
            fname=fname, fsize=fsize, user_id=uid,
        )
        state["videos"].append({
            "path":  path,
            "url":   None,
            "fname": os.path.basename(path),
        })
        await _video_added(st, state, uid, fname)
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)

    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 1: Receive video URL / magnet / subtitle URL
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start","help","settings","info","status","log","restart",
         "broadcast","admin","ban_user","unban_user","banned_list",
         "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","stream","forward","createarchive","archiveddone","mergedone",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
         "resize","compress","usage","allow","deny","allowed"]
    ),
    group=1,
)
async def hardsub_url_handler(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _user_state(uid)
    if not state:
        return
    if state["step"] not in ("waiting_video", "waiting_subtitle"):
        return

    text   = msg.text.strip()
    url_re = re.compile(r"^(https?://\S+|magnet:\?\S+)$", re.I)
    if not url_re.match(text):
        return

    if state["step"] == "waiting_subtitle":
        await _handle_subtitle_url(msg, state, text, uid)
        msg.stop_propagation()
        return

    from services.downloader import classify
    kind = classify(text)

    if kind == "direct":
        raw_name = text.split("/")[-1].split("?")[0]
        fname    = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"
        st = await msg.reply(
            f"⬇️ <b>Downloading video…</b>\n<code>{fname[:40]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = state["tmp"]
        try:
            from services.downloader import download_direct as _dl
            path = await _dl(text, tmp)
            if not os.path.isfile(path):
                raise FileNotFoundError("No output file after download")
            fname = os.path.basename(path)
            state["videos"].append({"path": path, "url": None, "fname": fname})
            await _video_added(st, state, uid, fname)
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        msg.stop_propagation()

    # FIX BUG-04: Added "scrape" to the elif tuple.
    elif kind in ("magnet","torrent","ytdlp","gdrive","mediafire","scrape"):
        st = await msg.reply(
            f"⬇️ Downloading video via {kind}…\n"
            "<i>This may take a while for magnets.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = state["tmp"]
        try:
            from services.downloader import smart_download
            from services.utils import largest_file
            path = await smart_download(text, tmp, user_id=uid, label="Hardsub DL")
            if os.path.isdir(path):
                resolved = largest_file(path)
                if resolved:
                    path = resolved
            if not os.path.isfile(path):
                raise FileNotFoundError("No output file found")
            fname = os.path.basename(path)
            state["videos"].append({"path": path, "url": None, "fname": fname})
            await _video_added(st, state, uid, fname)
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2a: Receive subtitle FILE
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.document,
    group=0,
)
async def hardsub_subtitle_file(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_subtitle":
        return

    media = msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext   = os.path.splitext(fname)[1].lower()

    if ext not in _SUB_EXTS:
        await msg.reply(
            f"❌ <b>Unsupported file type</b>: <code>{ext or 'unknown'}</code>\n\n"
            "Please send a subtitle file:\n"
            "<code>.ass  .srt  .vtt  .ssa  .sub  .txt</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()
        return

    tmp = state["tmp"]
    st  = await msg.reply("⬇️ Downloading subtitle…")

    try:
        sub_path = await client.download_media(
            media, file_name=os.path.join(tmp, fname)
        )
        state["sub_path"]  = sub_path
        state["sub_fname"] = os.path.basename(sub_path)
    except Exception as exc:
        await safe_edit(st, f"❌ Subtitle download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)
        msg.stop_propagation()
        return

    await _submit_batch(st, state, uid)
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2b: Receive subtitle URL
# ─────────────────────────────────────────────────────────────

async def _handle_subtitle_url(
    msg: Message, state: dict, url: str, uid: int,
) -> None:
    tmp = state["tmp"]

    parsed_path = _urlparse.urlparse(url).path
    raw_fname   = os.path.basename(parsed_path)
    fname       = _urlparse.unquote_plus(raw_fname) if raw_fname else "subtitle.ass"
    ext         = os.path.splitext(fname)[1].lower()
    if ext not in _SUB_EXTS:
        fname = fname + ".ass" if fname else "subtitle.ass"
    fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

    st = await msg.reply(
        f"⬇️ Downloading subtitle from URL…\n<code>{url[:60]}</code>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        sub_path = os.path.join(tmp, fname)
        headers  = {"User-Agent": "Mozilla/5.0"}

        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, allow_redirects=True) as resp:
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
            _clear(uid)
            return

        with open(sub_path, "wb") as f:
            f.write(content)

        state["sub_path"]  = sub_path
        state["sub_fname"] = fname
        log.info("[Hardsub] Subtitle from URL: %s (%s)",
                 fname, human_size(os.path.getsize(sub_path)))

    except Exception as exc:
        log.error("[Hardsub] Subtitle URL failed: %s", exc)
        await safe_edit(
            st,
            f"❌ Subtitle download failed:\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        _clear(uid)
        return

    await _submit_batch(st, state, uid)
