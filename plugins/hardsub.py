"""
plugins/hardsub.py
CloudConvert-powered hardsubbing — batch multi-video support.

Quality selection REMOVED — always uses CloudConvert default settings
(scale_height=0 means original resolution is preserved server-side).
"""
from __future__ import annotations

import asyncio
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


# ── Public entry-point used by cc_buttons.py / url_handler.py ──

async def start_hardsub_for_url(
    client: "Client",
    st,
    uid: int,
    url: str,
    fname: str,
) -> None:
    """
    Start the hardsub flow with a pre-resolved direct video URL.
    Called from url_handler.py when the user clicks the Hardsub button
    on a URL that has already been resolved to a direct link.
    The video is NOT downloaded locally — CloudConvert fetches it by URL.
    """
    _clear(uid)
    tmp = make_tmp(cfg.download_dir, uid)
    _STATE[uid] = {
        "step":      "waiting_subtitle",
        "tmp":       tmp,
        "videos":    [{"path": None, "url": url, "fname": fname}],
        "sub_path":  None,
        "sub_fname": None,
    }
    await safe_edit(
        st,
        "🔥 <b>Hardsub</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{fname[:45]}</code>\n"
        "☁️ <i>CloudConvert will fetch the video directly</i>\n\n"
        "Now send the <b>subtitle</b>:\n"
        "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ── Keyboards ─────────────────────────────────────────────────

def _more_or_done_kb(uid: int, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add another video",      callback_data=f"hs_more|{uid}"),
         InlineKeyboardButton(f"✅ Done ({count}) → Sub",  callback_data=f"hs_done|{uid}")],
        [InlineKeyboardButton("❌ Cancel",                  callback_data=f"hs_cancel|{uid}")],
    ])


# ─────────────────────────────────────────────────────────────
# Submit one hardsub job (always original resolution)
# ─────────────────────────────────────────────────────────────

async def _submit_one_job(
    api_key: str,
    video: dict,
    sub_path: str,
    sub_fname: str,
    uid: int,
) -> tuple[str, str, bool]:
    from services.cloudconvert_api import submit_hardsub

    video_fname = video.get("fname", "video.mkv")
    name_base   = os.path.splitext(video_fname)[0]
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + " [VOSTFR].mp4"

    try:
        job_id = await submit_hardsub(
            api_key,
            video_path=video.get("path"),
            video_url=video.get("url"),
            subtitle_path=sub_path,
            output_name=output_name,
            scale_height=0,  # Always use original resolution
        )
        return video_fname, job_id, True
    except Exception as exc:
        log.error("[Hardsub] Job failed for %s: %s", video_fname, exc)
        return video_fname, str(exc)[:80], False


# ─────────────────────────────────────────────────────────────
# Submit all videos (batch)
# ─────────────────────────────────────────────────────────────

async def _submit_batch(st, state: dict, uid: int) -> None:
    videos    = state.get("videos", [])
    sub_path  = state["sub_path"]
    sub_fname = state.get("sub_fname", "subtitle.ass")
    count     = len(videos)

    vid_list = "\n".join(
        f"  {i+1}. <code>{v['fname'][:40]}</code>"
        for i, v in enumerate(videos)
    )
    await safe_edit(
        st,
        f"☁️ <b>Submitting {count} hardsub job{'s' if count > 1 else ''}…</b>\n"
        "──────────────────────\n\n"
        f"{vid_list}\n\n"
        f"💬 <code>{sub_fname[:42]}</code>\n\n"
        "<i>Checking API key and creating jobs…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    api_key = os.environ.get("CC_API_KEY", "").strip()

    try:
        from services.cloudconvert_api import parse_api_keys, pick_best_key
        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
        else:
            key_info = "🔑 1 API key"
    except Exception as exc:
        await safe_edit(
            st,
            f"❌ <b>All API keys exhausted</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        _clear(uid)
        return

    results: list[str] = []
    ok_count = 0

    for i, video in enumerate(videos):
        vname, result, success = await _submit_one_job(
            api_key, video, sub_path, sub_fname, uid,
        )
        if success:
            results.append(f"✅ {i+1}. <code>{vname[:35]}</code> → <code>{result}</code>")
            ok_count += 1
        else:
            results.append(f"❌ {i+1}. <code>{vname[:35]}</code> — {result}")

    result_text = "\n".join(results)
    await safe_edit(
        st,
        f"{'✅' if ok_count == count else '⚠️'} <b>Hardsub — {ok_count}/{count} submitted</b>\n"
        "──────────────────────\n\n"
        f"{result_text}\n\n"
        f"💬 <code>{sub_fname[:38]}</code>\n"
        f"{key_info}\n\n"
        "⏳ <i>CloudConvert is processing…\n"
        "The webhook will auto-upload results to this chat.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    log.info("[Hardsub] Batch: %d/%d jobs submitted for uid=%d", ok_count, count, uid)
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
        count = len(state["videos"])
        await cb.message.edit(
            f"✅ <b>{count} video{'s' if count > 1 else ''} queued</b>\n\n"
            "Now send the <b>subtitle</b> (one for all):\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file",
            parse_mode=enums.ParseMode.HTML,
        )


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

    _VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
                   ".ts", ".m2ts", ".wmv", ".m4v"}
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


# Step 1: Receive video URL / magnet / subtitle URL
@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start", "help", "settings", "info", "status", "log", "restart",
         "broadcast", "admin", "ban_user", "unban_user", "banned_list",
         "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
         "hardsub", "stream", "forward", "createarchive", "archiveddone", "mergedone"]
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

    # ── Subtitle URL ──────────────────────────────────────────
    if state["step"] == "waiting_subtitle":
        await _handle_subtitle_url(msg, state, text, uid)
        msg.stop_propagation()
        return

    # ── Video URL ─────────────────────────────────────────────
    from services.downloader import classify
    kind = classify(text)

    if kind == "direct":
        raw_name = text.split("/")[-1].split("?")[0]
        fname    = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"
        state["videos"].append({"path": None, "url": text, "fname": fname})
        st = await msg.reply(
            f"✅ Video URL added: <code>{fname[:40]}</code>\n"
            "☁️ <i>CloudConvert will fetch directly</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        await _video_added(st, state, uid, fname)
        msg.stop_propagation()

    elif kind in ("magnet", "torrent", "ytdlp", "gdrive", "mediafire"):
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

    # Go straight to batch submission — no resolution picker
    await _submit_batch(st, state, uid)
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2b: Receive subtitle URL
# ─────────────────────────────────────────────────────────────

async def _handle_subtitle_url(msg: Message, state: dict, url: str, uid: int) -> None:
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

    # Go straight to batch submission — no resolution picker
    await _submit_batch(st, state, uid)
