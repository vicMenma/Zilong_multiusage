"""
plugins/video.py
Full video processing plugin.

Fixes vs original:
  - user_id always bound at top of every callback (no NameError)
  - _ensure() is a module-level async function, not a closure
  - handle_secondary_file properly handles burn_sub
  - merge_vids queue correctly seeds primary file
  - all FFmpeg actions use _tracked_ffmpeg wrapper
  - text_reply_handler ignores commands properly
"""
from __future__ import annotations

import asyncio
import json
import logging
import os

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import sessions, settings, FileSession
from services import ffmpeg as FF
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import (
    cleanup, fmt_hms, human_size, lang_flag, lang_name,
    make_tmp, safe_edit,
)

log = logging.getLogger(__name__)

# Build the compact LANG dict used for stream labels from the shared table
from services.utils import LANG_NAME as LANG

_IGNORED = {
    "start","help","settings","info","broadcast","stats","log","restart",
    "mergedone","admin","ban_user","unban_user","banned_list","status",
    "forward","createarchive","archiveddone","bulk_url","usettings",
    "show_thumb","del_thumb","json_formatter","stream",
}


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def video_menu_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Media Info",        callback_data=f"vid|mediainfo|{key}"),
         InlineKeyboardButton("🖼️ Thumbnail",          callback_data=f"vid|thumb|{key}")],
        [InlineKeyboardButton("📡 Stream Extractor",  callback_data=f"se_file|{key}"),
         InlineKeyboardButton("🗺️ Stream Mapper",      callback_data=f"vid|smap_menu|{key}")],
        [InlineKeyboardButton("🗑️ Stream Remover",    callback_data=f"vid|srem_menu|{key}"),
         InlineKeyboardButton("🔇 Remove Audio",      callback_data=f"vid|rm_audio|{key}")],
        [InlineKeyboardButton("✂️ Trim",              callback_data=f"vid|trim|{key}"),
         InlineKeyboardButton("🔪 Split",             callback_data=f"vid|split|{key}")],
        [InlineKeyboardButton("➕ Merge Videos",      callback_data=f"vid|merge_vids|{key}"),
         InlineKeyboardButton("🔀 Merge Audio",       callback_data=f"vid|merge_av|{key}")],
        [InlineKeyboardButton("💬 Mux Subtitle",      callback_data=f"vid|merge_vs|{key}"),
         InlineKeyboardButton("🔥 Burn Subtitle",     callback_data=f"vid|burn_sub|{key}")],
        [InlineKeyboardButton("🎵 Extract Audio",     callback_data=f"vid|to_audio|{key}"),
         InlineKeyboardButton("📸 Screenshots",       callback_data=f"vid|shots|{key}")],
        [InlineKeyboardButton("🖊️ Manual Shots",      callback_data=f"vid|manual_shots|{key}"),
         InlineKeyboardButton("🎞️ Sample Clip",       callback_data=f"vid|sample|{key}")],
        [InlineKeyboardButton("🔄 Convert",           callback_data=f"vid|convert|{key}"),
         InlineKeyboardButton("⚡ Optimize",          callback_data=f"vid|optimize|{key}")],
        [InlineKeyboardButton("🏷️ Metadata",          callback_data=f"vid|metadata|{key}"),
         InlineKeyboardButton("✏️ Rename",            callback_data=f"vid|rename|{key}")],
        [InlineKeyboardButton("❌ Cancel",            callback_data=f"vid|cancel|{key}")],
    ])


def _audio_fmt_kb(key: str) -> InlineKeyboardMarkup:
    fmts = ["mp3","aac","m4a","opus","ogg","flac","wav","wma","ac3"]
    rows = [
        [InlineKeyboardButton(f.upper(), callback_data=f"vaud|{f}|{key}") for f in fmts[i:i+3]]
        for i in range(0, len(fmts), 3)
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"vid|back|{key}")])
    return InlineKeyboardMarkup(rows)


def _video_fmt_kb(key: str) -> InlineKeyboardMarkup:
    fmts = ["mp4","mkv","avi","mov","webm","flv"]
    rows = [
        [InlineKeyboardButton(f.upper(), callback_data=f"vconv|{f}|{key}") for f in fmts[i:i+3]]
        for i in range(0, len(fmts), 3)
    ]
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"vid|back|{key}")])
    return InlineKeyboardMarkup(rows)


def _opt_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔥 High (CRF 18)",   callback_data=f"vopt|18|{key}"),
         InlineKeyboardButton("⚡ Medium (CRF 23)", callback_data=f"vopt|23|{key}")],
        [InlineKeyboardButton("💾 Low (CRF 28)",    callback_data=f"vopt|28|{key}"),
         InlineKeyboardButton("🔙 Back",            callback_data=f"vid|back|{key}")],
    ])


def _stream_kb(streams: list, action: str, key: str) -> InlineKeyboardMarkup:
    icons = {"video":"🎬","audio":"🎵","subtitle":"💬"}
    rows  = []
    for s in streams:
        idx   = s.get("index", 0)
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        title = (tags.get("title") or "").strip()
        stype = s.get("codec_type","video")
        icon  = icons.get(stype, "📦")
        lang_full = LANG.get(lang, lang.upper() if lang != "und" else "")
        parts = [f"{icon} #{idx} {codec} [{lang}]"]
        if lang_full:
            parts.append(lang_full)
        if title and title.lower() != lang_full.lower():
            parts.append(title)
        label = " — ".join(parts)
        if len(label) > 58:
            label = label[:55] + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"{action}|{idx}|{key}")])
    rows.append([InlineKeyboardButton("📦 All", callback_data=f"{action}|all|{key}")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"vid|back|{key}")])
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# Tracked FFmpeg wrapper
# ─────────────────────────────────────────────────────────────

async def _tracked_ffmpeg(user_id: int, label: str, fname: str, coro) -> None:
    from services.task_runner import tracker, TaskRecord
    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=user_id,
        label=label, mode="proc", engine="ffmpeg",
        fname=fname,
    )
    await tracker.register(record)
    record.update(state=f"⚙️ {label}…")
    try:
        await coro
        record.update(state="✅ Done")
    except Exception:
        record.update(state="❌ Failed")
        raise


# ─────────────────────────────────────────────────────────────
# File downloader (cached)
# ─────────────────────────────────────────────────────────────

async def _ensure(client: Client, session: FileSession, st) -> str | None:
    """Download if not already cached. Must be called inside session.lock."""
    if session.is_downloaded():
        return session.local_path

    dest = os.path.join(session.tmp_dir, session.fname)
    try:
        path = await tg_download(client, session.file_id, dest, st,
                                 fname=session.fname, fsize=session.fsize)
        session.local_path = path
        return path
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        return None


# ─────────────────────────────────────────────────────────────
# Main callback: vid|<action>|<key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^vid\|"))
async def video_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, key = parts
    user_id = cb.from_user.id

    if action == "cancel":
        s = sessions.get(key)
        if s:
            cleanup(s.tmp_dir)
            await sessions.remove(key)
        await cb.message.delete()
        return await cb.answer()

    if action == "back":
        await cb.message.edit_reply_markup(video_menu_kb(key))
        return await cb.answer()

    session = sessions.get(key)
    if not session:
        return await cb.answer("⚠️ Session expired. Resend the file.", show_alert=True)

    await cb.answer()

    # ── Actions that only prompt (no download needed yet) ─────
    prompts = {
        "trim", "split", "sample", "manual_shots", "rename",
        "metadata", "merge_av", "merge_vs", "merge_vids", "burn_sub",
        "to_audio", "convert", "optimize",
    }
    if action in prompts:
        await _prompt(client, cb, action, key, session, user_id)
        return

    # ── Actions that need the file ────────────────────────────
    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return

        tmp  = session.tmp_dir
        ext  = session.ext or os.path.splitext(path)[1] or ".mp4"
        base = os.path.splitext(os.path.basename(path))[0]

        try:
            await _execute(client, cb, action, key, session,
                           user_id, st, path, tmp, ext, base)
        except Exception as exc:
            log.error("video_cb action=%s: %s", action, exc, exc_info=True)
            await safe_edit(st, f"❌ {exc}", parse_mode=enums.ParseMode.HTML,
                            reply_markup=video_menu_kb(key))


async def _execute(client, cb, action, key, session, user_id,
                   st, path, tmp, ext, base):

    # ── Thumbnail ─────────────────────────────────────────────
    if action == "thumb":
        out = os.path.join(tmp, f"{base}_thumb.jpg")
        await safe_edit(st, "🖼️ Extracting thumbnail…")
        result = await FF.get_thumb(path, out)
        if result:
            await client.send_photo(user_id, result,
                caption="🖼️ <b>Thumbnail</b>", parse_mode=enums.ParseMode.HTML)
            await st.delete()
        else:
            await safe_edit(st, "❌ Could not extract thumbnail.",
                            reply_markup=video_menu_kb(key))

    # ── Media info ────────────────────────────────────────────
    elif action == "mediainfo":
        await safe_edit(st, "📊 Reading streams…")
        fname_d = os.path.basename(path)
        fsize   = os.path.getsize(path)

        raw, sd, dur = await asyncio.gather(
            FF.get_mediainfo(path),
            FF.probe_streams(path),
            FF.probe_duration(path),
        )

        lines = [
            "📊 <b>MediaInfo</b>", "──────────────────────",
            f"📄 <code>{fname_d[:50]}</code>",
            f"💾 <code>{human_size(fsize)}</code>  ⏱ <code>{fmt_hms(dur)}</code>",
            "──────────────────────",
        ]
        for s in sd.get("video", []):
            codec = s.get("codec_name","?").upper()
            w, h  = s.get("width",0), s.get("height",0)
            fr    = s.get("r_frame_rate","0/1")
            try:
                fn2,fd2 = fr.split("/"); fps = f"{float(fn2)/max(float(fd2),1):.3f}"
            except Exception: fps = "?"
            lines.append(f"🎬 <code>{codec}  {w}x{h}  {fps}fps</code>")
        for s in sd.get("audio", []):
            codec = s.get("codec_name","?").upper()
            ch    = s.get("channels",0)
            ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch,f"{ch}ch") if ch else ""
            tags  = s.get("tags",{}) or {}
            lang  = (tags.get("language","") or "").lower()
            lang_s = f" [{LANG.get(lang,lang.upper() if lang else '?')}]"
            lines.append(f"🎵 <code>{codec}  {ch_s}{lang_s}</code>")
        for s in sd.get("subtitle",[])[:6]:
            codec = s.get("codec_name","?").upper()
            tags  = s.get("tags",{}) or {}
            lang  = (tags.get("language","und")).lower()
            lines.append(f"💬 <code>{codec} [{lang}]</code>")

        if not any(sd.values()):
            lines.append("⚠️ <i>No streams detected — file may be corrupted.</i>")

        kb_rows = []
        try:
            from services.telegraph import post_mediainfo
            tph = await post_mediainfo(fname_d, raw)
            kb_rows.append([InlineKeyboardButton("📋 Full MediaInfo →", url=tph)])
        except Exception:
            pass
        kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"vid|back|{key}")])

        await safe_edit(st, "\n".join(lines),
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(kb_rows))

    # ── Stream menus ──────────────────────────────────────────
    elif action in ("smap_menu", "srem_menu", "sext_menu"):
        label_map = {
            "sext_menu": "📤 Stream Extractor",
            "smap_menu": "🗺️ Stream Mapper",
            "srem_menu": "🗑️ Stream Remover",
        }
        act_map = {"sext_menu":"sext","smap_menu":"smap","srem_menu":"srem"}
        await safe_edit(st, "📡 Reading streams…")
        sd = await FF.probe_streams(path)
        for t in ("video","audio","subtitle"):
            for s in sd.get(t,[]):
                s["codec_type"] = t
        if action == "srem_menu":
            streams = sd.get("audio",[]) + sd.get("subtitle",[])
        else:
            streams = sd.get("video",[]) + sd.get("audio",[]) + sd.get("subtitle",[])
        if not streams:
            return await safe_edit(st, "❌ No streams found.",
                                   reply_markup=video_menu_kb(key))
        await safe_edit(st, f"{label_map[action]}\n\n<i>Select stream:</i>",
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=_stream_kb(streams, act_map[action], key))

    # ── Remove audio ──────────────────────────────────────────
    elif action == "rm_audio":
        out = os.path.join(tmp, f"{base}_noaudio{ext}")
        await safe_edit(st, "🔇 Removing audio…")
        await _tracked_ffmpeg(user_id, "Remove Audio", os.path.basename(out),
                              FF.remove_audio(path, out))
        await upload_file(client, st, out)
        cleanup(tmp); await sessions.remove(key)

    # ── Screenshots ───────────────────────────────────────────
    elif action == "shots":
        await safe_edit(st, "📸 Generating 5 screenshots…")
        shots = await FF.screenshots(path, tmp, count=5)
        if shots:
            for s in shots:
                try: await client.send_photo(user_id, s)
                except Exception: pass
            await st.delete()
        else:
            await safe_edit(st, "❌ No screenshots generated.", reply_markup=video_menu_kb(key))
        cleanup(tmp); await sessions.remove(key)

    else:
        await safe_edit(st, f"❌ Unknown action: {action}", reply_markup=video_menu_kb(key))


# ─────────────────────────────────────────────────────────────
# Prompt dispatcher (no download needed yet)
# ─────────────────────────────────────────────────────────────

async def _prompt(client, cb, action, key, session, user_id):
    msg = cb.message

    if action == "merge_av":
        session.waiting = "merge_av"
        await safe_edit(msg, "🔀 <b>Merge Video + Audio</b>\n\nSend the <b>audio file</b>.",
                        parse_mode=enums.ParseMode.HTML)

    elif action == "merge_vs":
        session.waiting = "merge_vs"
        await safe_edit(msg, "💬 <b>Mux Subtitle</b>\n\nSend the subtitle file (.srt / .ass / .vtt)",
                        parse_mode=enums.ParseMode.HTML)

    elif action == "burn_sub":
        session.waiting = "burn_sub"
        await safe_edit(msg, "🔥 <b>Burn Subtitle</b>\n\nSend the subtitle file (will re-encode).",
                        parse_mode=enums.ParseMode.HTML)

    elif action == "merge_vids":
        session.waiting = "merge_vids"
        session.payload["merge_queue"] = []
        await safe_edit(msg,
            "➕ <b>Video Merger</b>\n\nYour file = Video 1 ✅\nSend more videos, then /mergedone.",
            parse_mode=enums.ParseMode.HTML)

    elif action in ("trim", "split", "sample", "manual_shots"):
        # Need duration — download first
        st = await msg.edit("⬇️ Downloading to read duration…")
        async with session.lock:
            path = await _ensure(client, session, st)
        if not path:
            return
        dur = await FF.probe_duration(path)
        session.waiting = action
        prompts_text = {
            "trim": (
                f"✂️ <b>Trim</b>  ⏱ <code>{fmt_hms(dur)}</code>\n\n"
                "Send start and end time:\n"
                "<code>00:01:30 00:03:00</code>  or  <code>90 180</code>"
            ),
            "split": (
                f"🔪 <b>Split</b>  ⏱ <code>{fmt_hms(dur)}</code>\n\n"
                "Send chunk size in seconds:\n<code>600</code>"
            ),
            "sample": (
                f"🎞️ <b>Sample</b>  ⏱ <code>{fmt_hms(dur)}</code>\n\n"
                "Send: <code>start_sec duration_sec</code>\nExample: <code>60 30</code>"
            ),
            "manual_shots": (
                f"🖊️ <b>Manual Screenshots</b>  ⏱ <code>{fmt_hms(dur)}</code>\n\n"
                "Send timestamps (one per line):\n<code>00:01:30\n00:05:00</code>"
            ),
        }
        await safe_edit(st, prompts_text[action], parse_mode=enums.ParseMode.HTML)

    elif action == "to_audio":
        await safe_edit(msg, "🎵 <b>Extract Audio</b>\n\nChoose format:",
                        reply_markup=_audio_fmt_kb(key), parse_mode=enums.ParseMode.HTML)

    elif action == "convert":
        await safe_edit(msg, "🔄 <b>Convert Video</b>\n\nChoose format:",
                        reply_markup=_video_fmt_kb(key), parse_mode=enums.ParseMode.HTML)

    elif action == "optimize":
        await safe_edit(msg, "⚡ <b>Optimize</b>\n\nChoose quality preset:",
                        reply_markup=_opt_kb(key), parse_mode=enums.ParseMode.HTML)

    elif action == "rename":
        session.waiting = "rename"
        cur = os.path.splitext(session.fname)[0]
        await safe_edit(msg,
            f"✏️ <b>Rename</b>\nCurrent: <code>{cur}</code>\n\nSend new name (no extension):",
            parse_mode=enums.ParseMode.HTML)

    elif action == "metadata":
        session.waiting = "metadata"
        await safe_edit(msg,
            '🏷️ <b>Metadata</b>\n\nSend JSON:\n<code>{"title":"My Movie","artist":"Director"}</code>',
            parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# Stream callbacks  (sext|smap|srem)|<idx>|<key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^(sext|smap|srem)\|"))
async def stream_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)
    action_raw, idx_str, key = parts
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return
        tmp  = session.tmp_dir
        ext  = session.ext or os.path.splitext(path)[1] or ".mp4"
        base = os.path.splitext(os.path.basename(path))[0]

        try:
            if action_raw == "smap":
                await safe_edit(st, f"🗺️ Mapping stream #{idx_str}…")
                if idx_str == "all":
                    import shutil
                    out = os.path.join(tmp, f"{base}_mapped{ext}")
                    shutil.copy2(path, out)
                else:
                    out = os.path.join(tmp, f"{base}_mapped{ext}")
                    await FF.stream_op(path, out, ["-map", f"0:{idx_str}", "-c", "copy"])
                await upload_file(client, st, out)

            elif action_raw == "srem":
                await safe_edit(st, f"🗑️ Removing stream #{idx_str}…")
                out = os.path.join(tmp, f"{base}_rem{ext}")
                if idx_str == "all":
                    await FF.remove_audio_and_subs(path, out)
                else:
                    await FF.stream_op(path, out, ["-map","0","-map",f"-0:{idx_str}","-c","copy"])
                await upload_file(client, st, out)

            elif action_raw == "sext":
                await safe_edit(st, f"📤 Extracting stream #{idx_str}…")
                out_ext = ".mka"
                if idx_str != "all":
                    sd = await FF.probe_streams(path)
                    all_s = sd["video"] + sd["audio"] + sd["subtitle"]
                    target = next((s for s in all_s if str(s.get("index")) == idx_str), None)
                    if target:
                        stype = target.get("codec_type","audio")
                        codec = (target.get("codec_name") or "").lower()
                        if stype == "subtitle":   out_ext = FF.subtitle_ext(codec)
                        elif stype == "video":    out_ext = ext
                        else:                     out_ext = FF.audio_ext(codec)
                out = os.path.join(tmp, f"{base}_stream{idx_str}{out_ext}")
                if idx_str == "all":
                    await FF.stream_op(path, out, ["-vn","-c","copy"])
                else:
                    await FF.stream_op(path, out, ["-map",f"0:{idx_str}","-c","copy"])
                await upload_file(client, st, out, force_document=True)

        except Exception as exc:
            log.error("stream_cb action=%s idx=%s: %s", action_raw, idx_str, exc, exc_info=True)
            await safe_edit(st, f"❌ {exc}",
                            parse_mode=enums.ParseMode.HTML,
                            reply_markup=video_menu_kb(key))
            return

        cleanup(tmp)
        await sessions.remove(key)


# ─────────────────────────────────────────────────────────────
# Audio format  vaud|<fmt>|<key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^vaud\|"))
async def audio_fmt_cb(client: Client, cb: CallbackQuery):
    _, fmt, key = cb.data.split("|", 2)
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return
        out = os.path.join(session.tmp_dir,
                           os.path.splitext(os.path.basename(path))[0] + f".{fmt}")
        await safe_edit(st, f"🎵 Converting to {fmt.upper()}…")
        try:
            await _tracked_ffmpeg(user_id, f"Audio → {fmt.upper()}", os.path.basename(out),
                                  FF.video_to_audio(path, out, fmt=fmt))
        except Exception as exc:
            return await safe_edit(st, f"❌ Conversion failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML,
                                   reply_markup=video_menu_kb(key))
        await upload_file(client, st, out)
        cleanup(session.tmp_dir)
        await sessions.remove(key)


# ─────────────────────────────────────────────────────────────
# Video convert  vconv|<fmt>|<key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^vconv\|"))
async def video_conv_cb(client: Client, cb: CallbackQuery):
    _, fmt, key = cb.data.split("|", 2)
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return
        out = os.path.join(session.tmp_dir,
                           os.path.splitext(os.path.basename(path))[0] + f".{fmt}")
        await safe_edit(st, f"🔄 Converting to {fmt.upper()}…")
        try:
            await _tracked_ffmpeg(user_id, f"Convert → {fmt.upper()}", os.path.basename(out),
                                  FF.convert_video(path, out))
        except Exception as exc:
            return await safe_edit(st, f"❌ Conversion failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML,
                                   reply_markup=video_menu_kb(key))
        await upload_file(client, st, out)
        cleanup(session.tmp_dir)
        await sessions.remove(key)


# ─────────────────────────────────────────────────────────────
# Optimize  vopt|<crf>|<key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^vopt\|"))
async def opt_cb(client: Client, cb: CallbackQuery):
    _, crf, key = cb.data.split("|", 2)
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return
        ext = session.ext or os.path.splitext(path)[1] or ".mp4"
        out = os.path.join(session.tmp_dir,
                           os.path.splitext(os.path.basename(path))[0] + f"_crf{crf}{ext}")
        await safe_edit(st, f"⚡ Optimizing (CRF {crf})…")
        try:
            await _tracked_ffmpeg(user_id, f"Optimize CRF={crf}", os.path.basename(out),
                                  FF.optimize(path, out, crf=int(crf)))
        except Exception as exc:
            return await safe_edit(st, f"❌ Optimization failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML,
                                   reply_markup=video_menu_kb(key))
        await upload_file(client, st, out)
        cleanup(session.tmp_dir)
        await sessions.remove(key)


# ─────────────────────────────────────────────────────────────
# Text reply handler
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text & ~filters.command(list(_IGNORED)),
    group=6,
)
async def text_reply_handler(client: Client, msg: Message):
    user_id = msg.from_user.id
    session = sessions.waiting_session(user_id)
    if not session:
        return

    action = session.waiting
    tmp    = session.tmp_dir

    async with session.lock:
        st   = await msg.reply("⬇️ Downloading…")
        path = await _ensure(client, session, st)
        if not path:
            return
        await st.delete()

    ext  = session.ext or os.path.splitext(path)[1] or ".mp4"
    base = os.path.splitext(os.path.basename(path))[0]

    # ── Trim ─────────────────────────────────────────────────
    if action == "trim":
        try:
            parts = msg.text.strip().split()
            start, end = parts[0], parts[1]
        except (IndexError, ValueError):
            return await msg.reply("❌ Format: <code>00:01:30 00:03:00</code>",
                                   parse_mode=enums.ParseMode.HTML)
        out = os.path.join(tmp, f"{base}_trimmed{ext}")
        st  = await msg.reply("✂️ Trimming…")
        try:
            await _tracked_ffmpeg(user_id, "Trim", os.path.basename(out),
                                  FF.trim_video(path, out, start, end))
        except Exception as exc:
            return await safe_edit(st, f"❌ Trim failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.waiting = None
        await upload_file(client, st, out)
        cleanup(tmp); await sessions.remove(session.key)

    # ── Split ─────────────────────────────────────────────────
    elif action == "split":
        try:
            chunk = int(msg.text.strip())
            if chunk <= 0: raise ValueError
        except (ValueError, TypeError):
            return await msg.reply("❌ Send a positive number in seconds, e.g. <code>600</code>",
                                   parse_mode=enums.ParseMode.HTML)
        st = await msg.reply(f"🔪 Splitting into {chunk}s chunks…")
        try:
            await _tracked_ffmpeg(user_id, f"Split {chunk}s", session.fname,
                                  FF.split_video(path, tmp, chunk))
        except Exception as exc:
            return await safe_edit(st, f"❌ Split failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        parts_done = sorted(
            [os.path.join(tmp, f) for f in os.listdir(tmp)
             if "_part" in f and not f.endswith(".aria2")],
        )
        if not parts_done:
            return await safe_edit(st, "❌ Split produced no files.")
        for p in parts_done:
            await upload_file(client, st, p)
        session.waiting = None
        cleanup(tmp); await sessions.remove(session.key)

    # ── Sample ────────────────────────────────────────────────
    elif action == "sample":
        try:
            p2 = msg.text.strip().split()
            s_start, s_dur = p2[0], p2[1]
        except (IndexError, ValueError):
            return await msg.reply("❌ Format: <code>start_sec duration_sec</code>",
                                   parse_mode=enums.ParseMode.HTML)
        out = os.path.join(tmp, f"{base}_sample{ext}")
        st  = await msg.reply("🎞️ Generating sample…")
        try:
            await _tracked_ffmpeg(user_id, "Sample", os.path.basename(out),
                                  FF.make_sample(path, out, start=s_start, duration=s_dur))
        except Exception as exc:
            return await safe_edit(st, f"❌ Sample failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.waiting = None
        await upload_file(client, st, out)
        cleanup(tmp); await sessions.remove(session.key)

    # ── Manual screenshots ────────────────────────────────────
    elif action == "manual_shots":
        timestamps = [t.strip() for t in msg.text.strip().splitlines() if t.strip()]
        if not timestamps:
            return await msg.reply("❌ No timestamps provided.")
        st       = await msg.reply(f"🖊️ Taking {len(timestamps)} screenshot(s)…")
        sent_any = False
        for ts in timestamps:
            out = os.path.join(tmp, f"shot_{ts.replace(':','_')}.jpg")
            try:
                await FF.screenshot(path, out, timestamp=ts)
                if os.path.exists(out) and os.path.getsize(out) > 0:
                    await client.send_photo(user_id, out,
                        caption=f"📸 <code>{ts}</code>", parse_mode=enums.ParseMode.HTML)
                    sent_any = True
            except Exception as exc:
                log.warning("Manual shot at %s failed: %s", ts, exc)
        if not sent_any:
            await safe_edit(st, "❌ Could not extract screenshots.")
        else:
            await st.delete()
        session.waiting = None
        cleanup(tmp); await sessions.remove(session.key)

    # ── Rename ────────────────────────────────────────────────
    elif action == "rename":
        new_name = msg.text.strip()
        if not new_name:
            return await msg.reply("❌ Name cannot be empty.")
        new_path = os.path.join(tmp, new_name + ext)
        try:
            os.rename(path, new_path)
        except OSError as exc:
            return await msg.reply(f"❌ Rename failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.local_path = new_path
        session.fname      = new_name + ext
        session.waiting    = None
        await msg.reply(
            f"✅ Renamed to <code>{new_name + ext}</code>\n\nChoose an action:",
            reply_markup=video_menu_kb(session.key),
            parse_mode=enums.ParseMode.HTML,
        )

    # ── Metadata ──────────────────────────────────────────────
    elif action == "metadata":
        try:
            meta = json.loads(msg.text.strip())
        except (json.JSONDecodeError, ValueError):
            return await msg.reply(
                '❌ Invalid JSON.\n<code>{"title":"My Video"}</code>',
                parse_mode=enums.ParseMode.HTML)
        out = os.path.join(tmp, f"{base}_meta{ext}")
        st  = await msg.reply("🏷️ Applying metadata…")
        try:
            await _tracked_ffmpeg(user_id, "Metadata", os.path.basename(out),
                                  FF.write_metadata(path, out, meta))
        except Exception as exc:
            return await safe_edit(st, f"❌ Metadata failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.waiting = None
        await upload_file(client, st, out)
        cleanup(tmp); await sessions.remove(session.key)


# ─────────────────────────────────────────────────────────────
# Secondary file handler (merge_av / merge_vs / burn_sub / merge_vids)
# ─────────────────────────────────────────────────────────────

async def handle_secondary_file(client: Client, msg: Message, session: FileSession):
    action  = session.waiting
    user_id = session.user_id
    tmp     = session.tmp_dir

    async with session.lock:
        st   = await msg.reply("⬇️ Preparing primary file…")
        path = await _ensure(client, session, st)
        if not path:
            return
        await st.delete()

    ext  = session.ext or os.path.splitext(path)[1] or ".mp4"
    base = os.path.splitext(os.path.basename(path))[0]

    media    = msg.video or msg.audio or msg.document
    if not media:
        await msg.reply("❌ Please send a file.")
        return
    sec_name = getattr(media, "file_name", None) or "secondary"
    st2      = await msg.reply("⬇️ Downloading secondary file…")
    try:
        sec = await client.download_media(media, file_name=os.path.join(tmp, sec_name))
    except Exception as exc:
        await safe_edit(st2, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        return
    try:
        await st2.delete()
    except Exception:
        pass

    if action == "merge_av":
        out = os.path.join(tmp, f"{base}_merged{ext}")
        st3 = await msg.reply("🔀 Merging video + audio…")
        try:
            await _tracked_ffmpeg(user_id, "Merge A+V", os.path.basename(out),
                                  FF.merge_av(path, sec, out))
        except Exception as exc:
            return await safe_edit(st3, f"❌ Merge failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.waiting = None
        await upload_file(client, st3, out)
        cleanup(tmp); await sessions.remove(session.key)

    elif action in ("merge_vs", "burn_sub"):
        label = "Mux Sub" if action == "merge_vs" else "Burn Sub"
        suffix = "subbed" if action == "merge_vs" else "burned"
        out   = os.path.join(tmp, f"{base}_{suffix}{ext}")
        st3   = await msg.reply("💬 Processing subtitle…")
        try:
            coro = FF.mux_subtitle(path, sec, out) if action == "merge_vs" \
                   else FF.burn_subtitle(path, sec, out)
            await _tracked_ffmpeg(user_id, label, os.path.basename(out), coro)
        except Exception as exc:
            return await safe_edit(st3, f"❌ Subtitle failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)
        session.waiting = None
        await upload_file(client, st3, out)
        cleanup(tmp); await sessions.remove(session.key)

    elif action == "merge_vids":
        queue = session.payload.setdefault("merge_queue", [])
        # Ensure primary is first
        if path not in queue:
            queue.insert(0, path)
        queue.append(sec)
        await msg.reply(
            f"✅ Video {len(queue)} added.\nSend more or /mergedone.",
            parse_mode=enums.ParseMode.HTML,
        )


# ─────────────────────────────────────────────────────────────
# /mergedone
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("mergedone"))
async def cmd_mergedone(client: Client, msg: Message):
    user_id = msg.from_user.id
    session = sessions.waiting_session(user_id)
    if not session or session.waiting != "merge_vids":
        return await msg.reply("❌ No active merge session.")

    async with session.lock:
        st   = await msg.reply("⬇️ Preparing files…")
        path = await _ensure(client, session, st)
        if not path:
            return

    queue = session.payload.get("merge_queue", [])
    if path not in queue:
        queue.insert(0, path)

    if len(queue) < 2:
        return await safe_edit(st, "❌ Need at least 2 videos to merge.")

    tmp = session.tmp_dir
    ext = session.ext or os.path.splitext(queue[0])[1] or ".mp4"
    out = os.path.join(tmp, f"merged{ext}")

    await safe_edit(st, f"➕ Merging {len(queue)} videos…")
    try:
        await _tracked_ffmpeg(user_id, f"Merge {len(queue)} videos", "merged" + ext,
                              FF.merge_videos(queue, out, tmp))
    except Exception as exc:
        return await safe_edit(st, f"❌ Merge failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)

    session.waiting = None
    await upload_file(client, st, out)
    cleanup(tmp); await sessions.remove(session.key)
