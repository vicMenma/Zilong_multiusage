"""
plugins/archive.py
Extract and create archives (zip / rar / 7z / tar.gz).

FIX H-07 (audit v3): _CREATE_STATE and _EXTRACT_STATE now have TTL eviction.
Previously, if a user started /createarchive and never sent /archiveddone,
the collected files in temp dirs were never cleaned up.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import cleanup, human_size, make_tmp, safe_edit

log = logging.getLogger(__name__)

# ── State dicts ──────────────────────────────────────────────
_CREATE_STATE:  dict = {}   # uid → {"files": [...], "tmp": str, "_ts": float}
_EXTRACT_STATE: dict = {}   # key → {"path": str, "tmp": str, "_ts": float}
_ARCHIVE_STATE_TTL = 1800  # 30 min


def _evict_archive_states() -> None:
    """FIX H-07 (audit v3): clean stale archive sessions and their temp dirs."""
    now = time.time()
    for store in (_CREATE_STATE, _EXTRACT_STATE):
        dead = [k for k, v in store.items()
                if now - v.get("_ts", 0) > _ARCHIVE_STATE_TTL]
        for k in dead:
            s = store.pop(k, None)
            if s and s.get("tmp"):
                cleanup(s["tmp"])


# ─────────────────────────────────────────────────────────────
# Archive inspection
# ─────────────────────────────────────────────────────────────

def _list_archive(path: str) -> list[str]:
    ext = Path(path).suffix.lower()
    try:
        if ext == ".zip":
            import zipfile
            with zipfile.ZipFile(path) as z:
                return z.namelist()
        if ext in (".rar", ".cbr"):
            import rarfile
            with rarfile.RarFile(path) as r:
                return r.namelist()
        if ext in (".7z", ".cb7"):
            import py7zr
            with py7zr.SevenZipFile(path, mode="r") as z:
                return z.getnames()
        if ext in (".tar", ".gz", ".bz2", ".xz"):
            import tarfile
            with tarfile.open(path) as t:
                return t.getnames()
    except Exception as exc:
        log.warning("_list_archive failed for %s: %s", path, exc)
    return []


# ─────────────────────────────────────────────────────────────
# Extraction
# ─────────────────────────────────────────────────────────────

async def _extract(archive: str, out_dir: str, password: str | None = None) -> list[str]:
    ext  = Path(archive).suffix.lower()
    loop = asyncio.get_running_loop()
    os.makedirs(out_dir, exist_ok=True)

    def _zip():
        import zipfile
        with zipfile.ZipFile(archive) as z:
            if password:
                z.setpassword(password.encode())
            z.extractall(out_dir)

    def _rar():
        import rarfile
        with rarfile.RarFile(archive) as r:
            r.extractall(out_dir, pwd=password)

    def _7z():
        import py7zr
        with py7zr.SevenZipFile(archive, mode="r", password=password) as z:
            z.extractall(path=out_dir)

    def _tar():
        import tarfile
        with tarfile.open(archive) as t:
            t.extractall(out_dir)

    if ext == ".zip":
        await loop.run_in_executor(None, _zip)
    elif ext in (".rar", ".cbr"):
        await loop.run_in_executor(None, _rar)
    elif ext in (".7z", ".cb7"):
        await loop.run_in_executor(None, _7z)
    elif ext in (".tar", ".gz", ".bz2", ".xz"):
        await loop.run_in_executor(None, _tar)
    else:
        # Fallback to system 7z
        proc = await asyncio.create_subprocess_exec(
            "7z", "x", archive, f"-o{out_dir}", "-y",
            *([f"-p{password}"] if password else []),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"7z extraction failed: {err.decode()[-400:]}")

    files = []
    for root, _, fnames in os.walk(out_dir):
        for f in fnames:
            files.append(os.path.join(root, f))
    return sorted(files)


# ─────────────────────────────────────────────────────────────
# Archive creation
# ─────────────────────────────────────────────────────────────

async def _create_zip(paths: list, out: str) -> None:
    import zipfile
    loop = asyncio.get_running_loop()
    def _make():
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            for p in paths:
                z.write(p, os.path.basename(p))
    await loop.run_in_executor(None, _make)


async def _create_7z(paths: list, out: str) -> None:
    import py7zr
    loop = asyncio.get_running_loop()
    def _make():
        with py7zr.SevenZipFile(out, mode="w") as z:
            for p in paths:
                z.write(p, os.path.basename(p))
    await loop.run_in_executor(None, _make)


async def create_archive(paths: list, out: str, fmt: str = "zip") -> None:
    if fmt == "zip":
        await _create_zip(paths, out)
    elif fmt == "7z":
        await _create_7z(paths, out)
    elif fmt == "tar.gz":
        import tarfile
        loop = asyncio.get_running_loop()
        def _make():
            with tarfile.open(out, "w:gz") as t:
                for p in paths:
                    t.add(p, arcname=os.path.basename(p))
        await loop.run_in_executor(None, _make)
    else:
        raise ValueError(f"Unsupported archive format: {fmt}")


# ─────────────────────────────────────────────────────────────
# Incoming archive file (from media_router)
# ─────────────────────────────────────────────────────────────

def _arc_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 Extract All",       callback_data=f"arc|extract|{key}"),
         InlineKeyboardButton("🟡 Extract → Re-ZIP", callback_data=f"arc|rezip|{key}")],
        [InlineKeyboardButton("❌ Cancel",             callback_data=f"arc|cancel|{key}")],
    ])


async def handle_archive_file(
    client: Client, msg: Message,
    media, fname: str, fsize: int, uid: int,
) -> None:
    st  = await msg.reply("⬇️ Downloading archive…")
    tmp = make_tmp(cfg.download_dir, uid)
    try:
        path = await tg_download(
            client, media.file_id,
            os.path.join(tmp, fname), st,
            fname=fname, fsize=fsize,
        )
    except Exception as exc:
        return await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)

    loop = asyncio.get_running_loop()
    contents = await loop.run_in_executor(None, _list_archive, path)
    preview  = "\n".join(f"  {f}" for f in contents[:20])
    if len(contents) > 20:
        preview += f"\n  …and {len(contents)-20} more"

    key = f"{uid}_{media.file_id[:10]}"
    _evict_archive_states()  # FIX H-07
    _EXTRACT_STATE[key] = {"path": path, "tmp": tmp, "_ts": time.time()}
    await st.edit(
        f"<b>{fname}</b>  <code>{human_size(fsize)}</code>\n\n"
        + (f"<pre>{preview}</pre>\n\n" if preview else "")
        + "<i>Choose action:</i>",
        reply_markup=_arc_kb(key),
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^arc\|"))
async def arc_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)
    _, action, key = parts
    state = _EXTRACT_STATE.get(key)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    path = state["path"]
    tmp  = state["tmp"]

    if action == "cancel":
        cleanup(tmp)
        _EXTRACT_STATE.pop(key, None)
        return await cb.message.delete()

    await cb.message.edit("📂 Extracting…")
    out_dir = os.path.join(tmp, "extracted")
    try:
        files = await _extract(path, out_dir)
    except Exception as exc:
        return await safe_edit(cb.message, f"❌ Extraction failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)

    if not files:
        await safe_edit(cb.message, "❌ No files extracted.")
        cleanup(tmp)
        _EXTRACT_STATE.pop(key, None)
        return

    if action == "extract":
        await cb.message.edit(f"✅ {len(files)} file(s) extracted. Uploading…")
        for f in files:
            try:
                await upload_file(client, cb.message, f, force_document=True)
            except Exception as exc:
                log.warning("Failed to upload extracted file %s: %s", f, exc)
    elif action == "rezip":
        await cb.message.edit("🗜️ Re-zipping…")
        try:
            zip_out = os.path.join(tmp, "repacked.zip")
            await _create_zip(files, zip_out)
            await upload_file(client, cb.message, zip_out, force_document=True)
        except Exception as exc:
            return await safe_edit(cb.message, f"❌ Re-zip failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)

    cleanup(tmp)
    _EXTRACT_STATE.pop(key, None)


# ─────────────────────────────────────────────────────────────
# Create archive flow
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("createarchive"))
async def cmd_createarchive(client: Client, msg: Message):
    uid = msg.from_user.id
    tmp = make_tmp(cfg.download_dir, uid)
    _evict_archive_states()  # FIX H-07
    _CREATE_STATE[uid] = {"files": [], "tmp": tmp, "_ts": time.time()}
    await msg.reply(
        "📦 <b>Create Archive</b>\n\n"
        "Send files one by one.\nWhen done, send /archiveddone.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.command("archiveddone"))
async def cmd_archiveddone(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _CREATE_STATE.get(uid)
    if not state or not state["files"]:
        return await msg.reply("No files collected. Use /createarchive first.")
    await msg.reply(
        f"📦 {len(state['files'])} file(s) collected. Choose format:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 ZIP",    callback_data=f"carc|zip|{uid}"),
             InlineKeyboardButton("🟡 7Z",     callback_data=f"carc|7z|{uid}"),
             InlineKeyboardButton("🔵 TAR.GZ", callback_data=f"carc|tar.gz|{uid}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"carc|cancel|{uid}")],
        ]),
    )


@Client.on_callback_query(filters.regex(r"^carc\|"))
async def carc_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)
    _, fmt, uid_s = parts
    try:
        uid = int(uid_s)
    except ValueError:
        return await cb.answer("Invalid state.", show_alert=True)
    state = _CREATE_STATE.get(uid)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    if fmt == "cancel":
        cleanup(state["tmp"])
        _CREATE_STATE.pop(uid, None)
        return await cb.message.delete()
    out = os.path.join(state["tmp"], f"archive.{fmt}")
    await cb.message.edit(f"🗜️ Creating {fmt.upper()} archive…")
    try:
        await create_archive(state["files"], out, fmt=fmt)
    except Exception as exc:
        return await safe_edit(cb.message, f"❌ Failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)
    await upload_file(client, cb.message, out, force_document=True)
    cleanup(state["tmp"])
    _CREATE_STATE.pop(uid, None)


async def handle_archive_collect(client: Client, msg: Message, uid: int) -> None:
    """Called from media_router when uid is in _CREATE_STATE."""
    state = _CREATE_STATE.get(uid)
    if not state:
        return
    media = msg.video or msg.audio or msg.document
    if not media:
        return
    fname = getattr(media, "file_name", None) or "file"
    try:
        path = await client.download_media(
            media, file_name=os.path.join(state["tmp"], fname)
        )
    except Exception as exc:
        await msg.reply(f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        return
    state["files"].append(path)
    await msg.reply(
        f"✅ <b>{fname}</b> added. Total: {len(state['files'])}.\n"
        "Send more or /archiveddone.",
        parse_mode=enums.ParseMode.HTML,
    )
