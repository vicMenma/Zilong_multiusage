"""
plugins/archive.py
Extract and create archives (zip / rar / 7z / tar.gz).
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import cleanup, human_size, make_tmp, safe_edit

log = logging.getLogger(__name__)

# ── State: {uid: {"files": [...], "tmp": str}} ───────────────
_CREATE_STATE: dict = {}
_EXTRACT_STATE: dict = {}


# ─────────────────────────────────────────────────────────────
# Extraction
# ─────────────────────────────────────────────────────────────

def _list_archive(path: str) -> list[str]:
    ext = Path(path).suffix.lower()
    try:
        if ext == ".zip":
            import zipfile
            with zipfile.ZipFile(path) as z: return z.namelist()
        if ext in (".rar",".cbr"):
            import rarfile
            with rarfile.RarFile(path) as r: return r.namelist()
        if ext in (".7z",".cb7"):
            import py7zr
            with py7zr.SevenZipFile(path, mode="r") as z: return z.getnames()
    except Exception:
        pass
    return []


async def _extract(archive: str, out_dir: str, password: str | None = None) -> list[str]:
    ext  = Path(archive).suffix.lower()
    loop = asyncio.get_event_loop()
    os.makedirs(out_dir, exist_ok=True)

    def _zip():
        import zipfile
        with zipfile.ZipFile(archive) as z:
            if password: z.setpassword(password.encode())
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

    if ext == ".zip":                          await loop.run_in_executor(None, _zip)
    elif ext in (".rar",".cbr"):               await loop.run_in_executor(None, _rar)
    elif ext in (".7z",".cb7"):                await loop.run_in_executor(None, _7z)
    elif ext in (".tar",".gz",".bz2",".xz"):   await loop.run_in_executor(None, _tar)
    else:
        proc = await asyncio.create_subprocess_exec(
            "7z","x",archive,f"-o{out_dir}","-y",
            *([ f"-p{password}"] if password else []),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
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
# Creation
# ─────────────────────────────────────────────────────────────

async def _create_zip(paths: list, out: str):
    import zipfile
    loop = asyncio.get_event_loop()
    def _make():
        with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
            for p in paths: z.write(p, os.path.basename(p))
    await loop.run_in_executor(None, _make)


async def _create_7z(paths: list, out: str):
    import py7zr
    loop = asyncio.get_event_loop()
    def _make():
        with py7zr.SevenZipFile(out, mode="w") as z:
            for p in paths: z.write(p, os.path.basename(p))
    await loop.run_in_executor(None, _make)


async def create_archive(paths: list, out: str, fmt: str = "zip"):
    if fmt == "zip":
        await _create_zip(paths, out)
    elif fmt == "7z":
        await _create_7z(paths, out)
    elif fmt == "tar.gz":
        import tarfile
        loop = asyncio.get_event_loop()
        def _make():
            with tarfile.open(out,"w:gz") as t:
                for p in paths: t.add(p, arcname=os.path.basename(p))
        await loop.run_in_executor(None, _make)
    else:
        raise ValueError(f"Unsupported archive format: {fmt}")


# ─────────────────────────────────────────────────────────────
# Archive file received (from media_router)
# ─────────────────────────────────────────────────────────────

def _arc_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📂 Extract All",       callback_data=f"arc|extract|{key}"),
         InlineKeyboardButton("📦 Extract → Re-ZIP", callback_data=f"arc|rezip|{key}")],
        [InlineKeyboardButton("❌ Cancel",             callback_data=f"arc|cancel|{key}")],
    ])


async def handle_archive_file(client: Client, msg: Message, media, fname: str, fsize: int, uid: int):
    st  = await msg.reply("⬇️ Downloading archive…")
    tmp = make_tmp(cfg.download_dir, uid)
    try:
        path = await tg_download(client, media.file_id,
                                 os.path.join(tmp, fname), st, fname=fname, fsize=fsize)
    except Exception as exc:
        return await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)

    contents = await asyncio.get_event_loop().run_in_executor(None, _list_archive, path)
    preview  = "\n".join(f"  {f}" for f in contents[:20])
    if len(contents) > 20:
        preview += f"\n  …and {len(contents)-20} more"

    key = f"{uid}_{media.file_id[:10]}"
    _EXTRACT_STATE[key] = {"path": path, "tmp": tmp}
    await st.edit(
        f"<b>{fname}</b>  <code>{human_size(fsize)}</code>\n\n"
        f"<pre>{preview}</pre>\n\n<i>Choose action:</i>",
        reply_markup=_arc_kb(key),
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^arc\|"))
async def arc_cb(client: Client, cb: CallbackQuery):
    _, action, key = cb.data.split("|", 2)
    state = _EXTRACT_STATE.get(key)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    path = state["path"]; tmp = state["tmp"]

    if action == "cancel":
        cleanup(tmp); _EXTRACT_STATE.pop(key, None)
        return await cb.message.delete()

    await cb.message.edit("📂 Extracting…")
    out_dir = os.path.join(tmp, "extracted")
    try:
        files = await _extract(path, out_dir)
    except Exception as exc:
        return await safe_edit(cb.message, f"❌ Extraction failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)

    if action == "extract":
        await cb.message.edit(f"✅ {len(files)} file(s) extracted. Uploading…")
        for f in files:
            await upload_file(client, cb.message, f)
    elif action == "rezip":
        await cb.message.edit("🗜️ Re-zipping…")
        try:
            zip_out = os.path.join(tmp, "repacked.zip")
            await _create_zip(files, zip_out)
            await upload_file(client, cb.message, zip_out, force_document=True)
        except Exception as exc:
            return await safe_edit(cb.message, f"❌ Re-zip failed: <code>{exc}</code>",
                                   parse_mode=enums.ParseMode.HTML)

    cleanup(tmp); _EXTRACT_STATE.pop(key, None)


# ─────────────────────────────────────────────────────────────
# Create archive flow
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("createarchive"))
async def cmd_createarchive(client: Client, msg: Message):
    uid = msg.from_user.id
    tmp = make_tmp(cfg.download_dir, uid)
    _CREATE_STATE[uid] = {"files": [], "tmp": tmp}
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
            [InlineKeyboardButton("🗜 ZIP",    callback_data=f"carc|zip|{uid}"),
             InlineKeyboardButton("🗜 7Z",     callback_data=f"carc|7z|{uid}"),
             InlineKeyboardButton("🗜 TAR.GZ", callback_data=f"carc|tar.gz|{uid}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"carc|cancel|{uid}")],
        ]),
    )


@Client.on_callback_query(filters.regex(r"^carc\|"))
async def carc_cb(client: Client, cb: CallbackQuery):
    _, fmt, uid_s = cb.data.split("|", 2)
    try:
        uid = int(uid_s)
    except ValueError:
        return await cb.answer("Invalid state.", show_alert=True)
    state = _CREATE_STATE.get(uid)
    if not state:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()
    if fmt == "cancel":
        cleanup(state["tmp"]); _CREATE_STATE.pop(uid, None)
        return await cb.message.delete()
    out = os.path.join(state["tmp"], f"archive.{fmt}")
    await cb.message.edit(f"🗜️ Creating {fmt.upper()} archive…")
    try:
        await create_archive(state["files"], out, fmt=fmt)
    except Exception as exc:
        return await safe_edit(cb.message, f"❌ Failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)
    await upload_file(client, cb.message, out, force_document=True)
    cleanup(state["tmp"]); _CREATE_STATE.pop(uid, None)


# Collect files for archive creation (called from media_router)
async def handle_archive_collect(client: Client, msg: Message, uid: int):
    state = _CREATE_STATE.get(uid)
    if not state:
        return
    media = msg.video or msg.audio or msg.document
    if not media:
        return
    fname = getattr(media, "file_name", None) or "file"
    try:
        path = await client.download_media(media, file_name=os.path.join(state["tmp"], fname))
    except Exception as exc:
        await msg.reply(f"❌ Download failed: <code>{exc}</code>", parse_mode=enums.ParseMode.HTML)
        return
    state["files"].append(path)
    await msg.reply(
        f"✅ <b>{fname}</b> added. Total: {len(state['files'])}.\n"
        "Send more or /archiveddone.",
        parse_mode=enums.ParseMode.HTML,
    )
