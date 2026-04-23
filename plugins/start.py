"""
plugins/start.py (OPTIMIZED)
/start  /help  /settings  /info  /botname

═══════════════════════════════════════════════════════════════════
CHANGES vs original:
  • Bot name is DYNAMIC everywhere — /start header, help text, welcome
    message, settings panel, idle panel — all use get_bot_name()
  • Added /botname command (same as zilong-leech) to rename at any time
  • /start header shows "{NAME} MULTIUSAGE BOT" not "ZILONG BOT"
  • Welcome message redesigned to match zilong-leech's clean style

FIX C-02 (audit v3): Added 7 missing commands to the exclusion lists
  in prefix_suffix_collector (group=8) and af_channel_collector (group=9).
  Previously /resize, /compress, /allow, /deny, /allowed, /usage,
  /captiontemplate typed while waiting for a prefix/suffix/channel value
  were silently saved as the setting value instead of being ignored.
═══════════════════════════════════════════════════════════════════
"""
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from core.config import cfg
from core.session import users, settings
from core.bot_name import get_bot_name, set_bot_name
from services.utils import human_size


def _help_text() -> str:
    name = get_bot_name().upper()
    return (
        f"⚡ <b>{name} MULTIUSAGE BOT — Features</b>\n\n"
        "📹 <b>Video processing</b>\n"
        "› Trim · Split · Merge · Rename\n"
        "› Stream Extractor / Mapper / Remover\n"
        "› Auto + Manual Screenshots · Sample Clip\n"
        "› Convert · Optimize (CRF) · Metadata\n"
        "› Subtitle mux/burn · Audio-Video merge\n\n"
        "🎵 <b>Audio</b>\n"
        "› Extract · Remove · Convert\n"
        "› Formats: mp3 aac m4a opus ogg flac wav wma ac3\n\n"
        "🔗 <b>Downloads</b>\n"
        "› HTTP/HTTPS direct links\n"
        "› YouTube · Instagram · TikTok · Twitter and 1000+ sites\n"
        "› Google Drive · Mediafire\n"
        "› Torrents &amp; Magnet links via aria2c\n\n"
        "🔥 <b>Hardsub</b>\n"
        "› /hardsub — burn subtitles via CloudConvert\n"
        "› Supports: video file, URL, magnet + subtitle (.ass/.srt)\n"
        "› Output: MP4 with hardcoded subs, auto-uploaded\n\n"
        "📡 <b>Nyaa Tracker</b>\n"
        "› /nyaa_add — track anime on Nyaa (auto-scrape weekly)\n"
        "› /nyaa_list — show tracked anime\n"
        "› /nyaa_remove — stop tracking\n"
        "› /nyaa_check — manual check now\n"
        "› /nyaa_search — one-shot Nyaa search\n"
        "› /nyaa_dump — set dump channel for raw results\n\n"
        "📦 <b>Archives</b>\n"
        "› Extract: zip rar 7z tar.gz\n"
        "› Create: zip 7z tar.gz\n\n"
        "📨 <b>Forward</b> without forward tag\n\n"
        "⚙️ /settings · /info · /status\n"
        "📊 /status — live dashboard\n"
        "✏️ /botname — rename the bot\n"
        "📋 /log — last 50 log lines (admin)"
    )


def _start_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Help",        callback_data="cb_help"),
         InlineKeyboardButton("⚙️ Settings",    callback_data="cb_settings")],
        [InlineKeyboardButton("👤 My Account",  callback_data="cb_account")],
    ])


def _settings_header(s: dict, bot_name: str) -> str:
    """Build a readable settings state header shown above the keyboard."""
    mode     = "📄 Document" if s.get("upload_mode") == "document" else "📁 Auto"
    prefix   = s.get("prefix", "").strip()
    suffix   = s.get("suffix", "").strip()
    af       = s.get("auto_forward", False)
    chs      = s.get("forward_channels", [])
    ps       = s.get("progress_style", "B")
    caption  = s.get("caption_style", "Monospace")
    thumb    = s.get("thumb_id")
    cn_mode  = s.get("custom_name_mode", "off")
    cn_name  = s.get("custom_name", "").strip()

    af_s  = f"✅ ON ({len(chs)} ch)" if af and chs else ("✅ ON (no channels)" if af else "❌ OFF")
    ps_s  = "Cards" if ps == "B" else "Minimal"
    th_s  = "✅ Set" if thumb else "❌ None"
    cn_s  = {"off": "❌ OFF", "mid": "❓ Ask each time", "on": f"✅ ON: {cn_name or '(not set)'}"}[cn_mode]

    SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    lines = [
        SEP,
        f"⚙️  <b>{bot_name} — Settings</b>",
        SEP,
        f"📤  Upload Mode  <code>{mode}</code>",
        f"🔡  Prefix       <code>{prefix or '—'}</code>",
        f"🔤  Suffix       <code>{suffix or '—'}</code>",
        f"🖼️  Thumbnail    <code>{th_s}</code>",
        f"📡  Auto-Fwd     <code>{af_s}</code>",
        f"✏️  Caption      <code>{caption}</code>",
        f"🎨  Progress     <code>{ps_s}</code>",
        f"📝  DL Name      <code>{cn_s}</code>",
        SEP,
        "<i>Tap a button below to change a setting.</i>",
    ]
    return "\n".join(lines)


def _settings_kb(s: dict) -> InlineKeyboardMarkup:
    mode     = "📄 Document" if s.get("upload_mode") == "document" else "📁 Auto"
    prefix   = s.get("prefix", "").strip()
    suffix   = s.get("suffix", "").strip()
    af       = s.get("auto_forward", False)
    chs      = s.get("forward_channels", [])
    af_lbl   = f"📡 Auto-Fwd ✅ ({len(chs)})" if af else "📡 Auto-Fwd ❌"
    ps       = s.get("progress_style", "B")
    ps_lbl   = "🎨 Progress: Cards" if ps == "B" else "🎨 Progress: Minimal"
    cn_mode  = s.get("custom_name_mode", "off")
    cn_icons = {"off": "📝 DL Name: OFF ❌", "mid": "📝 DL Name: Ask ❓", "on": "📝 DL Name: Fixed ✅"}
    cn_lbl   = cn_icons.get(cn_mode, "📝 DL Name: OFF ❌")

    prefix_lbl = f"🔡 Prefix: {prefix[:16]}" if prefix else "🔡 Prefix: none"
    suffix_lbl = f"🔤 Suffix: {suffix[:16]}" if suffix else "🔤 Suffix: none"

    rows = [
        [InlineKeyboardButton(prefix_lbl,                callback_data="st_prefix"),
         InlineKeyboardButton("🗑",                      callback_data="st_clrprefix")],
        [InlineKeyboardButton(suffix_lbl,                callback_data="st_suffix"),
         InlineKeyboardButton("🗑",                      callback_data="st_clrsuffix")],
        [InlineKeyboardButton(f"📤 Mode: {mode}",        callback_data="st_mode")],
        [InlineKeyboardButton("🖼️ Set Thumbnail",         callback_data="st_thumb"),
         InlineKeyboardButton("🗑️ Clear Thumbnail",       callback_data="st_clearthumb")],
        [InlineKeyboardButton(af_lbl,                    callback_data="st_af_toggle"),
         InlineKeyboardButton("⚙️ Channels",              callback_data="st_af_manage")],
        [InlineKeyboardButton(f"✏️ Caption: {s.get('caption_style', 'Monospace')[:12]}", callback_data="st_caption"),
         InlineKeyboardButton(ps_lbl,                    callback_data="st_progress_style")],
        [InlineKeyboardButton(cn_lbl,                    callback_data="st_cn_cycle"),
         InlineKeyboardButton("✏️ Set Name",              callback_data="st_cn_setname")],
        [InlineKeyboardButton("❌ Close",                 callback_data="st_close")],
    ]
    return InlineKeyboardMarkup(rows)


def _back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="cb_start")],
    ])


def _welcome(user_name: str) -> str:
    """Dynamic welcome message — bot name from get_bot_name(), NOT hardcoded."""
    bot_name = get_bot_name().upper()
    return (
        f"⚡ <b>{bot_name} MULTIUSAGE BOT</b>\n"
        "──────────────────\n"
        f"👋 Hello <b>{user_name}</b>!\n"
        "🟢 Online &amp; Ready\n\n"
        "Send me a <b>link</b>, <b>video</b>, or <b>audio</b> file.\n\n"
        "📥 Download from any URL\n"
        "🧲 Torrents &amp; magnet links\n"
        "🎬 Full video toolkit\n"
        "🔥 /hardsub — CloudConvert hardsub\n"
        "📡 /nyaa_add — track anime on Nyaa\n"
        "📦 Archive management\n\n"
        "<i>Tap <b>Help</b> to see everything.</i>"
    )


@Client.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, msg: Message):
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "there"
    await users.register(uid, name)
    await msg.reply(_welcome(name), reply_markup=_start_kb(),
                    parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, msg: Message):
    await msg.reply(_help_text(), parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True)


@Client.on_message(filters.command("settings") & filters.private)
async def cmd_settings(client: Client, msg: Message):
    s = await settings.get(msg.from_user.id)
    bot_name = get_bot_name().upper()
    await msg.reply(
        _settings_header(s, bot_name),
        reply_markup=_settings_kb(s),
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("info") & filters.private)
async def cmd_info(client: Client, msg: Message):
    u    = msg.from_user
    uid  = u.id
    is_admin = uid in cfg.admins
    limit    = human_size(cfg.file_limit_b)
    bot_name = get_bot_name().upper()
    await msg.reply(
        f"👤 <b>{bot_name} — Account Info</b>\n\n"
        f"<b>ID:</b> <code>{uid}</code>\n"
        f"<b>Name:</b> {u.first_name} {u.last_name or ''}\n"
        f"<b>Username:</b> @{u.username or 'none'}\n"
        f"<b>File limit:</b> <code>{limit}</code>\n"
        f"<b>Admin:</b> {'✅' if is_admin else '❌'}",
        parse_mode=enums.ParseMode.HTML,
    )


# ── /botname — rename at any time (same as zilong-leech) ─────

_waiting_botname: set = set()

@Client.on_message(filters.command("botname") & filters.private)
async def cmd_botname(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid != cfg.owner_id:
        return
    cur = get_bot_name()
    _waiting_botname.add(uid)
    await msg.reply(
        f"✏️ <b>Rename the bot</b>\n\n"
        f"Current name: <code>{cur}</code>\n\n"
        f"Send the new name (e.g. <code>Kitagawa</code>)\n"
        f"or /cancel to abort.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(
    filters.private & filters.text
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
async def botname_collector(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _waiting_botname:
        return
    name = msg.text.strip()
    if not name or name.startswith("/"):
        return
    _waiting_botname.discard(uid)
    set_bot_name(name)
    await msg.reply(
        f"✅ <b>Name updated!</b>\n\n"
        f"New name: <b>{name.upper()} MULTIUSAGE BOT</b>\n\n"
        f"<i>The change is immediate — /start will show the new name.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()


# ── Callback handlers ─────────────────────────────────────────

@Client.on_callback_query(filters.regex("^cb_start$"))
async def cq_start(client: Client, cb: CallbackQuery):
    name = cb.from_user.first_name or "there"
    await cb.message.edit(_welcome(name), reply_markup=_start_kb(),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_help$"))
async def cq_help(client: Client, cb: CallbackQuery):
    await cb.message.edit(_help_text(), parse_mode=enums.ParseMode.HTML,
                          reply_markup=_back_kb())
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_settings$"))
async def cq_settings(client: Client, cb: CallbackQuery):
    s = await settings.get(cb.from_user.id)
    bot_name = get_bot_name().upper()
    await cb.message.edit(
        _settings_header(s, bot_name),
        reply_markup=_settings_kb(s),
        parse_mode=enums.ParseMode.HTML,
    )
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_account$"))
async def cq_account(client: Client, cb: CallbackQuery):
    u      = cb.from_user
    uid    = u.id
    limit  = human_size(cfg.file_limit_b)
    bot_name = get_bot_name().upper()
    await cb.message.edit(
        f"👤 <b>{bot_name} — My Account</b>\n\n"
        f"<b>Name:</b> {u.first_name} {u.last_name or ''}\n"
        f"<b>ID:</b> <code>{uid}</code>\n"
        f"<b>Username:</b> @{u.username or 'none'}\n"
        f"<b>File limit:</b> <code>{limit}</code>\n"
        f"<b>Admin:</b> {'✅' if uid in cfg.admins else '❌'}",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_back_kb(),
    )
    await cb.answer()


@Client.on_callback_query(filters.regex("^st_mode$"))
async def cq_st_mode(client: Client, cb: CallbackQuery):
    s   = await settings.get(cb.from_user.id)
    new = "document" if s.get("upload_mode") != "document" else "auto"
    await settings.update(cb.from_user.id, {"upload_mode": new})
    s["upload_mode"] = new
    bot_name = get_bot_name().upper()
    await cb.message.edit(_settings_header(s, bot_name), reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer(f"Mode: {new} ✅")


@Client.on_callback_query(filters.regex("^st_progress_style$"))
async def cq_st_progress_style(client: Client, cb: CallbackQuery):
    s       = await settings.get(cb.from_user.id)
    current = s.get("progress_style", "B")
    new     = "C" if current == "B" else "B"
    await settings.update(cb.from_user.id, {"progress_style": new})
    s["progress_style"] = new
    bot_name = get_bot_name().upper()
    await cb.message.edit(_settings_header(s, bot_name), reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    label = "Cards" if new == "B" else "Minimal"
    await cb.answer(f"Progress style: {label} ✅")


@Client.on_callback_query(filters.regex("^st_thumb$"))
async def cq_st_thumb(client: Client, cb: CallbackQuery):
    await cb.message.edit(
        "🖼️ <b>Set Thumbnail</b>\n\nSend a photo — it will be used for all uploads.",
        parse_mode=enums.ParseMode.HTML,
    )
    await cb.answer()


@Client.on_callback_query(filters.regex("^st_clearthumb$"))
async def cq_st_clear(client: Client, cb: CallbackQuery):
    await settings.update(cb.from_user.id, {"thumb_id": None})
    await cb.answer("Thumbnail cleared ✅", show_alert=True)


_CAPTION_STYLES = ["Monospace", "Bold", "Italic", "Plain", "Bold Italic"]

def _caption_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("𝙼𝚘𝚗𝚘𝚜𝚙𝚊𝚌𝚎",  callback_data="st_cap|Monospace"),
         InlineKeyboardButton("Bold",       callback_data="st_cap|Bold")],
        [InlineKeyboardButton("Italic",     callback_data="st_cap|Italic"),
         InlineKeyboardButton("Plain",      callback_data="st_cap|Plain")],
        [InlineKeyboardButton("Bold Italic",callback_data="st_cap|Bold Italic"),
         InlineKeyboardButton("🔙 Back",    callback_data="cb_settings")],
    ]
    return InlineKeyboardMarkup(rows)


@Client.on_callback_query(filters.regex("^st_caption$"))
async def cq_st_caption(client: Client, cb: CallbackQuery):
    s = await settings.get(cb.from_user.id)
    cur = s.get("caption_style", "Monospace")
    await cb.message.edit(
        f"✏️ <b>Caption Style</b>\n\n"
        f"Current: <b>{cur}</b>\n\n"
        f"Monospace → <code>filename.mkv</code>\n"
        f"Bold      → <b>filename.mkv</b>\n"
        f"Italic    → <i>filename.mkv</i>\n"
        f"Plain     → filename.mkv\n"
        f"Bold Italic → <b><i>filename.mkv</i></b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_caption_kb(),
    )
    await cb.answer()


@Client.on_callback_query(filters.regex(r"^st_cap\|"))
async def cq_st_cap_pick(client: Client, cb: CallbackQuery):
    style = cb.data.split("|", 1)[1]
    await settings.update(cb.from_user.id, {"caption_style": style})
    s = await settings.get(cb.from_user.id)
    bot_name = get_bot_name().upper()
    await cb.message.edit(
        _settings_header(s, bot_name),
        reply_markup=_settings_kb(s),
        parse_mode=enums.ParseMode.HTML,
    )
    await cb.answer(f"Caption style: {style} ✅")


@Client.on_callback_query(filters.regex("^st_close$"))
async def cq_st_close(client: Client, cb: CallbackQuery):
    await cb.message.delete()
    await cb.answer()


# ── Custom Download Name handlers ─────────────────────────────
_CN_NAME_WAITING: set[int] = set()

_CN_CYCLE = {"off": "mid", "mid": "on", "on": "off"}
_CN_LABELS = {
    "off": "❌ OFF — original filename kept",
    "mid": "❓ Ask — you'll be prompted each time",
    "on":  "✅ Fixed — custom name applied automatically",
}


@Client.on_callback_query(filters.regex("^st_cn_cycle$"))
async def cq_cn_cycle(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    s   = await settings.get(uid)
    new = _CN_CYCLE.get(s.get("custom_name_mode", "off"), "mid")
    await settings.update(uid, {"custom_name_mode": new})
    s["custom_name_mode"] = new
    bot_name = get_bot_name().upper()
    await cb.message.edit(
        _settings_header(s, bot_name),
        reply_markup=_settings_kb(s),
        parse_mode=enums.ParseMode.HTML,
    )
    await cb.answer(f"Download Name mode: {_CN_LABELS[new]}", show_alert=True)


@Client.on_callback_query(filters.regex("^st_cn_setname$"))
async def cq_cn_setname(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    s   = await settings.get(uid)
    cur = s.get("custom_name", "").strip()
    _CN_NAME_WAITING.add(uid)
    await cb.answer()
    await cb.message.edit(
        "📝 <b>Set Fixed Download Name</b>\n\n"
        f"Current: <code>{cur or '(none)'}</code>\n\n"
        "Send the <b>base name</b> (without extension) to apply to every download "
        "when mode is <b>Fixed ✅</b>.\n\n"
        "Example: <code>My Anime S01E01</code>\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start","help","settings","info","status","log","restart","broadcast",
         "admin","ban_user","unban_user","banned_list","cancel",
         "show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","botname","ccstatus","convert",
         "resize","compress","captiontemplate","usage","allow","deny","allowed",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=11,
)
async def cn_name_collector(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _CN_NAME_WAITING:
        return
    text = msg.text.strip()
    if text.lower() in ("/cancel", "cancel"):
        _CN_NAME_WAITING.discard(uid)
        await msg.reply("❌ Cancelled.")
        msg.stop_propagation()
        return
    if text.startswith("/"):
        return
    _CN_NAME_WAITING.discard(uid)
    # Sanitise: strip extension if user accidentally included it
    import os as _os
    base, _ext = _os.path.splitext(text)
    name_to_save = base.strip() if _ext else text
    await settings.update(uid, {"custom_name": name_to_save})
    s_new = await settings.get(uid)
    bot_name = get_bot_name().upper()
    await msg.reply(
        f"✅ <b>Fixed download name saved!</b>\n\n"
        f"Name: <code>{name_to_save}</code>\n\n"
        f"<i>Switch mode to <b>Fixed ✅</b> in settings for it to apply.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    await msg.reply(
        _settings_header(s_new, bot_name),
        reply_markup=_settings_kb(s_new),
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()


# ── Prefix / Suffix handlers ──────────────────────────────────
_PREFIX_WAITING: set[int] = set()
_SUFFIX_WAITING: set[int] = set()


@Client.on_callback_query(filters.regex("^st_prefix$"))
async def cq_st_prefix(client: Client, cb: CallbackQuery):
    _PREFIX_WAITING.add(cb.from_user.id)
    _SUFFIX_WAITING.discard(cb.from_user.id)
    await cb.answer()
    await cb.message.edit(
        "🔡 <b>Set Prefix</b>\n\n"
        "Reply with the text to <b>prepend</b> before every filename.\n\n"
        "Example: <code>[VOSTFR] </code> → <code>[VOSTFR] Oshi no Ko S03E10.mkv</code>\n\n"
        "<i>Send /cancel to cancel.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex("^st_suffix$"))
async def cq_st_suffix(client: Client, cb: CallbackQuery):
    _SUFFIX_WAITING.add(cb.from_user.id)
    _PREFIX_WAITING.discard(cb.from_user.id)
    await cb.answer()
    await cb.message.edit(
        "🔤 <b>Set Suffix</b>\n\n"
        "Reply with the text to <b>append</b> after the filename (before extension).\n\n"
        "Example: <code> [FR]</code> → <code>Oshi no Ko S03E10 [FR].mkv</code>\n\n"
        "<i>Send /cancel to cancel.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex("^st_clrprefix$"))
async def cq_st_clrprefix(client: Client, cb: CallbackQuery):
    await settings.update(cb.from_user.id, {"prefix": ""})
    _PREFIX_WAITING.discard(cb.from_user.id)
    s = await settings.get(cb.from_user.id)
    bot_name = get_bot_name().upper()
    await cb.message.edit(_settings_header(s, bot_name), reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer("Prefix cleared ✅")


@Client.on_callback_query(filters.regex("^st_clrsuffix$"))
async def cq_st_clrsuffix(client: Client, cb: CallbackQuery):
    await settings.update(cb.from_user.id, {"suffix": ""})
    _SUFFIX_WAITING.discard(cb.from_user.id)
    s = await settings.get(cb.from_user.id)
    bot_name = get_bot_name().upper()
    await cb.message.edit(_settings_header(s, bot_name), reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer("Suffix cleared ✅")


@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start","help","settings","info","status","log","restart",
         "broadcast","admin","ban_user","unban_user","banned_list",
         "cancel","show_thumb","del_thumb","json_formatter","bulk_url",
         "hardsub","botname","ccstatus","convert",
         # FIX C-02 (audit v3): added missing commands so /resize etc. typed while
         # waiting for a prefix value are not saved as the prefix string.
         "resize","compress","captiontemplate","usage","allow","deny","allowed",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=8,
)
async def prefix_suffix_collector(client: Client, msg: Message):
    uid = msg.from_user.id
    waiting_prefix = uid in _PREFIX_WAITING
    waiting_suffix = uid in _SUFFIX_WAITING
    if not waiting_prefix and not waiting_suffix:
        return
    text = msg.text.strip()
    if text.lower() in ("/cancel", "cancel"):
        _PREFIX_WAITING.discard(uid)
        _SUFFIX_WAITING.discard(uid)
        await msg.reply("❌ Cancelled.")
        msg.stop_propagation()
        return

    if waiting_prefix:
        _PREFIX_WAITING.discard(uid)
        await settings.update(uid, {"prefix": text})
        await msg.reply(
            f"✅ <b>Prefix saved!</b>\n\n"
            f"Files will be named: <code>{text}Oshi no Ko S03E10.mkv</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        _SUFFIX_WAITING.discard(uid)
        await settings.update(uid, {"suffix": text})
        await msg.reply(
            f"✅ <b>Suffix saved!</b>\n\n"
            f"Files will be named: <code>Oshi no Ko S03E10{text}.mkv</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    # Re-send settings panel so user can see the updated state immediately
    s_new = await settings.get(uid)
    bot_name = get_bot_name().upper()
    await msg.reply(
        _settings_header(s_new, bot_name),
        reply_markup=_settings_kb(s_new),
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()


# ── Auto-Forward handlers (unchanged logic, just uses dynamic bot name) ──────
_AF_ADD_WAITING: set[int] = set()


def _af_manage_kb(channels: list) -> InlineKeyboardMarkup:
    rows = []
    for i, ch in enumerate(channels):
        name = ch.get("name", str(ch["id"]))[:28]
        rows.append([
            InlineKeyboardButton(f"📢 {name}", callback_data=f"af_info|{i}"),
            InlineKeyboardButton("🗑",         callback_data=f"af_del|{i}"),
        ])
    rows.append([
        InlineKeyboardButton("➕ Add channel",       callback_data="af_add"),
        InlineKeyboardButton("🔙 Back to Settings",  callback_data="cb_settings"),
    ])
    return InlineKeyboardMarkup(rows)


@Client.on_callback_query(filters.regex("^st_af_toggle$"))
async def cq_af_toggle(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    s   = await settings.get(uid)
    chs = s.get("forward_channels", [])
    if not chs and not s.get("auto_forward"):
        await cb.answer("⚠️ Add at least one channel first via ⚙️ Channels", show_alert=True)
        return
    new = not s.get("auto_forward", False)
    await settings.update(uid, {"auto_forward": new})
    s["auto_forward"] = new
    bot_name = get_bot_name().upper()
    await cb.message.edit(_settings_header(s, bot_name), reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer(f"Auto-Forward {'✅ ON' if new else '❌ OFF'}")


@Client.on_callback_query(filters.regex("^st_af_manage$"))
async def cq_af_manage(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    s   = await settings.get(uid)
    chs = s.get("forward_channels", [])
    count = len(chs)
    text = (
        f"📡 <b>Forward Channels</b>  ({count} saved)\n\n"
        + ("\n".join(f"  {i+1}. <code>{ch.get('name', ch['id'])}</code>" for i, ch in enumerate(chs)) if chs
           else "  <i>No channels yet.</i>")
        + "\n\n<i>Add a channel by tapping ➕. The bot must be an admin of that channel.</i>"
    )
    await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=_af_manage_kb(chs))
    await cb.answer()


@Client.on_callback_query(filters.regex("^af_add$"))
async def cq_af_add(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    _AF_ADD_WAITING.add(uid)
    await cb.answer()
    await cb.message.edit(
        "➕ <b>Add Forward Channel</b>\n\n"
        "Send the channel <b>username</b> or <b>numeric ID</b>:\n\n"
        "Examples:\n  <code>@mychannel</code>\n  <code>-1001234567890</code>\n\n"
        "<i>The bot must be an admin of that channel.\nSend /cancel to cancel.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^af_del\|(\d+)$"))
async def cq_af_del(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    idx = int(cb.data.split("|")[1])
    s   = await settings.get(uid)
    chs = list(s.get("forward_channels", []))
    if 0 <= idx < len(chs):
        removed = chs.pop(idx)
        await settings.update(uid, {"forward_channels": chs})
        await cb.answer(f"Removed: {removed.get('name', removed['id'])}")
    else:
        await cb.answer("Not found")
    s["forward_channels"] = chs
    count = len(chs)
    text = (
        f"📡 <b>Forward Channels</b>  ({count} saved)\n\n"
        + ("\n".join(f"  {i+1}. <code>{ch.get('name', ch['id'])}</code>" for i, ch in enumerate(chs)) if chs
           else "  <i>No channels yet.</i>")
        + "\n\n<i>Add a channel by tapping ➕.</i>"
    )
    await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=_af_manage_kb(chs))


@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start","help","settings","info","status","log","restart","broadcast",
         "admin","ban_user","unban_user","banned_list","cancel",
         "show_thumb","del_thumb","json_formatter","bulk_url","hardsub",
         "botname","ccstatus","convert",
         # FIX C-02 (audit v3)
         "resize","compress","captiontemplate","usage","allow","deny","allowed",
         "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
         "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit"]
    ),
    group=9,
)
async def af_channel_collector(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _AF_ADD_WAITING:
        return
    text = msg.text.strip()
    if text.lower() in ("/cancel", "cancel"):
        _AF_ADD_WAITING.discard(uid)
        await msg.reply("❌ Cancelled.")
        msg.stop_propagation()
        return
    try:
        if text.lstrip("-").isdigit():
            target = int(text)
        else:
            target = text if text.startswith("@") else f"@{text}"
        chat = await client.get_chat(target)
        ch_id   = chat.id
        ch_name = chat.title or chat.username or str(ch_id)
        s   = await settings.get(uid)
        chs = list(s.get("forward_channels", []))
        if any(c["id"] == ch_id for c in chs):
            await msg.reply(f"⚠️ <b>{ch_name}</b> is already in your list.", parse_mode=enums.ParseMode.HTML)
            _AF_ADD_WAITING.discard(uid)
            msg.stop_propagation()
            return
        chs.append({"id": ch_id, "name": ch_name})
        await settings.update(uid, {"forward_channels": chs})
        _AF_ADD_WAITING.discard(uid)
        await msg.reply(
            f"✅ <b>{ch_name}</b> added!\nTotal: <b>{len(chs)}</b> channel(s)",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as e:
        await msg.reply(
            f"❌ Could not resolve channel: <code>{e}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    msg.stop_propagation()


@Client.on_callback_query(filters.regex(r"^fwd\|"))
async def cq_forward(client: Client, cb: CallbackQuery):
    parts   = cb.data.split("|")
    action  = parts[1]
    src_cid = int(parts[2])
    msg_id  = int(parts[3])
    dest_id = int(parts[4]) if parts[4] != "0" else None

    if action == "skip":
        await cb.message.delete()
        return await cb.answer("Skipped ✖")

    uid = cb.from_user.id
    s   = await settings.get(uid)
    chs = s.get("forward_channels", [])
    targets = [ch for ch in chs if ch["id"] == dest_id] if action == "one" else chs

    ok, fail = 0, []
    for ch in targets:
        try:
            await client.copy_message(chat_id=ch["id"], from_chat_id=src_cid, message_id=msg_id)
            ok += 1
        except Exception:
            fail.append(ch.get("name", str(ch["id"])))

    result = f"✅ Forwarded to {ok} channel{'s' if ok != 1 else ''}."
    if fail:
        result += f"\n⚠️ Failed: {', '.join(fail)}"
    await cb.message.edit(result)
    await cb.answer()
