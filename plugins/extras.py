"""
plugins/extras.py
Miscellaneous commands: /show_thumb /del_thumb /json_formatter /bulk_url
"""
import json

from pyrogram import Client, filters, enums
from pyrogram.types import Message

from core.session import settings


@Client.on_message(filters.private & filters.command("show_thumb"))
async def cmd_show_thumb(client: Client, msg: Message):
    s     = await settings.get(msg.from_user.id)
    thumb = s.get("thumb_id")
    if thumb:
        await client.send_photo(msg.chat.id, thumb, caption="🖼️ Your saved thumbnail")
    else:
        await msg.reply("❌ No thumbnail saved.\nSend any photo to save it.")


@Client.on_message(filters.private & filters.command("del_thumb"))
async def cmd_del_thumb(client: Client, msg: Message):
    await settings.update(msg.from_user.id, {"thumb_id": None})
    await msg.reply("✅ Thumbnail deleted.")


@Client.on_message(filters.private & filters.command("json_formatter"))
async def cmd_json(client: Client, msg: Message):
    text = " ".join(msg.command[1:]) or (
        msg.reply_to_message.text if msg.reply_to_message else "")
    if not text:
        return await msg.reply(
            'Usage: <code>/json_formatter {"key":"value"}</code>',
            parse_mode=enums.ParseMode.HTML)
    try:
        parsed = json.loads(text)
        await msg.reply(
            f"<pre>{json.dumps(parsed, indent=2, ensure_ascii=False)[:4000]}</pre>",
            parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        await msg.reply(f"❌ Invalid JSON: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.private & filters.command("bulk_url"))
async def cmd_bulk_url(client: Client, msg: Message):
    await msg.reply(
        "📥 <b>Bulk Download</b>\n\n"
        "Send all links in a single message, one per line.\n"
        "<i>I'll process them one by one.</i>",
        parse_mode=enums.ParseMode.HTML)
