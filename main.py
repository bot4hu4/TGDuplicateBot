from alphagram import Client, filters, idle
from alphagram.types import Message
from alphagram.errors import FloodWait
import asyncio
from collections import defaultdict
from DA_Koyeb.health import emit_positive_health
import os
import sys

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    print("BOT_TOKEN missing")
    sys.exit()

app = Client(
    "DEX-DUP",
    bot_token=BOT_TOKEN,
    use_default_api=True,
    sleep_threshold=120
)

# --------- UNIQUE FILE ID DETECTOR ---------

def get_unique_id(msg: Message):
    if msg.photo: return msg.photo.file_unique_id
    if msg.video: return msg.video.file_unique_id
    if msg.document: return msg.document.file_unique_id
    if msg.audio: return msg.audio.file_unique_id
    if msg.voice: return msg.voice.file_unique_id
    if msg.animation: return msg.animation.file_unique_id
    if msg.sticker: return msg.sticker.file_unique_id
    return None


# --------- CLEAR COMMAND ---------

@app.on_message(filters.command("clear"))
async def clear_duplicate_handler(client: Client, m: Message):

    try:
        spl = m.text.split()
        if len(spl) != 4:
            return await m.reply("Usage:\n/clear chat_id start_msg end_msg")

        cid = int(spl[1])
        st  = int(spl[2])
        en  = int(spl[3])

    except:
        return await m.reply("Invalid arguments.")

    status_msg = await m.reply("📥 Fetching messages...")

    message_ids = list(range(st, en + 1))
    all_msgs = []

    # --------- FETCH MESSAGES SAFELY ---------
    for i in range(0, len(message_ids), 200):
        chunk = message_ids[i:i+200]
        try:
            msgs = await client.get_messages(cid, chunk)
            if not isinstance(msgs, list):
                msgs = [msgs]
            all_msgs.extend([x for x in msgs if x])

        except FloodWait as e:
            await asyncio.sleep(e.value)
            msgs = await client.get_messages(cid, chunk)
            if not isinstance(msgs, list):
                msgs = [msgs]
            all_msgs.extend([x for x in msgs if x])

    await status_msg.edit(f"📊 {len(all_msgs)} messages fetched.\n🔍 Checking duplicates...")

    # --------- GROUP BY TELEGRAM HASH ---------
    groups = defaultdict(list)

    for msg in all_msgs:
        uid = get_unique_id(msg)
        if uid:
            groups[uid].append(msg)

    duplicates_to_delete = []

    for uid, msgs in groups.items():
        if len(msgs) > 1:
            # keep first, delete rest
            for m2 in msgs[1:]:
                duplicates_to_delete.append(m2.id)

    if not duplicates_to_delete:
        return await status_msg.edit("✅ No duplicates found.")

    await status_msg.edit(f"🗑 Found {len(duplicates_to_delete)} duplicates.\nDeleting...")

    # --------- DELETE SAFELY ---------
    for i in range(0, len(duplicates_to_delete), 100):
        chunk = duplicates_to_delete[i:i+100]
        try:
            await client.delete_messages(cid, chunk)
            await asyncio.sleep(2)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await client.delete_messages(cid, chunk)

    await status_msg.edit(f"🎉 Done!\nDeleted {len(duplicates_to_delete)} duplicate files.")


# --------- START BOT ---------
if __name__ == "__main__":
    print("Duplicate Cleaner Bot Started")
    emit_positive_health()
    app.start()
    idle()
