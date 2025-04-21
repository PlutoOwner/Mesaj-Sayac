import os
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message, ChatMemberUpdated
from pymongo import MongoClient
from pyrogram.enums import ChatMemberStatus
from datetime import datetime, timedelta
from pyrogram.enums import ParseMode
from pyrogram.enums import ChatAction
import pytz
import asyncio



load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
bot_token = os.getenv("BOT_TOKEN")
session_name = "MesajSayac"
USERNAME = os.getenv("USERNAME")
DUYURU_KANAL = os.getenv("DUYURU_KANAL")
MONGO_URI = os.getenv("MONGO_URI")
LOG_KANAL = int(os.getenv("LOG_KANAL"))
OWNER_ID = int(os.getenv("OWNER_ID"))




timezone = pytz.timezone("Europe/Istanbul")

bot = Client(
    session_name,
    api_id=api_id,
    api_hash=api_hash,
    bot_token=bot_token
)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["MesajSayaç"]
message_collection = db["GrupMesajları"]
users_collection = db["kullanıcılar"]
blocked_collection = db["engel_yemişler"]   
groups_collection = db["gruplar"]


keyboard_baslangıc = InlineKeyboardMarkup([
    [
        InlineKeyboardButton("📚 Komutlar", url=f"https://telegra.ph/-04-21-1924"),
    ],
    [
        InlineKeyboardButton("🗯 Duyuru", url=f"https://t.me/{DUYURU_KANAL}"),
        InlineKeyboardButton("➕ Beni Grubuna Ekle", url=f"https://t.me/{USERNAME}?startgroup=s&admin=delete_messages"),
    ]
])



@bot.on_message(filters.command(["top", f"top@{USERNAME}", "gtop", f"gtop@{USERNAME}", "haftalik", f"haftalik@{USERNAME}", "aylik", f"aylik@{USERNAME}"]) & ~filters.private & ~filters.channel) 
async def listele(c: Client, m: Message):
    komut = m.command[0].lower()
    group_id = m.chat.id
    user_id = m.from_user.id
    user = await c.get_users(user_id)
    isim = user.first_name # Kahkül bir kültürdür✨.  @AnonimYazar

    if komut == "top":
        alan = "daily_count"
        baslik = "Günün"
    elif komut == "haftalik":
        alan = "weekly_count"
        baslik = "Haftanın"
    elif komut == "aylik":
        alan = "monthly_count"
        baslik = "Ayın"
    elif komut == "gtop":
        alan = "global_count"
        baslik = "Bütün Zamanlarda"
    else:
        return

    
    all_users = list(message_collection.find({"group_id": group_id, alan: {"$gt": 0}}).sort(alan, -1))
    
    if not all_users:
        await m.reply_text("Bu dönem için yeterli veri yok.")
        return

    top15 = all_users[:15]
    mentions = ""
    toplam_mesaj = 0
    aktif_kullanici = 0

    for user in top15:
        try:
            user_data = await c.get_users(user['user_id'])
            if user_data.is_deleted:
                continue  

            first_name = user_data.first_name.replace("[", "").replace("]", "")  
            msg_count = user[alan]
            toplam_mesaj += msg_count
            aktif_kullanici += 1
            mentions += f"[{first_name}](tg://user?id={user['user_id']}) - {msg_count}\n"
        except Exception as e:
            continue  

    if not mentions:
        await m.reply_text("Listeye uygun kullanıcı bulunamadı.")
        return

    kullanici_sirasi = next((i for i, u in enumerate(all_users) if u["user_id"] == user_id), None)
    kendi_mesaj_sayisi = next((u[alan] for u in all_users if u["user_id"] == user_id), 0)

    if kullanici_sirasi is not None:
        siralama_bilgi = f"\n📌 {isim} sen {kendi_mesaj_sayisi} mesaj ile {kullanici_sirasi + 1}. sıradasınız."
    else:
        siralama_bilgi = "\n📌 Sıralamada yer almıyorsunuz."

    mesaj = (
        f"🏆 {baslik} En Çok Mesaj Atanlar:\n\n"
        f"Grup → Mesaj\n"
        f"{mentions}\n"
        f"👥 Toplam aktif kullanıcı: {aktif_kullanici}\n"
        f"✉️ Toplam mesaj: {toplam_mesaj}\n\n"
        f"{siralama_bilgi}"
    )

    await m.reply_text(mesaj, parse_mode=ParseMode.MARKDOWN)



@bot.on_message(
    filters.command(["me", f"me@{USERNAME}"], prefixes=[".", "/"]) & filters.group
)
async def kendim_istatistik_gonder(c: Client, m: Message):
    user_id = m.from_user.id
    group_id = m.chat.id  # Varlığınızın değerli olduğu yerde kalın✿ᴗ͈ˬᴗ͈ @AnonimYazar

    try:
        await c.send_chat_action(user_id, ChatAction.TYPING)
        url = f"https://t.me/{USERNAME}?start=start_istatistik_{group_id}_{user_id}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏃🏻 🏃🏻 Özele koşşş", url=url)]
        ])

        reply_to_msg_id = m.message_id if hasattr(m, 'message_id') else None

        #await c.send_message(
           # group_id,
        await m.reply_text(
            "📬 Detayları özelden ileteceğim.",
            reply_markup=keyboard,
            reply_to_message_id=reply_to_msg_id,
            parse_mode=ParseMode.MARKDOWN
        )

    except Exception as e:
        print(f"Error: {e}")


@bot.on_message(filters.private & filters.command("start"))
async def start_mesaji(c: Client, m: Message):
    import re
    text = m.text # Zerafetin Felaketim; Erdin mi düşlerimi helak edip ? @AnonimYazar
    

    match = re.match(r"/start start_istatistik_(\-?\d+)_(\d+)", text)
    if match:
        group_id = int(match.group(1))
        user_id = int(match.group(2))
        
        users_collection.update_one(
            {"user_id": m.from_user.id},
            {
                "$set": {
                    "user_id": m.from_user.id,
                    "first_name": m.from_user.mention,
                    "username": m.from_user.username
                }
            },
            upsert=True
        )
    
        await c.send_message(LOG_KANAL, f"""
#BİLGİLERİNİ AÇTI

🤖 **Kullanıcı:** {m.from_user.mention}
📛 **Kullanıcı Adı:** @{m.from_user.username}
🆔 **Kullanıcı ID:** `{m.from_user.id}`
""")
        
        doc = message_collection.find_one({"user_id": user_id, "group_id": group_id})
        
        chat = await c.get_chat(group_id)
        grup_ismi = chat.title
        
        user = await c.get_users(user_id)
        isim = user.first_name
        kullanıcı_adı = user.username if user.username else "Kullanıcı adı yok"

        if doc:
            mesaj = f"🆔 ID: {user_id}\n"
            mesaj += f"👦🏻 İsim: {isim}\n"
            mesaj += f"🐼 Kullanıcı adı: {kullanıcı_adı}\n\n"
            mesaj += f"💬 Mesajlar:\n"
            mesaj += f"┌Günlük: {doc.get('daily_count', 0)}\n├Haftalık: {doc.get('weekly_count', 0)}\n├Aylık: {doc.get('monthly_count', 0)}\n└Tüm Zamanlar: {doc.get('global_count', 0)}\n\n"
            mesaj += f"👥 Grup Bilgisi :\n"
            mesaj += f"┌🏷 Grup başlığı: {grup_ismi}\n└🆔 ID: {group_id}"
        else:
            mesaj = "Bu grupta henüz mesaj verin yok."

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Detaylı İstatistiklerim", callback_data=f"genel_{user_id}")]
        ])
        await c.send_message(user_id, mesaj, reply_markup=keyboard)
    else:
        users_collection.update_one(
            {"user_id": m.from_user.id},
            {
                "$set": {
                    "user_id": m.from_user.id,
                    "first_name": m.from_user.mention,
                    "username": m.from_user.username
                }
            },
            upsert=True
        )
    
        await c.send_message(LOG_KANAL, f"""
#ÖZELDEN START VERDİ#

🤖 **Kullanıcı:** {m.from_user.mention}
📛 **Kullanıcı Adı:** @{m.from_user.username}
🆔 **Kullanıcı ID:** `{m.from_user.id}`
""")
        await m.reply_text(
            "Telegram gruplarınızda en aktif üyeleri takip etmek için buradayım!\n"
            "📊 Günlük, haftalık ve aylık olarak en çok mesaj atan üyeleri sıralar, grubunuzdaki etkileşimi artırmanıza yardımcı olurum.\n\n"
            "⚠️ Daha stabil çalışmam için bana boş bir yetki veya sadece mesaj silme izni vermeniz yeterli."
            "Komutlarımı bir gruba eklediğinizde kullanabilirsiniz:\n",
            reply_markup=keyboard_baslangıc,
            parse_mode=ParseMode.MARKDOWN
        )


@bot.on_callback_query()
async def istatistik_gonder(c: Client, cq: CallbackQuery):
    data = cq.data
    user_id = cq.from_user.id

    user = await c.get_users(user_id)
    isim = user.first_name
    kullanıcı_adı = user.username if user.username else "Kullanıcı adı yok"

    if data.startswith("genel_") or data.startswith("geri_"):
        user_docs = list(message_collection.find({"user_id": user_id}))

        gunluk = sum(doc.get("daily_count", 0) for doc in user_docs)
        haftalik = sum(doc.get("weekly_count", 0) for doc in user_docs)
        aylik = sum(doc.get("monthly_count", 0) for doc in user_docs)
        global_sayac = sum(doc.get("global_count", 0) for doc in user_docs)

        text = f"🆔 ID: {user_id}\n"
        text += f"👦🏻 İsim: {isim}\n"
        text += f"🐼 Kullanıcı adı: {kullanıcı_adı}\n\n"
        text += f"💬 Bulunduğun gruplarda toplam mesaj:\n"
        text += f"┌Günlük: {gunluk}\n├Haftalık: {haftalik}\n├Aylık: {aylik}\n└Tüm Zamanlar: {global_sayac}\n\n\n"
        text += f"Unutma ki bu sadece hem sen hem @{USERNAME}'un olduğu grupları gösterebilir. Sıralama da görmediğin ama ekli olduğun grup var ise, botu o gruba eklemen yeterlidir."

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Günlük", callback_data=f"gunluk_{user_id}"),
             InlineKeyboardButton("🗓️ Haftalık", callback_data=f"haftalik_{user_id}")],
            [InlineKeyboardButton("📆 Aylık", callback_data=f"aylik_{user_id}"),
             InlineKeyboardButton("📊 Bütün zamanlarda", callback_data=f"global_{user_id}")]
        ])

        await cq.edit_message_text(text, reply_markup=keyboard)
        await cq.answer()

    elif any(data.startswith(prefix) for prefix in ["gunluk_", "haftalik_", "aylik_", "global_"]):
        alan = data.split("_")[0]
        user_docs = list(message_collection.find({"user_id": user_id}))

        async def get_group_name(group_id):
            try:
                chat = await c.get_chat(group_id)
                return chat.title
            except Exception:
                return f"Grup Adı Bulunamadı: {group_id}"

        butonlar = []
        if alan != "gunluk":
            butonlar.append(InlineKeyboardButton("📅 Günlük", callback_data=f"gunluk_{user_id}"))
        if alan != "haftalik":
            butonlar.append(InlineKeyboardButton("🗓️ Haftalık", callback_data=f"haftalik_{user_id}"))
        if alan != "aylik":
            butonlar.append(InlineKeyboardButton("📆 Aylık", callback_data=f"aylik_{user_id}"))
        if alan != "global":
            butonlar.append(InlineKeyboardButton("📊 Bütün zamanlarda", callback_data=f"global_{user_id}"))

        buton_gruplari = [butonlar[i:i+2] for i in range(0, len(butonlar), 2)]
        buton_gruplari.append([InlineKeyboardButton("🔙 Geri", callback_data=f"geri_{user_id}")])

        buton = InlineKeyboardMarkup(buton_gruplari)

        if alan == "gunluk":
            text = "📅 Bugün aktif olduğun 👥 Gruplar:\n\nGrup → Mesaj\n"
            for doc in user_docs:
                if doc.get("daily_count", 0) == 0:
                    continue
                group_name = await get_group_name(doc['group_id'])
                text += f"• {group_name}: {doc['daily_count']}\n"

        elif alan == "haftalik":
            text = "🗓️ Bu hafta aktif olduğun 👥 Gruplar:\n\nGrup → Mesaj\n"
            for doc in user_docs:
                if doc.get("weekly_count", 0) == 0:
                    continue
                group_name = await get_group_name(doc['group_id'])
                text += f"• {group_name}: {doc['weekly_count']}\n"

        elif alan == "aylik":
            text = "📆 Bu ay aktif olduğun 👥 Gruplar:\n\nGrup → Mesaj\n"
            for doc in user_docs:
                if doc.get("monthly_count", 0) == 0:
                    continue
                group_name = await get_group_name(doc['group_id'])
                text += f"• {group_name}: {doc['monthly_count']}\n"

        elif alan == "global":
            text = "🌍 Bütün zamanlarda aktif olduğun 👥 Gruplar:\n\nGrup → Mesaj\n"
            for doc in user_docs:
                if doc.get("global_count", 0) == 0:
                    continue
                group_name = await get_group_name(doc['group_id'])
                text += f"• {group_name}: {doc['global_count']}\n"

        await cq.edit_message_text(text, reply_markup=buton)
        await cq.answer()









#################### owner ##############################

@bot.on_message(filters.command(["stat"]) & filters.user(OWNER_ID))
async def stat(client: Client, message: Message):
    user_count = users_collection.count_documents({})
    group_count = groups_collection.count_documents({})
    await message.reply(f"📊 **İstatistikler**\n\n👤 **Kullanıcı Sayısı:** `{user_count}`\n👥 **Grup Sayısı:** `{group_count}`")


@bot.on_message(filters.command(["duyuru"]) & filters.user(OWNER_ID))
async def duyuru(client: Client, message: Message):
    chat_id = message.chat.id
    if not message.reply_to_message:
        await message.reply("Lütfen bir mesajı yanıtlayarak komutu kullanın.")
        return

    duyuru_mesaji = message.reply_to_message
    gruplar = groups_collection.find({})
    kullanicilar = users_collection.find({})
    
    basarili_grup = 0
    basarisiz_grup = 0
    basarili_kullanici = 0
    basarisiz_kullanici = 0

    # Eğer komutta "-user" geçiyorsa kullanıcıları da dahil et
    if "-user" in message.text:
        for grup in gruplar:
            try:
                await client.forward_messages(grup["chat_id"], duyuru_mesaji.chat.id, duyuru_mesaji.id)
                basarili_grup += 1
            except Exception as e:
                basarisiz_grup += 1

        for kullanici in kullanicilar:
            try:
                await client.forward_messages(kullanici["user_id"], duyuru_mesaji.chat.id, duyuru_mesaji.id)
                basarili_kullanici += 1
            except Exception as e:
                basarisiz_kullanici += 1
                
        await client.send_message(chat_id, f"""       
**Başarılı Kullanıcılar:** `{basarili_kullanici}` - ✅
**Başarısız Kullanıcılar:** `{basarisiz_kullanici}` - ❌
        """)
    else:
        # Sadece gruplara duyuru gönder
        for grup in gruplar:
            try:
                await client.forward_messages(grup["chat_id"], duyuru_mesaji.chat.id, duyuru_mesaji.id)
                basarili_grup += 1
            except Exception as e:
                basarisiz_grup += 1

    # Duyuru gönderim sonuçları
    await client.send_message(chat_id, f""" 
**Toplam Başarılı Grup:** `{basarili_grup}` - ✅
**Toplam Başarısız Grup:** `{basarisiz_grup}` - ❌
    """)





@bot.on_message(filters.command(["gban"]) & filters.user(OWNER_ID))
async def block_group(client: Client, message: Message):
    if len(message.command) != 2:
        await message.reply_text("__Kullanım: /gban <grup_id>__")
        return

    try:
        chat_id = int(message.command[1])
    except ValueError:
        await message.reply_text("__Geçerli bir grup ID'si girin.__")
        return

    # Grubu banla
    groups_collection.update_one({"group_id": chat_id}, {"$set": {"blocked": True}}, upsert=True)
    await message.reply_text(f"__Grup `{chat_id}` banlandı.__")
    
    try:
        await client.leave_chat(chat_id)
    except Exception as e:
        await message.reply_text(f"__Grubu terk ederken hata: {str(e)}__")


@bot.on_message(filters.command(["gban"]) & filters.user(OWNER_ID))
async def block_group(client: Client, message: Message):
    if len(message.command) != 2:
        await message.reply_text("__Kullanım: /gban <grup_id>__")
        return

    try:
        chat_id = int(message.command[1])
    except ValueError:
        await message.reply_text("__Geçerli bir grup ID'si girin.__")
        return

    # Grubu banla
    groups_collection.update_one({"group_id": chat_id}, {"$set": {"blocked": True}}, upsert=True)
    await message.reply_text(f"__Grup `{chat_id}` banlandı.__")
    
    try:
        await client.leave_chat(chat_id)
    except Exception as e:
        await message.reply_text(f"__Grubu terk ederken hata: {str(e)}__")




@bot.on_message(filters.command(["ungban"]) & filters.user(OWNER_ID))
async def unblock_group(client: Client, message: Message):
    if len(message.command) != 2:
        await message.reply_text("__Kullanım: /ungban <grup_id>__")
        return

    try:
        chat_id = int(message.command[1])
    except ValueError:
        await message.reply_text("__Geçerli bir grup ID'si girin.__")
        return

    # Grubun banını kaldır
    groups_collection.update_one({"group_id": chat_id}, {"$set": {"blocked": False}}, upsert=True)
    await message.reply_text(f"__Grup `{chat_id}` banı kaldırıldı.__")


@bot.on_message(filters.new_chat_members)
async def welcome(client: Client, message: Message):
    chat_id = message.chat.id
    chat_name = message.chat.title
    ekleyen = message.from_user.first_name
    kullanı_id = message.from_user.id
    
    if message.from_user.username:
        kullanıcı_adı = f"@{message.from_user.username}"
    else:
        kullanıcı_adı = "Yok ❌"
        
    if message.chat.username:
        chatusername = f"@{message.chat.username}"
    else:
        chatusername = "ɢɪᴢʟɪ ɢʀᴜᴘ 🔏"
        
        
    for member in message.new_chat_members:
        if member.is_self:
            # Güncelleme ve log gönderme işlemleri
            groups_collection.update_one(
                {"chat_id": chat_id},
                {
                    "$set": {
                        "chat_id": chat_id,
                        "chat_name": chat_name
                    }
                },
                upsert=True
            )

            await client.send_message(LOG_KANAL, f"""
#YENİ GRUBA KATILDIM#

🤖 **Grup Adı:** {chat_name}
⚡️ **Grup Linki:** {chatusername}
🆔 **Grup ID:** `{chat_id}`
👤 **Gruba Ekleyen:** {ekleyen}
🎲 **Kullanıcı Adı:** {kullanıcı_adı}
🆔 **Kullanıcı ID:** {kullanı_id}
""")

            # Grup engelli mi kontrolü ve yanıt verme
            if groups_collection.find_one({"chat_id": chat_id, "blocked": True}):
                await client.send_message(chat_id, f"__ℹ️ Bu grup banlandı. Eğer bunun bir hata olduğunu düşünüyorsanız t.me/AnonimYazar 'e bildirin.__")
                await client.leave_chat(chat_id)
            else:
                await client.send_message(chat_id, f"__Merhaba! mesaj sayaç botunu grubunuza eklediğiniz için teşekkürler. 💫")
        


async def sifirla_ve_raporla(donem: str):
    now = datetime.now(timezone)
    
    # Dönemi belirleyelim ve bekleme süresi için gerekli ayarlamaları yapalım
    if donem == "daily":
        reset_field = "daily_count"
        period_name = "Günlük"
        gecmis_period = "Güne"
        wait_time = 0  # Günlük hemen gönderilsin
    elif donem == "weekly":
        reset_field = "weekly_count"
        period_name = "Haftalık"
        gecmis_period = "Haftaya"
        wait_time = 300  # 5 dakika
    elif donem == "monthly":
        reset_field = "monthly_count"
        period_name = "Aylık"
        gecmis_period = "Aya"
        wait_time = 300  # 5 dakika
    else:
        return

    groups = message_collection.distinct("group_id")
    for group_id in groups:
        aktif_kullanicilar = list(message_collection.find(
            {"group_id": group_id, reset_field: {"$gt": 0}},
            {"user_id": 1, reset_field: 1}
        ))

        if not aktif_kullanicilar:
            continue

        total_messages = sum(u.get(reset_field, 0) for u in aktif_kullanicilar)
        total_users = 0
        mention_lines = []

        sorted_users = sorted(aktif_kullanicilar, key=lambda u: u.get(reset_field, 0), reverse=True)
        for user in sorted_users:
            try:
                user_data = await bot.get_users(user["user_id"])
                if user_data.is_deleted:
                    continue  

                name = user_data.first_name or "Kullanıcı"
                mesaj_sayisi = user.get(reset_field, 0)

                if mesaj_sayisi > 0:
                    total_users += 1
                    mention = f"{total_users}. [{name}](tg://user?id={user['user_id']}) - {mesaj_sayisi}"
                    mention_lines.append(mention)

                if len(mention_lines) >= 15:
                    break
            except:
                continue  

        if not mention_lines:
            continue 

        message = (
            f"Grubunuzda **{period_name}** en çok aktif olan 15 kişi:\n\nKullanıcı → Mesaj\n"
            + "\n".join(mention_lines) +
            f"\n\n📊 Bu sıralama geçtiğimiz {gecmis_period} aittir.\n├👥 Toplam aktif kullanıcı: {total_users}\n💬 Toplam mesaj: {total_messages}"
        )
        
        try:
            await bot.send_message(group_id, message, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            if "CHANNEL_PRIVATE" in str(e) or "PEER_ID_INVALID" in str(e):
                print(f"💀 Mesaj gönderilemedi ({group_id}): {e}")
            else:
                print(f"⚠️ Bilinmeyen mesaj gönderim hatası ({group_id}): {e}")
            continue


        message_collection.update_many({"group_id": group_id}, {"$set": {reset_field: 0}})

        if wait_time > 0:
            await asyncio.sleep(wait_time)




@bot.on_chat_member_updated()
async def monitor_group(client: Client, chat_member_updated: ChatMemberUpdated):
    if chat_member_updated.new_chat_member and chat_member_updated.new_chat_member.user.id == client.me.id:
        chat_id = chat_member_updated.chat.id
        if groups_collection.find_one({"group_id": chat_id, "blocked": True}):
            await client.send_message(chat_id, f"__ℹ️ Bu grup banlandı. Eğer bunun bir hata olduğunu düşünüyorsanız t.me/AnonimYazar 'e bildirin.__")
            await client.leave_chat(chat_id)
            

async def zamanlayici():
    while True:
        now = datetime.now(timezone)
        if now.hour == 0 and now.minute == 0:
            await sifirla_ve_raporla("daily")
            if now.weekday() == 0:
                await sifirla_ve_raporla("weekly")
            if now.day == 1:
                await sifirla_ve_raporla("monthly")
        await asyncio.sleep(60)


async def ana_gorev():
    print("Bot başlatılıyor...")
    await bot.start()
    asyncio.create_task(zamanlayici())
    
    try:
        await bot.send_message(LOG_KANAL, "**Mesaj Sayaç Başlatıldı\n🫧 @PlutoKanal ¦ @libaryljfe ¦ @AnonimYazar**")
    except Exception as e:
        print(f"🚫 Log kanalına mesaj gönderilemedi: {e}")
        
    print("Bot başlatıldı...🫧 @PlutoKanal ¦ @libaryljfe ¦ @AnonimYazar")
    await idle()


@bot.on_message(filters.group & ~filters.service)
async def mesaj_sayisini_artir(client, message):
    if message.from_user is None or message.from_user.is_bot:
        return

    user_id = message.from_user.id
    user_name = message.from_user.first_name  
    group_id = message.chat.id

    query = {"_id": f"{user_id}_{group_id}"}
    update = {
        "$inc": {
            "daily_count": 1,
            "weekly_count": 1,
            "monthly_count": 1,
            "global_count": 1  
        },
        "$setOnInsert": {
            "user_id": user_id,
            "group_id": group_id
        }
    }

    result = message_collection.find_one_and_update(
        query, update, upsert=True, return_document=True
    )

    global_query = {"user_id": user_id}  
    global_update = {
        "$inc": {
            "global_count": 1  
        },
        "$setOnInsert": {
            "user_id": user_id
        }
    }

    global_result = message_collection.find_one_and_update(
        global_query, global_update, upsert=True, return_document=True
    )

    if result:
        yeni_gunluk_sayac = result.get("daily_count", 0) + 1
        yeni_global_sayac = global_result.get("global_count", 0)  

        if yeni_gunluk_sayac in [250, 1000, 2000]:
            await message.reply_text(
                f"🎉 Tebrikler {user_name}! Bu gün {yeni_gunluk_sayac} mesaj attınız! 👏"
            )

        if yeni_global_sayac in [1000, 5000, 10000, 20000, 50000]:
            await message.reply_text(
                f"🏆 Tebrikler {user_name}! Toplam {yeni_global_sayac} mesaj attınız! Harika bir başarı! 🎊"
            )

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ana_gorev())
    print("Bot Durdu...")
    
    
    
