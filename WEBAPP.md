# Create Quiz — Telegram Mini App
## Loyiha nomi va logo

Ilovadagi loyiha nomi **Create Quiz**. JPG logongizni `webapp/assets/` papkasiga **logo.jpg** nomi bilan qo‘ying. Ilova tepasidagi logo va brauzer belgisi shu fayldan olinadi. Kvadrat rasm tavsiya etiladi. `logo.jpg` hali qo‘yilmagan bo‘lsa, ilova tepasida vaqtinchalik **CQ** belgisi ko‘rinadi.

Rasmni shunchaki `.svg` dan `.jpg` ga qayta nomlamang — haqiqiy JPG faylni joylashtiring. Fayllarni hostingga yangilab, web ilovani qayta oching.

## Bo‘limlar

Botning web ilovasida quyidagi bo‘limlar mavjud:

1. **Fayldan test** — PDF, DOCX, PPTX, XLSX, TXT, MD, JSON va rasmlardan savollar olish. Tayyor testlarni AI ishlatmasdan bepul import qilish mumkin.
2. **AI test** — mavzu, savollar soni, qiyinlik, vaqt va test tilini tanlash.
3. **Yangi test** — savol, to‘rtta javob, to‘g‘ri javob va izohni qo‘lda kiritish. Qoralama shu qurilmada saqlanadi.
4. **Interaktiv testlar** — ta’lim, sevgi, do‘stlik va boshqa yo‘nalishlarda birgalikda test ishlash, yoqtirish foizlari va faol mualliflar reytingi. Qoidalar: [INTERACTIVE.md](INTERACTIVE.md).

Pastki menyuda shu to‘rt bo‘lim va **Profil** bor. Profil ichida Premium tariflari, referral bonuslar, mavjud limitlar, saqlangan testlar va interfeys tili joylashgan. Interfeys hozir o‘zbek, rus va ingliz tillarida; AI test natijalari botdagi 9 tilni qo‘llaydi. Telegramning yorug‘ va qorong‘i mavzulariga moslashadi.

**Profil → E’lon qilgan testlarim** ochiq va maxfiy interaktiv testlaringizni ko‘rsatadi. Bu yerdan xona boshlash, o‘zingiz ochgan faol xonaga qaytish yoki testni olib tashlash mumkin. Olib tashlash faqat muallifga ruxsat etiladi va shu testning barcha faol xonalarini to‘xtatadi; botdagi asl saqlangan test o‘chmaydi.

E’lon qilishda **Barchaga ochiq** yoki **Maxfiy** tanlanadi. Maxfiy test va xonalari umumiy ro‘yxat yoki reytingga chiqmaydi. Muallif xona ochib, **Do‘stlarni taklif qilish** yoki **Xona havolasini olish** orqali ulashadi; havolani olgan odam Telegram orqali kirishi mumkin.

**Profil → Profilni tahrirlash** orqali ism, ilovadagi username va rasmni o‘zgartirish mumkin. Username 3–32 ta lotin harfi, raqam yoki `_` dan iborat bo‘lib, harf bilan boshlanadi; katta-kichik harf farqlanmaydi va boshqa foydalanuvchida band bo‘lsa saqlanmaydi. Rasm 5 MB gacha, serverda kichraytirilib PNG sifatida saqlanadi. O‘zgarishlar «O‘zgarishlarni saqlash» bosilganda yoziladi. Telegram akkauntining username va rasmi alohida; botga qayta kirish ilovadagi profilni almashtirib yubormaydi. Rasmlarga autentifikatsiyasiz yoki boshqa foydalanuvchi sifatida kirib bo‘lmaydi. Hostingda `media/` uchun persistent disk kerak.

Oylik admin statistikasi va ommaviy xabar yuborish faqat Telegram botda mavjud: [ADMIN.md](ADMIN.md).

Testlar mavjud bazaga saqlanadi va botdagi «Mening testlarim» bo‘limida ham chiqadi. Tayyor testni ko‘rib chiqish web ilovada, uni Telegram quiz sifatida o‘tkazish esa «Telegramda boshlash» orqali botda bajariladi. Premium to‘lovi mavjud tarif tanlash, chek yuborish va administrator tasdiqlashi jarayonidan foydalanadi; bu qadamlar botda davom etadi. Kanal bonusi mavjud botda o‘chirilgan, shu sabab web ilovada referral bonusi ko‘rsatiladi.

## Lokal ko‘rish

Bot, token yoki AI so‘rovisiz dizaynni ko‘rish:

```powershell
.venv/Scripts/python.exe scripts/preview_webapp.py
```

Brauzerda `http://127.0.0.1:8081/app` ni oching. Bu ko‘rish rejimi; testni bazaga saqlash va shaxsiy profil uchun Telegramdan kirish kerak.

## Telegramda ishlatish

1. Loyihani **Web Service** sifatida HTTPS bilan joylashtiring. Render uchun build: `pip install -r requirements.txt`, start: `python main.py`, health check: `/healthz`. Background Worker web ilova uchun ommaviy HTTP manzil bermaydi.
2. Mavjud `.env` yoki hosting sozlamalariga `WEBAPP_URL=https://SIZNING-DOMEN/app` qo‘shing. Namuna: [.env.webapp.example](.env.webapp.example). `RENDER_EXTERNAL_URL` yoki `WEBHOOK_URL` mavjud bo‘lsa, `/app` manzili avtomatik olinadi. `WEBAPP_URL` bilan uni aniq belgilash mumkin.
3. Botni qayta ishga tushiring. Bot avtomatik Telegram menyu tugmasini **Web ilova** qilib o‘zgartiradi. `/app` buyrug‘i ham ochish tugmasini chiqaradi.
4. BotFather orqali bot profiliga alohida ochish tugmasi kerak bo‘lsa: `/mybots` → bot → **Bot Settings → Configure Mini App → Enable Mini App**, shu HTTPS manzilni kiriting.

Polling va webhook rejimlarining ikkalasida ham web server ishlaydi. `main_noai.py` ham shu serverdan foydalanadi, lekin AI yaratish o‘chiriladi. Tayyor savollar importi va qo‘lda yaratish ishlaydi. Mahalliy to‘liq server: `python main.py`, web manzil: `http://127.0.0.1:8080/app` (port sozlamasiga qarab). HTTP manzil lokal ko‘rish uchun; Telegram ochish tugmasiga HTTPS kerak.

## Ma’lumotlar va cheklovlar

### Sessiya yoki mavjud testlar yuklanmasa

- **Profil → Yordam** web ilovaning o‘zida matn kiritish va adminga yuborish oynasini ochadi. Deployga `webapp/` papkasidagi yangilangan HTML, JS va tarjimalarni ham yuboring.
- **Kirishni tasdiqlab bo‘lmadi** xatosi sessiya eskirganini anglatmaydi: deploydagi `BOT_TOKEN` ilovani ochayotgan aynan shu botning tokeni bo‘lishi kerak. `.env` da bir nechta turli `BOT_TOKEN` qatori bo‘lsa, bitta to‘g‘ri qatorni qoldiring. Hosting Environment’dagi token `.env` dagi qiymatdan ustun turadi. Tokenni chatga yubormang.
- **Sessiya muddati tugadi** xatosida ilovani to‘liq yopib, botdagi `/app` yoki «Web ilova» tugmasidan qayta oching. Xatodagi **Botdan qayta ochish** tugmasi web server ulangan botda yangi ochish tugmasini chiqaradi. Server logida rad etish sababi ko‘rinadi; maxfiy `initData` va token yozilmaydi.
- Bot va web bitta `DATABASE_URL` bilan ishlashi kerak. Lokal `quiz_bot.db` Render’dagi yangi SQLite bazasiga avtomatik ko‘chmaydi. Eski bot alohida xizmatda ishlayotgan bo‘lsa, web xizmatini uning mavjud Postgres bazasiga ulang. SQLite ishlatilsa, aynan eski baza faylini persistent diskda saqlab, `DATABASE_URL` ni o‘sha faylga yo‘naltiring; mavjud bazani bo‘sh baza bilan almashtirmang.
- **Mening testlarim** shu foydalanuvchining botda va webda yaratgan testlarini umumiy bazadan oladi. So‘rov xato bilan tugasa, «Testlar yo‘q» o‘rniga xato va qayta yuklash tugmasi chiqadi. Boshqa Telegram akkauntida yaratilgan testlar ko‘rsatilmaydi.

### Saqlash va yuklash

- Backend har bir API so‘rovida Telegram `initData` imzosi va muddatini tekshiradi. Foydalanuvchi ID si brauzerdan alohida qabul qilinmaydi. Begona foydalanuvchining test tafsilotlari ochilmaydi.
- AI jarayonlari fon vazifasi sifatida ishlaydi; ilovadagi boshqa bo‘limga o‘tish jarayonni to‘xtatmaydi. Bir foydalanuvchi uchun bir vaqtda bitta AI jarayoni. Muvaffaqiyatsiz urinish uchun band qilingan limit qaytariladi.
- Jarayon holati xotirada saqlanadi; server qayta ishga tushsa tugallanmagan jarayonni takrorlash kerak. Saqlangan testlar bazada qoladi. Hozir bitta server nusxasi ishlatilishi kerak.
- Skanerlangan PDF uchun dastlab matnli PDF yoki rasm yuklang. Sahifa oralig‘ini tanlash hozir bu web interfeysga kiritilmagan.
- Standart yuklash limiti hujjat va rasmlarda 5 MB, PPTX da 25 MB. Botdagi `MAX_UPLOAD_MB` va `MAX_UPLOAD_PPTX_MB` sozlamalari serverda ham qo‘llanadi.
- Render’da doimiy ma’lumotlar uchun Postgres yoki persistent diskdan foydalaning. Botning maxfiy kalitlari frontendga berilmaydi.

## Yordam

Profil rasmi tanlanganda dumaloq ko‘rish oynasi ochiladi. Rasmni barmoq yoki sichqoncha bilan siljitish, ikki barmoq yoki surilma bilan kattalashtirish mumkin. **To‘liq sig‘dirish** uzun rasmning barcha qismini aylana ichiga joylaydi; **Markazga qaytarish** boshlang‘ich kesimni qaytaradi. **O‘zgarishlarni saqlash** tanlangan ko‘rinishni kvadrat PNG sifatida saqlaydi; barcha avatarlar shu kesimni doira ichida ko‘rsatadi.

Web ilovada **Profil → Yordam** doimiy yozishmani ochadi. Foydalanuvchi xabari webdagi **Profil → Admin panel · Xabarlar** ro‘yxatiga tushadi. Admin yozishmani ochib javob yuboradi; javob foydalanuvchining aynan shu **Yordam** bo‘limida ko‘rinadi. Faol bo‘lim har 5 soniyada yangilanadi, yozayotgan qoralama saqlanadi. Yangi javoblar va yangi murojaatlar soni Profil ichida ko‘rsatiladi. Yozishmalar bazada qoladi, eski xabarlar bosqichma-bosqich ochiladi.

Web panel faqat `.env` yoki hostingdagi `ADMIN_IDS` ro‘yxatida bo‘lgan Telegram akkauntlariga ochiladi; ruxsat serverda har bir so‘rovda tekshiriladi. Foydalanuvchi faqat o‘z yozishmasini ko‘radi. Administratorlar uchun o‘qilgan holati umumiy hisoblanadi.

Web murojaatlari haqida administratorning Telegram chatiga ham bildirishnoma yuboriladi. Bu bildirishnoma yetmasa ham, bazaga saqlangan web xabari panelda qoladi va foydalanuvchiga muvaffaqiyatli yuborilgan deb ko‘rsatiladi. Botning shaxsiy chatidagi **Yordam**, `/help`, `/support`, `/yordam` esa avvalgi Telegram xabar yuborish oqimidan foydalanadi; u yerdagi xabarni **Bekor qilish** yoki `/cancel` bilan bekor qilish mumkin.

Bir xabar 3000 belgigacha; foydalanuvchi har daqiqada bitta murojaat yubora oladi. Webdagi yuborish cheklovi server qayta ishga tushganda ham saqlanadi. Kanal a’zoligini tekshirish ishlamayotganda ham yordam so‘rash mumkin. Yangilanishdan keyin bot/serverni qayta ishga tushiring va mini-ilovani qayta oching. Yangi jadvallar mavjud bazaga avtomatik qo‘shiladi; eski testlar saqlanadi. Admin statistikasi va ommaviy xabar yuborish botda qoladi.

## Tekshirish

```powershell
.venv/Scripts/python.exe -m unittest discover -s tests -v
node --check webapp/assets/app.js
node --check webapp/assets/locales.js
```

API testlari izolyatsiya qilingan vaqtinchalik baza, soxta Telegram identifikatori va AI javoblaridan foydalanadi; haqiqiy botga xabar yoki AI xizmatiga pullik so‘rov yubormaydi.

Windows’da Chrome o‘rnatilgan bo‘lsa, mobil va desktop ko‘rinishlari hamda asosiy tugmalarni tekshirish:

```powershell
.venv/Scripts/python.exe tests/browser_smoke.py
```

Skrinshotlar `downloads/webapp-qa/` ichida saqlanadi. Bu tekshiruv ham soxta Telegram sessiyasi va AI javoblaridan foydalanadi.

Telegram integratsiyasi: [rasmiy Mini Apps hujjati](https://core.telegram.org/bots/webapps).
