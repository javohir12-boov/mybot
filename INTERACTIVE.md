# Interaktiv testlar

Footerda odamlar ikonkasi bilan **Interaktiv** bo‘limi mavjud. Yuqorida ta’lim, sevgi, do‘stlik, qiziqishlar va boshqa yo‘nalishlar alohida ikonkalarda ko‘rsatiladi. Ta’lim ichida matematika, tarix, tabiiy fanlar, tillar, texnologiya va umumiy bilim filtrlari bor.

Matematika, tabiiy fanlar, ingliz tili, texnologiya va umumiy bilimdan beshtadan savolli boshlang‘ich testlar avtomatik qo‘shiladi. Bu testlarni istalgan foydalanuvchi xonada boshlashi mumkin. Onlayn odamlar yoki reyting uchun sun’iy sonlar qo‘shilmaydi.

## Birga ishlash

1. **Tayyor testlar → Birga boshlash** orqali xona oching.
2. Boshqalar shu yo‘nalishdagi **Jonli xonalar → Qo‘shilish** tugmasi yoki taklif havolasi bilan kiradi.
3. Savol va vaqt hamma uchun umumiy. Foydalanuvchi har savolga bir marta javob beradi. Qayta kirganda berilgan javobi saqlanadi.
4. Xona ochgan foydalanuvchi chiqib ketsa ham qolganlar ishlayveradi. Savollarni serverdagi taymer boshqaradi.
5. **Ketma-ket uchta savolga hech kim javob bermasa**, xona avtomatik to‘xtaydi. Istalgan qatnashchining javobi bu ketma-ketlikni nolga qaytaradi. Savollar tugasa test odatdagidek yakunlanadi.

Xonadan chiqish butun xonani to‘xtatmaydi. Brauzer yoki Telegram yopilganda foydalanuvchining onlayn belgisi 45 soniyagacha saqlanishi mumkin. Onlayn holat boshqa qatnashchilarning davom etishiga ta’sir qilmaydi. Kech qo‘shilgan odam ayni vaqtdagi savoldan davom etadi.

## Test yaratish va yoqtirish foizi

**Test qo‘shish** tugmasida yangi savollar kiriting yoki oxirgi saqlangan shaxsiy testlardan birini tanlang. Shaxsiy testning natija ekranidagi **Testni e’lon qilish** tugmasi orqali avvalroq saqlangan testni ham ulashish mumkin. Shaxsiy testlar faqat muallifning o‘zi e’lon qilgandan keyin boshqalarga ochiladi.

- **Bilim testi:** har savol uchun bitta to‘g‘ri javob belgilanadi. Yakunda to‘g‘ri javoblar va testning umumiy savollari bo‘yicha foiz chiqadi.
- **Yoqtirish testi:** muallif har bir javobga 0–100 ball beradi. Natija tanlangan javoblar ballarining o‘rtachasi: `yig‘indi / javoblar soni`, eng yaqin butun foizga yaxlitlanadi. Masalan, 75, 100 va 25 ball — **67%**. Natija muallif belgilagan ballar va qatnashchi javoblariga asoslangan o‘yin natijasidir.

Testni qisman ishlagan foydalanuvchiga natija **Qisman natija** deb ko‘rsatiladi. Yoqtirish natijasida faqat javob berilgan savollar o‘rtachasi olinadi. Boshqa qatnashchilarning shaxsiy yoqtirish foizi xonadagi ro‘yxatda berilmaydi.

Test 1–50 savoldan iborat bo‘ladi. Har savolga 4 ta javob; vaqt 10–120 soniya. E’lon qilingan savollar alohida nusxa sifatida saqlanadi, shaxsiy testni keyinchalik tahrirlash boshlangan xonadagi savollarni almashtirmaydi.

## Maxfiylik va olib tashlash

E’lon qilishdagi **Kimlar ko‘rsin?** maydonida **Barchaga ochiq** yoki **Maxfiy** tanlanadi. Avval e’lon qilingan testlar ochiq bo‘lib qoladi. Ochiq testlar va xonalari umumiy katalogda ko‘rinadi. Maxfiy testlar, xonalari va yakunlashlari katalog yoki mualliflar reytingiga kiritilmaydi. Maxfiy xona faqat xona havolasini olgan va Telegram orqali kirgan foydalanuvchilarga ochiladi; havolani keyin boshqalarga ulashish ham kirish huquqini beradi. Maxfiy testdan yangi xonani faqat muallif ochadi.

**Profil → E’lon qilgan testlarim** orqali muallif o‘zining ochiq va maxfiy testlarini ko‘radi. **Ochilgan xonaga qaytish** mavjud xonaga qaytaradi; **Birga boshlash** yangi xona ochadi. **Olib tashlash** va tasdiqlash testni katalogdan, profil ro‘yxatidan va reytingdan chiqaradi. Shu testga bog‘liq barcha faol xonalar, hatto ularni boshqa odam ochgan bo‘lsa ham, to‘xtaydi. Yangi qatnashchi qo‘shila olmaydi, savollarga javob berish va yangi xona ochish yopiladi. Botdagi shaxsiy asl test o‘chmaydi. Olib tashlangan e’lon va oldingi natijalar bazada tarix sifatida saqlanadi.

## Faol foydalanuvchilar va mualliflar reytingi

**Hozir faol** blokida shu interaktiv bo‘limda bo‘lgan foydalanuvchilar ko‘rinadi. Ism va ilovadagi username profildan olinadi. Telegram foydalanuvchi ID lari ommaviy ro‘yxatlarda berilmaydi.

**Faol mualliflar** reytingi tanlangan yo‘nalish/fan bo‘yicha testlari boshqalar tomonidan ko‘p yakunlangan mualliflarni 1, 2, 3 va keyingi o‘rinlarda ko‘rsatadi:

- Barcha savollarga javob berish yakunlash hisoblanadi; to‘g‘ri javob bo‘lishi shart emas.
- Bir foydalanuvchining aynan bir testni yakunlashi faqat bir marta sanaladi. Boshqa xonada qayta ishlash reytingni oshirmaydi.
- Muallifning o‘z testini ishlashi va qisman ishlangan testlar reytingga qo‘shilmaydi.
- Bir muallifning turli testlarini ishlash alohida sanaladi.

Bu ommaviy o‘yin reytingi; botdagi administrator statistikasi va xabar tarqatish bo‘limlari o‘z joyida qoladi.

## Ishga tushirish va saqlash

Yangilangan botni qayta ishga tushiring. Jadvallar va boshlang‘ich testlar avtomatik yaratiladi. Qo‘shimcha AI kaliti yoki paket o‘rnatish kerak emas. Havolalar shu bot orqali `start=room_<xona_id>` bilan ochiladi; botning mavjud `WEBAPP_URL` manzili ishlatiladi.

Xonalar, javoblar, ballar va yakunlashlar bazaga yoziladi. Server qayta ishga tushsa taymer bazadagi muddatdan davom etadi; o‘tib ketgan javobsiz savollar ham hisobga olinadi. Xonada hech kim qolmasa ham server uchta javobsiz savol qoidasini bajaradi.

Hozir bitta server nusxasi uchun mo‘ljallangan. Bir xonada 200 qatnashchi, foydalanuvchiga bir vaqtda 3 ta ochiq xona, serverda 100 ta ochiq xona va muallifga kuniga 20 ta yangi test chegarasi bor. Ro‘yxatlar so‘nggi 60 ta test, 50 ta xona, 20 ta yetakchi muallif va 12 tagacha onlayn profilni ko‘rsatadi; onlayn umumiy son alohida hisoblanadi.

## Tekshirish

```powershell
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/python.exe tests/browser_smoke.py
```

Tekshiruvlar vaqtinchalik baza va sinov foydalanuvchilari bilan bajariladi. Brauzer skrinshotlari: `downloads/webapp-qa/interactive-*.png`.
