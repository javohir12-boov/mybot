# Botdagi statistika va xabar yuborish

Bu imkoniyatlar faqat `ADMIN_IDS` dagi administratorlar uchun Telegram bot menyusida ishlaydi. Web ilovada admin statistikasi yoki ommaviy yuborish bo‘limi yo‘q.

## Statistika

Bot menyusidagi admin statistikasida jami foydalanuvchilar, 24 soatlik faollik, **oxirgi 30 kundagi noyob faol foydalanuvchilar**, 30 kunda yangi qo‘shilganlar, testlar va urinishlar soni ko‘rsatiladi. Bir kishi necha marta foydalansa ham faol foydalanuvchilar sonida bir marta sanaladi.

Faollik foydalanuvchining botga xabar yuborishi, tugma bosishi yoki testga javob berishi bilan qayd qilinadi; faqat test yechganlar bilan cheklanmaydi. Bir foydalanuvchining faollik vaqti ko‘pi bilan har 5 daqiqada bazaga yangilanadi. Bu hisob rolling 30 kunlik davr uchun, kalendar oyi uchun emas. Web ilovani ko‘rishning o‘zi bot faolligiga qo‘shilmaydi.

Avvalgi versiyada barcha menyu harakatlari yozilmagan. Yangilanishdan oldingi davr uchun mavjud ro‘yxatdan o‘tish va test urinishlari hisobga olinadi; yangi umumiy faollik to‘liq qayd qilinishi bot qayta ishga tushirilgandan boshlanadi.

## Ommaviy xabarlar

1. Admin statistikasida foydalanuvchilar yoki guruhlarga xabar yuborishni tanlang.
2. Matn, rasm, video, hujjat, audio yoki stikerni yuboring. Matn formatlari va media `copyMessage` orqali saqlanadi. Yuborilgan xabarni bot barcha qabul qiluvchilarga asta-sekin tarqatadi.
3. Bot bir paytda bitta tarqatishni bajaradi. Standart guruh hajmi 25 ta; xabarlar orasida kamida 0.1 soniya, guruhlar orasida 2 soniya tanaffus bo‘ladi. Bazadan ham qabul qiluvchilar qismlab olinadi.
4. Jarayon xabarida jami, yuborilgan, yetib bormagan, shundan bloklangan/yopiq chatlar, navbatda qolganlar va yuborilgan guruhlar soni yangilanadi.
5. **Yuborishni to‘xtatish** bilan navbatni to‘xtatish mumkin. Yuborilmay qolganlar yetib bormaganlar sonidan alohida ko‘rsatiladi. Tugma tarqatishni boshlagan administrator uchun ishlaydi.

Telegram `RetryAfter` qaytarsa, bot ko‘rsatilgan muddatni kutib, shu qabul qiluvchiga yana urinadi. Bloklagan foydalanuvchilar sabab boshqa odamlarga yuborish to‘xtamaydi. Tarmoq xatosida Telegram xabarni qabul qilgan-qilmagani noaniq bo‘lishi mumkin, shuning uchun takroriy xabar yuborish xavfini kamaytirish uchun bunday urinish avtomatik takrorlanmaydi.

«Yuborildi» Telegram API muvaffaqiyatli javob qaytarganini bildiradi; bu foydalanuvchi xabarni o‘qiganini bildirmaydi. Telegram botlarga o‘qilganlik hisobotini bermaydi. Tezlik [Telegramning ommaviy yuborish cheklovlari](https://core.telegram.org/bots/faq#how-do-i-avoid-hitting-limits) hisobga olinib tanlangan; pullik tarqatish yoqilmaydi.

Natija har bir guruh tugagach va jarayon yakunlanganda bazaga yoziladi. Admin statistikasi oxirgi yuborish natijasini ham ko‘rsatadi. Bot to‘satdan o‘chsa, oxirgi tugallangan guruh hisoblari saqlanadi; keyingi ishga tushishda bunday tarqatish «uzilib qolgan» deb belgilanadi va avtomatik qayta yuborilmaydi. Bitta bot uchun bitta server nusxasi ishlatilishi kerak.

Ixtiyoriy sozlamalar (`.env` yoki hosting):

```dotenv
BROADCAST_BATCH_SIZE=25
BROADCAST_MESSAGE_DELAY=0.1
BROADCAST_BATCH_PAUSE=2
```

Yangilanishni ishga tushirish uchun botni qayta boshlang. Yangi jadvallar avtomatik yaratiladi. Testlar haqiqiy foydalanuvchilarga xabar yubormaydi.
