"""Small ready-to-play question packs; no simulated people or activity."""

STARTERS = [
    ("math", "Matematika · Bilimingizni sinang", [
        ("7 × 8 nechaga teng?", ["54", "56", "64", "48"], 1),
        ("144 ning kvadrat ildizi nechaga teng?", ["10", "11", "12", "14"], 2),
        ("200 ning 25 foizi nechaga teng?", ["25", "40", "50", "75"], 2),
        ("Uchburchak ichki burchaklari yig‘indisi qancha?", ["90°", "180°", "270°", "360°"], 1),
        ("3² + 4² nechaga teng?", ["7", "12", "24", "25"], 3),
    ]),
    ("science", "Tabiiy fanlar · Atrofimizdagi olam", [
        ("Suvning kimyoviy formulasi qaysi?", ["CO₂", "O₂", "H₂O", "NaCl"], 2),
        ("Yer Quyosh atrofida aylanadimi?", ["Ha", "Yo‘q", "Faqat yozda", "Faqat kechasi"], 0),
        ("Inson tanasida qonni qaysi a’zo haydaydi?", ["O‘pka", "Yurak", "Buyrak", "Oshqozon"], 1),
        ("O‘simliklar fotosintez jarayonida qaysi gazni yutadi?", ["Kislorod", "Azot", "Karbonat angidrid", "Vodorod"], 2),
        ("Muz eriganda qaysi holatga o‘tadi?", ["Gaz", "Suyuqlik", "Plazma", "Qattiq jism"], 1),
    ]),
    ("languages", "Ingliz tili · Birinchi qadamlar", [
        ("“Book” so‘zining tarjimasi qaysi?", ["Qalam", "Kitob", "Daftar", "Stol"], 1),
        ("“I ___ a student.” Qaysi so‘z mos?", ["is", "are", "am", "be"], 2),
        ("“Child” so‘zining ko‘plik shakli qaysi?", ["Childs", "Childes", "Children", "Childrens"], 2),
        ("“Apple” oldidan qaysi artikl qo‘yiladi?", ["a", "an", "am", "are"], 1),
        ("“Thank you” nimani anglatadi?", ["Salom", "Xayr", "Rahmat", "Iltimos"], 2),
    ]),
    ("technology", "Texnologiya · Raqamli bilimlar", [
        ("Ikkilik sanoq tizimi qaysi raqamlardan foydalanadi?", ["1 va 2", "0 va 1", "0 va 9", "2 va 8"], 1),
        ("Klaviatura nima uchun ishlatiladi?", ["Matn kiritish", "Qog‘oz chop etish", "Ovoz chiqarish", "Tasvirni kattalashtirish"], 0),
        ("Veb sahifani ko‘rish uchun qaysi dastur kerak?", ["Kalkulyator", "Brauzer", "Arxivator", "Taymer"], 1),
        ("1 bayt necha bitdan iborat?", ["2", "4", "8", "16"], 2),
        ("Qaysi parol odatda xavfsizroq?", ["123456", "password", "Ali2000", "Uzun va tasodifiy belgilar aralashmasi"], 3),
    ]),
    ("general", "Umumiy bilim · Dunyo bo‘ylab", [
        ("O‘zbekiston poytaxti qaysi shahar?", ["Samarqand", "Toshkent", "Buxoro", "Xiva"], 1),
        ("Bir haftada necha kun bor?", ["5", "6", "7", "8"], 2),
        ("Quyosh qaysi tomondan chiqadi?", ["Sharq", "G‘arb", "Shimol", "Janub"], 0),
        ("Yerning tabiiy yo‘ldoshi qaysi?", ["Mars", "Oy", "Venera", "Quyosh"], 1),
        ("Bir soat necha daqiqaga teng?", ["30", "45", "60", "100"], 2),
    ]),
]
