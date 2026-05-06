from __future__ import annotations

import json
from pathlib import Path

from typing import Any, Dict

SUPPORTED_UI_LANGS = ('uz', 'ru', 'en', 'de', 'tr', 'kk', 'ar', 'zh', 'ko')


_STRINGS: Dict[str, Dict[str, str]] = {
    'uz': {
        'ref_rewarded': "Referral bonusi berildi: +{files} ta fayl, +{topics} ta mavzu.",
        'ref_progress': "Referral: {n}/3 (yana {need} ta kerak).",
        'ref_info': "Referral havola:\n{link}\n\nHolat:\n- Jami startlar: {total}\n- Kanalga a'zo bo'lganlar: {qualified}\n- Kutilmoqda: {pending}\n\nBonus: 3 ta yangi user kanalga a'zo bo'lsa sizga +2 ta fayl va +1 ta mavzu limiti beriladi.\nKeyingi bonus uchun kerak: {to_next} ta",
        'btn_referral': "Referral bonus",
        'menu': 'Menyu:',
        'about_menu': 'Menyu:',
        'menu_help': "Menyudagi bo'limlar:\n- Fayl yuklash: fayl yoki elektron kitob yuklash orqali test tuzishingiz mumkin.\n- Mavzu bo'yicha test: mavzuni yozib, qiyinlik/son/vaqtni tanlab test tuzishingiz mumkin.\n- Yangi test: savollarni qo'lda kiritib test tuzishingiz mumkin.\n- Interfeys tili: botning interfeys tilini o'zgartirishingiz mumkin.\n- Premium: limitlarni oshirish va premium olish bo'limi.\n- Bonuslar: referral va kanal bonusi bo'limi.",
        'must_join_channel': 'Botdan foydalanish uchun kanalga a\'zo bo\'ling: {channel}\nSo\'ng "Tekshirish" ni bosing.',
        'sub_required_alert': "Botdan foydalanish uchun kanalga a'zo bo'ling.",
        'sub_check_fail': "Hali kanalga a'zo emassiz.",
        'sub_check_ok': 'Rahmat! Endi botdan foydalanishingiz mumkin.',
        'btn_join_channel': "Kanalga a'zo bo'lish",
        'btn_check_sub': 'Tekshirish',
        'ui_lang_choose': 'Interfeys tilini tanlang:',
        'ui_lang_saved': 'Saqladim. Interfeys tili: {lang_name}',
        'ai_lang_choose': 'Tarjima tili (default) ni tanlang:',
        'ai_lang_saved': 'Tarjima tili (default): {lang_name}',
        'upload_hint': "Fayl yuboring (.pdf / .docx / .pptx / .txt).\nPDF/PPTX bo'lsa sahifa oralig'ini tanlash majburiy.",
        'upload_hint_noai': "Test faylini yuboring (.txt / .json / .docx / .pptx / .pdf).\nFaylda tayyor savollar bo'lishi kerak: Savol + A/B/C/D variantlar + Answer.\nMisol:\n1) Savol?\nA) ...\nB) ...\nC) ...\nD) ...\nAnswer: B",
        'topic_prompt': "Qaysi mavzu bo'yicha test tuzay?\nMavzuni yozing.\nMisol: `Davlatlar Poytaxti`",
        'file_topic_prompt': "Yuklangan fayl ichidan qaysi mavzuga fokus qilay? (ixtiyoriy)\nMavzuni yozing.\nMisol: `Davlatlar Poytaxti`",
        'topic_searching': "Manba qidiryapman (kitob/maqola)...",
        'topic_source_found': "Manba topildi: {title}\nEndi test sozlamalarini tanlaymiz.",
        'topic_source_not_found': "Aniq manba topilmadi. Yaxshiroq natija uchun kitob faylini (.pdf/.docx/.txt) yuboring yoki mavzuni aniqroq yozing.",
        'btn_topic_continue_anyway': "Baribir davom et",
        'quiz_not_found': 'Test topilmadi.',
        'quiz_card': 'Test: {title}\nSavollar: {count} ta\nHar savol: {sec}s\nID: {id}',
        'quiz_brief': 'Test: {title}\nSavollar: {count} ta\nID: {id}',
        'quiz_no_questions': 'Testda savollar topilmadi.',
        'no_quizzes_yet': "Sizda hali test yo'q. Fayl yuboring yoki Yangi test yaratishingiz mumkun.",
        'more_quizzes': 'Yana {n} ta test bor.',
        'chat_has_active_quiz': 'Bu chatda allaqachon aktiv test bor. Uni tugashini kuting yoki test egasi /cancel qilsin.',
        'quiz_started_private': "Test boshlandi: {count} ta savol, har biri {sec}s (taxminan {est}). Quiz ID: {quiz_id}\nJavob belgilagan zahoti keyingi savolga o'tadi.\nTezlashtirish: Keyingi tugmasi. To'xtatish: /cancel",
        'quiz_finished': 'Test tugadi.',
        'quiz_paused_inactive': "3 ta savolga ketma-ket javob bermadingiz. Test vaqtincha to'xtatildi.\nDavom ettirish uchun tugmani bosing.",
        'quiz_resumed_private': 'Test davom ettirildi. Qolgan: {remaining} ta savol, har biri {sec}s (taxminan {est}).',
        'quiz_stopped_no_participants': "Hamma ishtirokchi 3 ta savolga javob bermadi. Test to'xtatildi.",
        'resume_not_found': 'Davom ettirish sessiyasi topilmadi. Testni qayta boshlang.',
        'resumed_short': 'Davom ettirildi.',
        'stats_title': 'Statistika: {title}',
        'stats_no_attempts': 'Hali hech kim bu testni ishlamagan.',
        'stats_creator_only': "Statistikani faqat test egasi ko'ra oladi.",
        'scoreboard_title': 'Natijalar:',
        'participants_joined': 'Ishtirokchilar (Join): {n} ta',
        'total_questions': 'Jami savol: {n} ta',
        'skipped': "o'tkazib yubordi {n} ta",
        'scoreboard_more': '... yana {n} ta foydalanuvchi bor',
        'scoreboard_row': "{i}) {name}: {correct}/{answered} to'g'ri, vaqt {t}s, o'rtacha {avg}s{extra}",
        'lobby_ready': 'Test tayyor: {total} ta savol, har biri {sec}s.\nIshtirok etish uchun `Join` bosing.\nHamma (Join qilganlar) javob bersa, keyingi savol darhol chiqadi.\nIshtirokchilar: {n} ta.',
        'quiz_id_line': 'Quiz ID: {id}',
        'lobby_joined': "Qo'shildi: {name}",
        'lobby_already_joined': "Siz allaqachon qo'shilgansiz.",
        'lobby_not_found': 'Test topilmadi.',
        'quiz_cancelled': 'Test bekor qilingan.',
        'cannot_join_started': "Test boshlangan, endi qo'shib bo'lmaydi.",
        'joined_ok': 'Join qilindi.',
        'already_started': 'Allaqachon boshlangan.',
        'started_ok': 'Boshladik.',
        'group_started_status': "Test boshlandi: {count} ta savol, har biri {sec}s (taxminan {est}).\nIshtirokchilar: {n} ta.\nTezlashtirish: Keyingi (faqat creator). To'xtatish: /cancel",
        'lobby_creator_only': 'Faqat testni boshlagan user.',
        'quiz_not_started': 'Test hali boshlanmagan.',
        'next_question': 'Keyingi savol.',
        'lobby_started': 'Test boshlandi.',
        'btn_join': 'Join',
        'btn_start': 'Start',
        'btn_stop': "To'xtatish",
        'btn_next': 'Keyingi',
        'btn_resume': 'Davom ettirish',
        'btn_upload': 'Fayl yuklash',
        'btn_topic': "Mavzu bo'yicha test",
        'btn_newquiz': 'Yangi test',
        'btn_mytests': 'Mening testlarim',
        'btn_cancel_active': "Aktiv testni to'xtatish",
        'btn_ui_lang': 'Interfeys tili',
        'btn_ai_lang': 'AI tarjima tili',
        'btn_start_quiz': 'Testni boshlash',
        'btn_retry_quiz': "Qayta urinib ko'rish",
        'btn_share_quiz': 'Testni ulashish',
        'btn_start_group': 'Guruhda boshlash',
        'btn_stats': 'рџ“Љ Statistika',
        'btn_edit_quiz': 'Tahrirlash',
        'btn_export_docx': 'Test fayli (.docx)',
        'btn_edit_title': 'Nomini tahrirlash',
        'btn_edit_time': 'Vaqtni tahrirlash',
        'btn_edit_answers': "Javoblarni tahrirlash",
        'btn_prev_page': 'в¬…пёЏ Oldingi',
        'btn_next_page': 'Keyingi вћЎпёЏ',
        'btn_back': 'Orqaga',
        'btn_cancel': 'Bekor qilish',
        'btn_topic_optional': 'Mavzu (ixtiyoriy)',
        'btn_pages_optional': 'Sahifa',
        'btn_no_translate': "Tarjima yo'q (asl)",
        'lang_source': "Asl (tarjima yo'q)",
        'btn_translate_default': 'Tarjima (default: {name})',
        'btn_translate_choose': 'Boshqa til',
        'btn_translate': 'Tarjima qilish',
        'ai_disabled': "AI funksiyalar o'chirilgan. (BOT_MODE=noai)",
        'choose_count': "Savol sonini tugmalardan tanlang.",
        'choose_time': 'Har savol uchun nechchi sekund? 20/30/40/50/60/70 tugmalaridan birini bosing.',
        'need_translation': 'Tarjima kerakmi?',
        'shuffle_prompt_ai': 'Aralashtirish turini tanlang:',
        'shuffle_prompt_manual': 'Test uchun aralashtirish turini tanlang:',
        'btn_shuffle_questions': 'Faqat savollar',
        'btn_shuffle_answers': 'Faqat javoblar',
        'btn_shuffle_both': 'Ikkalasini ham',
        "btn_shuffle_keep": "Aralashtirma",
        'shuffle_strategy_prompt': 'Random tartibni qanday ishlataylik?',
        'btn_shuffle_saved_once': 'Bir marta random qilib saqlash',
        'btn_shuffle_every_run': 'Har urinishda qayta random',
        'choose_translation_lang': 'Qaysi tilga tarjima qilay?',
        'pages_prompt': "📄 Qaysi sahifalar oralig'idan test tuzay? (1..{total})\nMasalan: `20-30` yoki `20`",
        'pages_required': "Sahifa oralig'ini tanlash majburiy.",
        'ai_partial': "Eslatma: so'ralgan {wanted} ta, yaratildi {made} ta.",
        'pages_invalid': "❌ Sahifa oralig'i noto'g'ri.\n1..{total} oralig'ida `20-30` yoki `20` ko'rinishida yuboring.",
        'current_pages': '📌 Hozirgi sahifalar: {p_from}-{p_to}',
        'pages_set': '✅ Sahifalar saqlandi: {p_from}-{p_to}',
        'pages_cleared': "Sahifa oralig'i olib tashlandi (hamma sahifa).",
        'ai_working': 'AI test tayyorlamoqda, kuting...',
        'topic_line': '🧠 Mavzu: {topic}\n',
        'pages_line': '📄 Sahifalar: {p_from}-{p_to}\n',
        'done_line': '✨ Yakun: sahifa {p_from}-{p_to}, savol {n} ta.\n',
        'ai_quiz_ready': '✅ Test tayyor!\n\n📚 Test: {title}\n{topic_line}❓ Savollar: {count} ta\n⏱ Har savol: {sec}s (taxminan {est})\n🆔 ID: {id}\n\n🚀 Boshlash uchun `Testni boshlash` tugmasini bosing.',
        'err_ai': 'Xato: {err}',
        'err_unexpected': 'Kutilmagan xato: {err}',
        'err_rate_limit': 'AI xizmati hozirda band (so\'rovlar limiti to\'ldi). Iltimos, 1-2 daqiqadan so\'ng qayta urinib ko\'ring.',
        'err_unexpected_safe': 'Kutilmagan xato yuz berdi. Iltimos, bir oz kuting va qayta urinib ko\'ring.',
        'no_input_for_ai': "Ma'lumot topilmadi. Fayl yuboring yoki /topic buyrug'idan foydalaning.",
        'session_missing': "Session topilmadi. Bot qayta ishga tushgan bo'lishi mumkin yoki bot 2 ta nusxada ishlayapti. /menu dan qayta boshlang.",
        'session_owner_only': 'Faqat session egasi tanlay oladi.',
        'choose_one_count': 'Iltimos bitta savol sonini tanlang: {options}',
        'choose_one_sec': 'Iltimos bitta sekund tanlang: {options}',
        'current_topic': 'Hozirgi mavzu: {topic}',
        'count_invalid': "Iltimos, savol sonini tugmalardan tanlang.",
        'time_invalid': 'Faqat 20/30/40/50/60/70 tugmalaridan birini tanlang.',
        'accepted': 'Qabul qilindi.',
        'count_chosen': 'Savol soni: {n} ta.',
        'file_type_only': 'Faqat .pdf, .docx, .pptx, .png, .jpg, .txt, .md yoki .json fayl yuboring.',
        'file_too_large': 'Fayl juda katta. Maksimal: {mb} MB.',
        'file_received_downloading': 'Fayl qabul qilindi. Yuklab olinmoqda...',
        'extracting_text': 'Matn ajratib olinmoqda...',
        'text_too_short': 'Matn juda qisqa chiqdi.\nTXT/DOCX yuboring yoki PDF matnli ekanini tekshiring.',
        'scan_pdf_need_gemini': "Bu fayl skan ko'rinadi (matn yo'q) yoki rasm.\nBunday test uchun vision kerak: OpenAI yoki Gemini.\n`.env` da `OPENAI_API_KEY=...` (yoki `GEMINI_API_KEY=...`) va `AI_PROVIDER=auto|openai|gemini`.",
        'scan_pdf_rendering': 'PDF skan. Sahifalar rasmga aylantirilmoqda, kuting...',
        'scan_pdf_no_images': "PDF dan rasm ajratib bo'lmadi. Boshqa fayl yuboring.",
        'scan_pdf_choose_count': 'PDF skan: {pages} ta sahifa.\nHar sahifadan 1 ta savol qilaman.\nSavol sonini tugmalardan tanlang.',
        'cancelled': 'Bekor qilindi.',
        'saved_short': 'Saqladim.',
        'stopped_n': "To'xtatildi: {n} ta aktiv test.",
        'invalid_button': 'Xato tugma.',
        'error_short': 'Xato.',
        'rate_limited': 'Juda tez yuboryapsiz. {sec}s kuting.',
        'chat_not_found': 'Chat topilmadi.',
        'starting': 'Boshlanmoqda...',
        'bot_username_missing': 'Bot username topilmadi.',
        'share_link': 'Ulashish linki:\n{link}',
        'group_start_link': 'Guruhda boshlash uchun link:\n{link}',
        'export_working': 'Test fayli tayyorlanmoqda...',
        'export_failed': 'Fayl yaratib bo\'lmadi: {err}',
        'export_docx_caption': 'Test fayli (.docx): {title}\\nID: {id}\\nJavoblar oxirida.',
        'export_answer_key_title': 'Javoblar kaliti',
        'export_sent_private_notice': 'Test fayli shaxsiy chatga yuborildi.',
        'manual_title_prompt': 'Yangi test nomini kiriting:',
        'manual_title_required': 'Test nomini kiriting:',
        'manual_first_question': "1-savolni yuboring.\n- Matn yozishingiz mumkin\n- Yoki savol rasmini yuborishingiz mumkin (caption bo'lsa savol matni bo'ladi)",
        'manual_image_not_found': 'Rasm topilmadi. Qayta yuboring.',
        'manual_send_4_options': '4 ta variant yuboring (har biri alohida qatorda):',
        'manual_send_image_or_text': 'Rasm yuboring (.jpg/.png/.webp) yoki savol matnini yozing.',
        'manual_send_question_or_image': 'Savol matnini yuboring yoki rasm yuboring.',
        'manual_has_image': 'Savol uchun rasm bormi? Rasm yuboring yoki /skip yozing.',
        'manual_image_or_skip': 'Rasm yuboring (.jpg/.png/.webp) yoki /skip yozing.',
        'manual_image_or_skip_short': 'Rasm yuboring yoki /skip yozing.',
        'manual_need_4_lines': 'Iltimos, aynan 4 ta variant yuboring (4 qator).',
        'manual_choose_correct': "To'g'ri variant raqamini tanlang:",
        'manual_callback_error': 'Xato callback.',
        'manual_question_missing': "Savol ma'lumotlari topilmadi, /newquiz dan qayta boshlang.",
        'manual_saved_total': 'Savol saqlandi. Jami: {n} ta.',
        'manual_next_question': 'Keyingi savolni yuboring (matn yoki rasm):',
        'manual_empty': "Test bo'sh. /newquiz dan qayta boshlang.",
        'manual_created': 'Test yaratildi! ID: {id}. Savollar: {n} ta.',
        'manual_draft_found': "Sizda saqlangan qo'lda tuzilayotgan test bor. Davom ettiraymi?",
        'btn_manual_add_more': 'Yana savol',
        'btn_manual_finish': 'Tugatish',
        'btn_manual_continue': 'Davom ettirish',
        'btn_manual_restart': 'Yangidan',
        'bad_link': "Link noto'g'ri.",
        'stopped': "Test to'xtatildi.",
        'image_question': 'Rasmli savol',
        'explanation_prefix': 'Izoh: {text}',
        'choose_answer': 'Javobni tanlang',
        'importing_quiz': 'Test import qilinmoqda...',
        'import_failed': "Fayldan test import qilib bo'lmadi. Formatni tekshiring.",
        'import_ok': 'Test import qilindi!\nTest: {title}\nSavollar: {n} ta\nID: {id}',
        'import_partial': "Fayldan {found} ta savol topildi (so'ralgan: {wanted}). Sahifa oralig'ini kengaytiring yoki formatni tekshiring.",
        'edit_menu': 'Test: {title}\nSavollar: {count} ta\nHar savol: {sec}s\nID: {id}\n\nNimani tahrirlaymiz?',
        'edit_answers_choose_question': "Javobni tahrirlash: savol raqamini tanlang ({count} ta).",
        'edit_answers_choose_correct': "To'g'ri javob raqamini tanlang:",
        'edit_answers_updated': "Saqladim.",
        'edit_title_prompt': 'Yangi nomni yuboring:',
        'edit_time_prompt': 'Yangi vaqtni 20/30/40/50/60/70 sekund tugmalaridan tanlang:',
        'edit_saved': 'Saqladim.',
        'edit_creator_only': 'Tahrirlash faqat test egasiga mumkin.',
        'choose_difficulty': 'Qiyinlik darajasini tanlang:',
        'difficulty_invalid': "Iltimos qiyinlikni tanlang: Oson | O'rta | Qiyin | Aralash",
        'btn_diff_easy': 'Oson',
        'btn_diff_medium': "O'rta",
        'btn_diff_hard': 'Qiyin',
        'btn_diff_mixed': 'Aralash (default)',
        'btn_premium': 'Premium',
        'btn_admin_users': 'Admin statistika',
        'admin_users_stats': 'Admin statistika\n\nJami foydalanuvchilar: {total} ta\nOxirgi 24 soatda qo\'shilganlar: {joined} ta\nOxirgi 24 soatda aktiv foydalanuvchilar: {active} ta\nJami testlar: {quizzes} ta\nOxirgi 24 soatda ishlangan testlar: {attempts} ta',
        'premium_choose_plan': 'Premium paketni tanlang:',
        'premium_status_free': "Bepul (1 marta): Fayl {f_left}/{f_total}, Mavzu {t_left}/{t_total}\nMuddat: {days} kun (tugash: {until})",
        'premium_status_premium': 'Premium faol (tugash: {until})\nFayl: {f_left}/{f_total}\nMavzu: {t_left}/{t_total}',
        'premium_send_screenshot': "To'lov cheki yoki screenshotini yuboring. Rasm, PDF yoki share-check matni ham bo'ladi.",
        'receipt_checking': 'Chek tekshirilyapti...',
        'premium_received': 'Qabul qilindi. Admin tekshiradi va premium beradi.',
        'premium_need_image': "Iltimos, to'lov cheki yoki screenshotini yuboring. Rasm, PDF yoki share-check matni ham bo'ladi.",
        'limit_free_reached': 'Bepul limit tugadi. Premium oling.',
        'limit_premium_reached': 'Premium limiti tugadi. Premium tugash vaqti: {until}\nPaketni uzaytirish uchun premium oling.',
        'premium_plan_1d': '1 kun',
        'premium_plan_7d': '7 kun',
        'premium_plan_30d': '1 oy',
        'btn_premium_approve': 'Tasdiqlash',
        'btn_premium_reject': 'Rad etish',
        'premium_approved_user': 'Premium aktiv qilindi. Tugash: {until}',
        'premium_rejected_user': "Premium so'rovi rad etildi.",
        'admin_only': 'Faqat admin.',
        'topic_too_long': 'Mavzu juda uzun. Maks: {n} belgi.',
        'btn_pay': "To'lov qilish",
        'payment_card_missing': "Karta raqami sozlanmagan. Admin bilan bog'laning.",
        'payment_card_info': "To'lov uchun karta:\n{card}\nEga: {holder}\n\nPaket: {plan}\nNarx: {price} UZS\n\nTo'lov qiling va chek yoki screenshot yuboring.",
        'premium_plan_details': "Tanlangan paket: {plan}\nDavomiylik: {days} kun\nNarx: {price} UZS\nChegirma: -{disc}%\n\nLimit: Fayl {files}, Mavzu {topics}\n\nDavom etish uchun `To'lov qilish` tugmasini bosing.",
    },
    'ru': {
        'ref_rewarded': "???????? ??????????? ?????: +{files} ??????, +{topics} ???.",
        'ref_progress': "???????: {n}/3 (????? ??? {need}).",
        'ref_info': "??????????? ??????:\n{link}\n\n??????:\n- ????? ???????: {total}\n- ??????????? ?? ?????: {qualified}\n- ? ????????: {pending}\n\n?????: ???? 3 ????? ???????????? ?????????? ?? ?????, ?? ???????? +2 ???????? ?????? ? +1 ???? ?? ????.\n?? ?????????? ??????: {to_next}",
        'btn_referral': "???????",
        'menu': 'РњРµРЅСЋ:',
        'about_menu': 'РњРµРЅСЋ:',
        'ui_lang_choose': 'Р’С‹Р±РµСЂРёС‚Рµ СЏР·С‹Рє РёРЅС‚РµСЂС„РµР№СЃР°:',
        'ui_lang_saved': 'РЎРѕС…СЂР°РЅРµРЅРѕ. РЇР·С‹Рє РёРЅС‚РµСЂС„РµР№СЃР°: {lang_name}',
        'ai_lang_choose': 'Р’С‹Р±РµСЂРёС‚Рµ СЏР·С‹Рє AI-РїРµСЂРµРІРѕРґР° (РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ):',
        'ai_lang_saved': 'РЇР·С‹Рє AI-РїРµСЂРµРІРѕРґР° (РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ): {lang_name}',
        'upload_hint': 'РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» (.pdf / .docx / .pptx / .txt).\nР”Р»СЏ PDF/PPTX РІС‹Р±РѕСЂ РґРёР°РїР°Р·РѕРЅР° СЃС‚СЂР°РЅРёС† РѕР±СЏР·Р°С‚РµР»РµРЅ.',
        'upload_hint_noai': 'РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» СЃ С‚РµСЃС‚РѕРј (.txt / .json / .docx / .pptx / .pdf).\nР’ С„Р°Р№Р»Рµ РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ РіРѕС‚РѕРІС‹Рµ РІРѕРїСЂРѕСЃС‹: РІРѕРїСЂРѕСЃ + РІР°СЂРёР°РЅС‚С‹ A/B/C/D + Answer.\nРџСЂРёРјРµСЂ:\n1) Р’РѕРїСЂРѕСЃ?\nA) ...\nB) ...\nC) ...\nD) ...\nAnswer: B',
        'topic_prompt': 'РџРѕ РєР°РєРѕР№ С‚РµРјРµ СЃРґРµР»Р°С‚СЊ С‚РµСЃС‚?\nРќР°РїРёС€РёС‚Рµ С‚РµРјСѓ.\nРџСЂРёРјРµСЂ: `SQL JOIN`',
        'file_topic_prompt': 'РћРїС†РёРѕРЅР°Р»СЊРЅРѕ: РЅР° РєР°РєСѓСЋ С‚РµРјСѓ РІРЅСѓС‚СЂРё Р·Р°РіСЂСѓР¶РµРЅРЅРѕРіРѕ С„Р°Р№Р»Р° СЃС„РѕРєСѓСЃРёСЂРѕРІР°С‚СЊСЃСЏ?\nРќР°РїРёС€РёС‚Рµ С‚РµРјСѓ.\nРџСЂРёРјРµСЂ: `SQL JOIN`',
        'topic_searching': 'РС‰Сѓ РёСЃС‚РѕС‡РЅРёРє (РєРЅРёРіР°/СЃС‚Р°С‚СЊСЏ)...',
        'topic_source_found': 'РСЃС‚РѕС‡РЅРёРє РЅР°Р№РґРµРЅ: {title}\nРўРµРїРµСЂСЊ РІС‹Р±РµСЂРёС‚Рµ РЅР°СЃС‚СЂРѕР№РєРё С‚РµСЃС‚Р°.',
        'topic_source_not_found': 'РќРµ СѓРґР°Р»РѕСЃСЊ РЅР°Р№С‚Рё С‚РѕС‡РЅС‹Р№ РёСЃС‚РѕС‡РЅРёРє. Р”Р»СЏ Р»СѓС‡С€РµРіРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р° РѕС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» РєРЅРёРіРё (.pdf/.docx/.txt) РёР»Рё СѓС‚РѕС‡РЅРёС‚Рµ С‚РµРјСѓ.',
        'btn_topic_continue_anyway': 'Р’СЃРµ СЂР°РІРЅРѕ РїСЂРѕРґРѕР»Р¶РёС‚СЊ',
        'quiz_not_found': 'РўРµСЃС‚ РЅРµ РЅР°Р№РґРµРЅ.',
        'quiz_card': 'РўРµСЃС‚: {title}\nР’РѕРїСЂРѕСЃРѕРІ: {count}\nРќР° РІРѕРїСЂРѕСЃ: {sec}s\nID: {id}',
        'quiz_brief': 'РўРµСЃС‚: {title}\nР’РѕРїСЂРѕСЃРѕРІ: {count}\nID: {id}',
        'quiz_no_questions': 'Р’ С‚РµСЃС‚Рµ РЅРµС‚ РІРѕРїСЂРѕСЃРѕРІ.',
        'no_quizzes_yet': 'РЈ РІР°СЃ РїРѕРєР° РЅРµС‚ С‚РµСЃС‚РѕРІ. РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» РёР»Рё СЃРѕР·РґР°Р№С‚Рµ РЅРѕРІС‹Р№ С‚РµСЃС‚.',
        'more_quizzes': 'Р•С‰Рµ {n} С‚РµСЃС‚(РѕРІ).',
        'chat_has_active_quiz': 'Р’ СЌС‚РѕРј С‡Р°С‚Рµ СѓР¶Рµ РµСЃС‚СЊ Р°РєС‚РёРІРЅС‹Р№ С‚РµСЃС‚. Р”РѕР¶РґРёС‚РµСЃСЊ РѕРєРѕРЅС‡Р°РЅРёСЏ РёР»Рё РїРѕРїСЂРѕСЃРёС‚Рµ Р°РІС‚РѕСЂР° СЃРґРµР»Р°С‚СЊ /cancel.',
        'quiz_started_private': 'РўРµСЃС‚ РЅР°С‡Р°Р»СЃСЏ: {count} РІРѕРїСЂРѕСЃРѕРІ, РїРѕ {sec}s (РїСЂРёРјРµСЂРЅРѕ {est}). Quiz ID: {quiz_id}\nРџРѕСЃР»Рµ РѕС‚РІРµС‚Р° СЃСЂР°Р·Сѓ РїРµСЂРµС…РѕРґРёС‚ Рє СЃР»РµРґСѓСЋС‰РµРјСѓ.\nРЈСЃРєРѕСЂРёС‚СЊ: РєРЅРѕРїРєР° В«Р”Р°Р»РµРµВ». РћСЃС‚Р°РЅРѕРІРёС‚СЊ: /cancel',
        'quiz_finished': 'РўРµСЃС‚ Р·Р°РІРµСЂС€РµРЅ.',
        'quiz_paused_inactive': 'Р’С‹ РїСЂРѕРїСѓСЃС‚РёР»Рё 3 РІРѕРїСЂРѕСЃР° РїРѕРґСЂСЏРґ. РўРµСЃС‚ РїРѕСЃС‚Р°РІР»РµРЅ РЅР° РїР°СѓР·Сѓ.\nРќР°Р¶РјРёС‚Рµ В«РџСЂРѕРґРѕР»Р¶РёС‚СЊВ».',
        'quiz_resumed_private': 'РўРµСЃС‚ РїСЂРѕРґРѕР»Р¶РµРЅ. РћСЃС‚Р°Р»РѕСЃСЊ: {remaining} РІРѕРїСЂРѕСЃРѕРІ, РїРѕ {sec}s (РїСЂРёРјРµСЂРЅРѕ {est}).',
        'quiz_stopped_no_participants': 'Р’СЃРµ СѓС‡Р°СЃС‚РЅРёРєРё РїСЂРѕРїСѓСЃС‚РёР»Рё 3 РІРѕРїСЂРѕСЃР°. РўРµСЃС‚ РѕСЃС‚Р°РЅРѕРІР»РµРЅ.',
        'resume_not_found': 'РЎРµСЃСЃРёСЏ РїСЂРѕРґРѕР»Р¶РµРЅРёСЏ РЅРµ РЅР°Р№РґРµРЅР°. Р—Р°РїСѓСЃС‚РёС‚Рµ С‚РµСЃС‚ Р·Р°РЅРѕРІРѕ.',
        'resumed_short': 'РџСЂРѕРґРѕР»Р¶РµРЅРѕ.',
        'stats_title': 'РЎС‚Р°С‚РёСЃС‚РёРєР°: {title}',
        'stats_no_attempts': 'Р­С‚РѕС‚ С‚РµСЃС‚ РµС‰Рµ РЅРёРєС‚Рѕ РЅРµ РїСЂРѕС…РѕРґРёР».',
        'stats_creator_only': 'РЎС‚Р°С‚РёСЃС‚РёРєСѓ РјРѕР¶РµС‚ СЃРјРѕС‚СЂРµС‚СЊ С‚РѕР»СЊРєРѕ РІР»Р°РґРµР»РµС† С‚РµСЃС‚Р°.',
        'scoreboard_title': 'Р РµР·СѓР»СЊС‚Р°С‚С‹:',
        'participants_joined': 'РЈС‡Р°СЃС‚РЅРёРєРё (Join): {n}',
        'total_questions': 'Р’СЃРµРіРѕ РІРѕРїСЂРѕСЃРѕРІ: {n}',
        'skipped': 'РїСЂРѕРїСѓСЃС‚РёР» {n}',
        'scoreboard_more': '... РµС‰Рµ {n} РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№',
        'scoreboard_row': '{i}) {name}: {correct}/{answered} РІРµСЂРЅРѕ, РІСЂРµРјСЏ {t}s, СЃСЂРµРґ. {avg}s{extra}',
        'lobby_ready': 'РўРµСЃС‚ РіРѕС‚РѕРІ: {total} РІРѕРїСЂРѕСЃРѕРІ, РїРѕ {sec}s.\nРќР°Р¶РјРёС‚Рµ `Join`, С‡С‚РѕР±С‹ СѓС‡Р°СЃС‚РІРѕРІР°С‚СЊ.\nР•СЃР»Рё РІСЃРµ (РєС‚Рѕ РЅР°Р¶Р°Р» Join) РѕС‚РІРµС‚СЏС‚, СЃР»РµРґСѓСЋС‰РёР№ РІРѕРїСЂРѕСЃ РІС‹Р№РґРµС‚ СЃСЂР°Р·Сѓ.\nРЈС‡Р°СЃС‚РЅРёРєРё: {n}.',
        'quiz_id_line': 'Quiz ID: {id}',
        'lobby_joined': 'РџСЂРёСЃРѕРµРґРёРЅРёР»СЃСЏ: {name}',
        'lobby_already_joined': 'Р’С‹ СѓР¶Рµ РїСЂРёСЃРѕРµРґРёРЅРёР»РёСЃСЊ.',
        'lobby_not_found': 'РўРµСЃС‚ РЅРµ РЅР°Р№РґРµРЅ.',
        'quiz_cancelled': 'РўРµСЃС‚ РѕС‚РјРµРЅРµРЅ.',
        'cannot_join_started': 'РўРµСЃС‚ СѓР¶Рµ РЅР°С‡Р°Р»СЃСЏ, РїСЂРёСЃРѕРµРґРёРЅРёС‚СЊСЃСЏ РЅРµР»СЊР·СЏ.',
        'joined_ok': 'Р’С‹ РїСЂРёСЃРѕРµРґРёРЅРёР»РёСЃСЊ.',
        'already_started': 'РЈР¶Рµ Р·Р°РїСѓС‰РµРЅРѕ.',
        'started_ok': 'Р—Р°РїСѓСЃРєР°СЋ.',
        'group_started_status': 'РўРµСЃС‚ РЅР°С‡Р°Р»СЃСЏ: {count} РІРѕРїСЂРѕСЃРѕРІ, РїРѕ {sec}s (РїСЂРёРјРµСЂРЅРѕ {est}).\nРЈС‡Р°СЃС‚РЅРёРєРё: {n}.\nРЈСЃРєРѕСЂРёС‚СЊ: Р”Р°Р»РµРµ (С‚РѕР»СЊРєРѕ Р°РІС‚РѕСЂ). РћСЃС‚Р°РЅРѕРІРёС‚СЊ: /cancel',
        'lobby_creator_only': 'РўРѕР»СЊРєРѕ Р°РІС‚РѕСЂ С‚РµСЃС‚Р°.',
        'quiz_not_started': 'РўРµСЃС‚ РµС‰Рµ РЅРµ РЅР°С‡Р°Р»СЃСЏ.',
        'next_question': 'РЎР»РµРґСѓСЋС‰РёР№ РІРѕРїСЂРѕСЃ.',
        'lobby_started': 'РўРµСЃС‚ РЅР°С‡Р°Р»СЃСЏ.',
        'btn_join': 'Join',
        'btn_start': 'Start',
        'btn_stop': 'РЎС‚РѕРї',
        'btn_next': 'Р”Р°Р»РµРµ',
        'btn_resume': 'РџСЂРѕРґРѕР»Р¶РёС‚СЊ',
        'btn_upload': 'Р—Р°РіСЂСѓР·РёС‚СЊ С„Р°Р№Р»',
        'btn_topic': 'РўРµСЃС‚ РїРѕ С‚РµРјРµ',
        'btn_newquiz': 'РќРѕРІС‹Р№ С‚РµСЃС‚',
        'btn_mytests': 'РњРѕРё С‚РµСЃС‚С‹',
        'btn_cancel_active': 'РћСЃС‚Р°РЅРѕРІРёС‚СЊ Р°РєС‚РёРІРЅС‹Р№ С‚РµСЃС‚',
        'btn_ui_lang': 'РЇР·С‹Рє РёРЅС‚РµСЂС„РµР№СЃР°',
        'btn_ai_lang': 'РЇР·С‹Рє AI-РїРµСЂРµРІРѕРґР°',
        'btn_start_quiz': 'РќР°С‡Р°С‚СЊ С‚РµСЃС‚',
        'btn_retry_quiz': 'РџРѕРїСЂРѕР±РѕРІР°С‚СЊ СЃРЅРѕРІР°',
        'btn_share_quiz': 'РџРѕРґРµР»РёС‚СЊСЃСЏ',
        'btn_start_group': 'Р—Р°РїСѓСЃС‚РёС‚СЊ РІ РіСЂСѓРїРїРµ',
        'btn_stats': 'рџ“Љ РЎС‚Р°С‚РёСЃС‚РёРєР°',
        'btn_edit_quiz': 'Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ',
        'btn_export_docx': 'Р¤Р°Р№Р» С‚РµСЃС‚Р° (.docx)',
        'btn_edit_title': 'РќР°Р·РІР°РЅРёРµ',
        'btn_edit_time': 'Р’СЂРµРјСЏ',
        'btn_edit_answers': 'Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РѕС‚РІРµС‚С‹',
        'btn_prev_page': 'в¬…пёЏ РќР°Р·Р°Рґ',
        'btn_next_page': 'Р”Р°Р»РµРµ вћЎпёЏ',
        'btn_back': 'РќР°Р·Р°Рґ',
        'btn_cancel': 'РћС‚РјРµРЅР°',
        'btn_topic_optional': 'РўРµРјР° (РѕРїС†.)',
        'btn_pages_optional': 'РЎС‚СЂР°РЅРёС†С‹',
        'btn_no_translate': 'Р‘РµР· РїРµСЂРµРІРѕРґР° (РѕСЂРёРі.)',
        'lang_source': 'РћСЂРёРіРёРЅР°Р» (Р±РµР· РїРµСЂРµРІРѕРґР°)',
        'btn_translate_default': 'РџРµСЂРµРІРѕРґ (РїРѕ СѓРјРѕР»С‡.: {name})',
        'btn_translate_choose': 'Р”СЂСѓРіРѕР№ СЏР·С‹Рє',
        'btn_translate': 'РџРµСЂРµРІРµСЃС‚Рё',
        'ai_disabled': 'Р¤СѓРЅРєС†РёРё AI РѕС‚РєР»СЋС‡РµРЅС‹. (BOT_MODE=noai)',
        'choose_count': 'Р’С‹Р±РµСЂРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ РєРЅРѕРїРєР°РјРё.',
        'choose_time': 'РЎРєРѕР»СЊРєРѕ СЃРµРєСѓРЅРґ РЅР° РІРѕРїСЂРѕСЃ? РќР°Р¶РјРёС‚Рµ 20/30/40/50 РёР»Рё РѕС‚РїСЂР°РІСЊС‚Рµ С‡РёСЃР»Рѕ (5..600). РџСЂРёРјРµСЂ: 30',
        'need_translation': 'РќСѓР¶РµРЅ РїРµСЂРµРІРѕРґ?',
        'shuffle_prompt_ai': 'Р’С‹Р±РµСЂРёС‚Рµ СЂРµР¶РёРј РїРµСЂРµРјРµС€РёРІР°РЅРёСЏ РґР»СЏ С‚РµСЃС‚Р°:',
        'shuffle_prompt_manual': 'Р’С‹Р±РµСЂРёС‚Рµ СЂРµР¶РёРј РїРµСЂРµРјРµС€РёРІР°РЅРёСЏ РґР»СЏ СЌС‚РѕРіРѕ С‚РµСЃС‚Р°:',
        'btn_shuffle_questions': 'РўРѕР»СЊРєРѕ РІРѕРїСЂРѕСЃС‹',
        'btn_shuffle_answers': 'РўРѕР»СЊРєРѕ РѕС‚РІРµС‚С‹',
        'btn_shuffle_both': 'Р РІРѕРїСЂРѕСЃС‹, Рё РѕС‚РІРµС‚С‹',
        'btn_shuffle_keep': 'Р‘РµР· РїРµСЂРµРјРµС€РёРІР°РЅРёСЏ',
        'shuffle_strategy_prompt': 'РљР°Рє РґРѕР»Р¶РµРЅ СЂР°Р±РѕС‚Р°С‚СЊ СЃР»СѓС‡Р°Р№РЅС‹Р№ РїРѕСЂСЏРґРѕРє?',
        'btn_shuffle_saved_once': 'РџРµСЂРµРјРµС€Р°С‚СЊ РѕРґРёРЅ СЂР°Р· Рё СЃРѕС…СЂР°РЅРёС‚СЊ',
        'btn_shuffle_every_run': 'РџРµСЂРµРјРµС€РёРІР°С‚СЊ РїСЂРё РєР°Р¶РґРѕР№ РїРѕРїС‹С‚РєРµ',
        'choose_translation_lang': 'РќР° РєР°РєРѕР№ СЏР·С‹Рє РїРµСЂРµРІРµСЃС‚Рё?',
        'pages_prompt': 'РљР°РєРѕР№ РґРёР°РїР°Р·РѕРЅ СЃС‚СЂР°РЅРёС† РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ? (1..{total})\nРџСЂРёРјРµСЂ: `20-30` РёР»Рё `20`',
        'pages_required': 'Р’С‹Р±РѕСЂ РґРёР°РїР°Р·РѕРЅР° СЃС‚СЂР°РЅРёС† РѕР±СЏР·Р°С‚РµР»РµРЅ.',
        'ai_partial': 'РџСЂРёРјРµС‡Р°РЅРёРµ: Р·Р°РїСЂРѕС€РµРЅРѕ {wanted}, СЃРѕР·РґР°РЅРѕ {made}.',
        'pages_invalid': 'РќРµРІРµСЂРЅС‹Р№ РґРёР°РїР°Р·РѕРЅ СЃС‚СЂР°РЅРёС†. РСЃРїРѕР»СЊР·СѓР№С‚Рµ 1..{total}. РџСЂРёРјРµСЂ: 20-30.',
        'current_pages': 'РўРµРєСѓС‰РёРµ СЃС‚СЂР°РЅРёС†С‹: {p_from}-{p_to}',
        'pages_set': 'РЎС‚СЂР°РЅРёС†С‹ СЃРѕС…СЂР°РЅРµРЅС‹: {p_from}-{p_to}',
        'pages_cleared': 'Р”РёР°РїР°Р·РѕРЅ СЃС‚СЂР°РЅРёС† РѕС‡РёС‰РµРЅ (РІСЃРµ СЃС‚СЂР°РЅРёС†С‹).',
        'ai_working': 'AI РіРѕС‚РѕРІРёС‚ С‚РµСЃС‚, РїРѕРґРѕР¶РґРёС‚Рµ...',
        'topic_line': 'РўРµРјР°: {topic}\n',
        'pages_line': 'РЎС‚СЂР°РЅРёС†С‹: {p_from}-{p_to}\n',
        'done_line': 'РС‚РѕРі: СЃС‚СЂР°РЅРёС†С‹ {p_from}-{p_to}, РІРѕРїСЂРѕСЃ {n}.\n',
        'ai_quiz_ready': 'РўРµСЃС‚ РіРѕС‚РѕРІ!\nРўРµСЃС‚: {title}\n{topic_line}Р’РѕРїСЂРѕСЃРѕРІ: {count}\nРќР° РІРѕРїСЂРѕСЃ: {sec}s (РїСЂРёРјРµСЂРЅРѕ {est})\nID: {id}\n\nРќР°Р¶РјРёС‚Рµ `РќР°С‡Р°С‚СЊ С‚РµСЃС‚`.',
        'err_ai': 'РћС€РёР±РєР°: {err}',
        'err_unexpected': 'РќРµРѕР¶РёРґР°РЅРЅР°СЏ РѕС€РёР±РєР°: {err}',
        'err_rate_limit': 'Р›РёРјРёС‚ Р·Р°РїСЂРѕСЃРѕРІ AI РёСЃС‡РµСЂРїР°РЅ. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РїРѕРІС‚РѕСЂРёС‚Рµ С‡РµСЂРµР· 1-2 РјРёРЅСѓС‚С‹.',
        'err_unexpected_safe': 'РќРµРѕР¶РёРґР°РЅРЅР°СЏ РѕС€РёР±РєР°. РџРѕРґРѕР¶РґРёС‚Рµ РЅРµРјРЅРѕРіРѕ Рё РїРѕРІС‚РѕСЂРёС‚Рµ РїРѕРїС‹С‚РєСѓ.',
        'no_input_for_ai': 'РќРµС‚ РґР°РЅРЅС‹С…. РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» РёР»Рё РёСЃРїРѕР»СЊР·СѓР№С‚Рµ /topic.',
        'session_missing': 'РЎРµСЃСЃРёСЏ РЅРµ РЅР°Р№РґРµРЅР°. РќР°С‡РЅРёС‚Рµ Р·Р°РЅРѕРІРѕ С‡РµСЂРµР· /menu.',
        'session_owner_only': 'РўРѕР»СЊРєРѕ РІР»Р°РґРµР»РµС† СЃРµСЃСЃРёРё РјРѕР¶РµС‚ РІС‹Р±РёСЂР°С‚СЊ.',
        'choose_one_count': 'Р’С‹Р±РµСЂРёС‚Рµ РѕРґРЅРѕ С‡РёСЃР»Рѕ РІРѕРїСЂРѕСЃРѕРІ: {options}',
        'choose_one_sec': 'Р’С‹Р±РµСЂРёС‚Рµ РѕРґРЅРѕ Р·РЅР°С‡РµРЅРёРµ СЃРµРєСѓРЅРґ: {options}',
        'current_topic': 'РўРµРєСѓС‰Р°СЏ С‚РµРјР°: {topic}',
        'count_invalid': 'РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РІС‹Р±РµСЂРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ РєРЅРѕРїРєР°РјРё.',
        'time_invalid': 'РћС‚РїСЂР°РІСЊС‚Рµ СЃРµРєСѓРЅРґС‹ С‡РёСЃР»РѕРј (5..600) РёР»Рё РЅР°Р¶РјРёС‚Рµ РѕРґРЅСѓ РёР· РєРЅРѕРїРѕРє 20/30/40/50.',
        'accepted': 'РџСЂРёРЅСЏС‚Рѕ.',
        'count_chosen': 'РљРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ: {n}.',
        'file_type_only': 'РћС‚РїСЂР°РІСЊС‚Рµ С‚РѕР»СЊРєРѕ .pdf, .docx, .pptx, .png, .jpg, .txt, .md РёР»Рё .json.',
        'file_too_large': 'Р¤Р°Р№Р» СЃР»РёС€РєРѕРј Р±РѕР»СЊС€РѕР№. РњР°РєСЃ: {mb} РњР‘.',
        'file_received_downloading': 'Р¤Р°Р№Р» РїРѕР»СѓС‡РµРЅ. РЎРєР°С‡РёРІР°СЋ...',
        'extracting_text': 'РР·РІР»РµРєР°СЋ С‚РµРєСЃС‚...',
        'text_too_short': 'РўРµРєСЃС‚ СЃР»РёС€РєРѕРј РєРѕСЂРѕС‚РєРёР№.\nРћС‚РїСЂР°РІСЊС‚Рµ TXT/DOCX РёР»Рё РїСЂРѕРІРµСЂСЊС‚Рµ PDF.',
        'scan_pdf_need_gemini': 'РџРѕС…РѕР¶Рµ, СЌС‚Рѕ СЃРєР°РЅ (РЅРµС‚ С‚РµРєСЃС‚Р°) РёР»Рё РёР·РѕР±СЂР°Р¶РµРЅРёРµ.\nР”Р»СЏ С‚РµСЃС‚Р° РїРѕ РєР°СЂС‚РёРЅРєР°Рј РЅСѓР¶РµРЅ vision: OpenAI РёР»Рё Gemini.\n`.env` -> `OPENAI_API_KEY=...` (РёР»Рё `GEMINI_API_KEY=...`) Рё `AI_PROVIDER=auto|openai|gemini`.',
        'scan_pdf_rendering': 'PDF СЃРєР°РЅ. РџСЂРµРѕР±СЂР°Р·СѓСЋ СЃС‚СЂР°РЅРёС†С‹ РІ РёР·РѕР±СЂР°Р¶РµРЅРёСЏ...',
        'scan_pdf_no_images': 'РќРµ РїРѕР»СѓС‡РёР»РѕСЃСЊ РёР·РІР»РµС‡СЊ РёР·РѕР±СЂР°Р¶РµРЅРёСЏ РёР· PDF. РћС‚РїСЂР°РІСЊС‚Рµ РґСЂСѓРіРѕР№ С„Р°Р№Р».',
        'scan_pdf_choose_count': 'PDF СЃРєР°РЅ: {pages} СЃС‚СЂР°РЅРёС†.\nРЎРґРµР»Р°СЋ РїРѕ 1 РІРѕРїСЂРѕСЃСѓ СЃ РєР°Р¶РґРѕР№ СЃС‚СЂР°РЅРёС†С‹.\nР’С‹Р±РµСЂРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ РєРЅРѕРїРєР°РјРё.',
        'cancelled': 'РћС‚РјРµРЅРµРЅРѕ.',
        'saved_short': 'РЎРѕС…СЂР°РЅРµРЅРѕ.',
        'stopped_n': 'РћСЃС‚Р°РЅРѕРІР»РµРЅРѕ Р°РєС‚РёРІРЅС‹С… С‚РµСЃС‚РѕРІ: {n}.',
        'invalid_button': 'РќРµРІРµСЂРЅР°СЏ РєРЅРѕРїРєР°.',
        'error_short': 'РћС€РёР±РєР°.',
        'rate_limited': 'РЎР»РёС€РєРѕРј С‡Р°СЃС‚Рѕ. РџРѕРґРѕР¶РґРёС‚Рµ {sec}СЃ.',
        'chat_not_found': 'Р§Р°С‚ РЅРµ РЅР°Р№РґРµРЅ.',
        'starting': 'Р—Р°РїСѓСЃРєР°СЋ...',
        'bot_username_missing': 'РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ username Р±РѕС‚Р°.',
        'share_link': 'РЎСЃС‹Р»РєР° РґР»СЏ С€Р°СЂРёРЅРіР°:\n{link}',
        'group_start_link': 'РЎСЃС‹Р»РєР° РґР»СЏ Р·Р°РїСѓСЃРєР° РІ РіСЂСѓРїРїРµ:\n{link}',
        'export_working': 'Р“РѕС‚РѕРІР»СЋ С„Р°Р№Р» С‚РµСЃС‚Р°...',
        'export_failed': 'РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ С„Р°Р№Р»: {err}',
        'export_docx_caption': 'Р¤Р°Р№Р» С‚РµСЃС‚Р° (.docx): {title}\\nID: {id}\\nРћС‚РІРµС‚С‹ РІ РєРѕРЅС†Рµ.',
        'export_answer_key_title': 'РћС‚РІРµС‚С‹',
        'export_sent_private_notice': 'Р¤Р°Р№Р» С‚РµСЃС‚Р° РѕС‚РїСЂР°РІР»РµРЅ РІ Р»РёС‡РЅС‹Р№ С‡Р°С‚.',
        'manual_title_prompt': 'Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ РЅРѕРІРѕРіРѕ С‚РµСЃС‚Р°:',
        'manual_title_required': 'Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ С‚РµСЃС‚Р°:',
        'manual_first_question': 'РћС‚РїСЂР°РІСЊС‚Рµ 1-Р№ РІРѕРїСЂРѕСЃ.\n- РњРѕР¶РЅРѕ С‚РµРєСЃС‚РѕРј\n- РР»Рё РєР°СЂС‚РёРЅРєСѓ СЃ РІРѕРїСЂРѕСЃРѕРј (РµСЃР»Рё РµСЃС‚СЊ РїРѕРґРїРёСЃСЊ, СЌС‚Рѕ Р±СѓРґРµС‚ С‚РµРєСЃС‚ РІРѕРїСЂРѕСЃР°)',
        'manual_image_not_found': 'РљР°СЂС‚РёРЅРєР° РЅРµ РЅР°Р№РґРµРЅР°. РћС‚РїСЂР°РІСЊС‚Рµ РµС‰Рµ СЂР°Р·.',
        'manual_send_4_options': 'РћС‚РїСЂР°РІСЊС‚Рµ 4 РІР°СЂРёР°РЅС‚Р° (РєР°Р¶РґС‹Р№ СЃ РЅРѕРІРѕР№ СЃС‚СЂРѕРєРё):',
        'manual_send_image_or_text': 'РћС‚РїСЂР°РІСЊС‚Рµ РєР°СЂС‚РёРЅРєСѓ (.jpg/.png/.webp) РёР»Рё РЅР°РїРёС€РёС‚Рµ С‚РµРєСЃС‚ РІРѕРїСЂРѕСЃР°.',
        'manual_send_question_or_image': 'РћС‚РїСЂР°РІСЊС‚Рµ С‚РµРєСЃС‚ РІРѕРїСЂРѕСЃР° РёР»Рё РєР°СЂС‚РёРЅРєСѓ.',
        'manual_has_image': 'Р•СЃС‚СЊ РєР°СЂС‚РёРЅРєР° Рє РІРѕРїСЂРѕСЃСѓ? РћС‚РїСЂР°РІСЊС‚Рµ РєР°СЂС‚РёРЅРєСѓ РёР»Рё РЅР°РїРёС€РёС‚Рµ /skip.',
        'manual_image_or_skip': 'РћС‚РїСЂР°РІСЊС‚Рµ РєР°СЂС‚РёРЅРєСѓ (.jpg/.png/.webp) РёР»Рё /skip.',
        'manual_image_or_skip_short': 'РћС‚РїСЂР°РІСЊС‚Рµ РєР°СЂС‚РёРЅРєСѓ РёР»Рё /skip.',
        'manual_need_4_lines': 'РќСѓР¶РЅРѕ СЂРѕРІРЅРѕ 4 РІР°СЂРёР°РЅС‚Р° (4 СЃС‚СЂРѕРєРё).',
        'manual_choose_correct': 'Р’С‹Р±РµСЂРёС‚Рµ РЅРѕРјРµСЂ РїСЂР°РІРёР»СЊРЅРѕРіРѕ РІР°СЂРёР°РЅС‚Р°:',
        'manual_callback_error': 'РћС€РёР±РєР° callback.',
        'manual_question_missing': 'Р”Р°РЅРЅС‹Рµ РІРѕРїСЂРѕСЃР° РЅРµ РЅР°Р№РґРµРЅС‹, РЅР°С‡РЅРёС‚Рµ Р·Р°РЅРѕРІРѕ С‡РµСЂРµР· /newquiz.',
        'manual_saved_total': 'Р’РѕРїСЂРѕСЃ СЃРѕС…СЂР°РЅРµРЅ. Р’СЃРµРіРѕ: {n}.',
        'manual_next_question': 'РћС‚РїСЂР°РІСЊС‚Рµ СЃР»РµРґСѓСЋС‰РёР№ РІРѕРїСЂРѕСЃ (С‚РµРєСЃС‚ РёР»Рё РєР°СЂС‚РёРЅРєР°):',
        'manual_empty': 'РўРµСЃС‚ РїСѓСЃС‚. РќР°С‡РЅРёС‚Рµ Р·Р°РЅРѕРІРѕ С‡РµСЂРµР· /newquiz.',
        'manual_created': 'РўРµСЃС‚ СЃРѕР·РґР°РЅ! ID: {id}. Р’РѕРїСЂРѕСЃРѕРІ: {n}.',
        'manual_draft_found': 'РЈ РІР°СЃ РµСЃС‚СЊ СЃРѕС…СЂР°РЅС‘РЅРЅС‹Р№ С‡РµСЂРЅРѕРІРёРє С‚РµСЃС‚Р°. РџСЂРѕРґРѕР»Р¶РёС‚СЊ?',
        'btn_manual_add_more': 'Р•С‰Рµ РІРѕРїСЂРѕСЃ',
        'btn_manual_finish': 'Р—Р°РІРµСЂС€РёС‚СЊ',
        'btn_manual_continue': 'РџСЂРѕРґРѕР»Р¶РёС‚СЊ',
        'btn_manual_restart': 'РќР°С‡Р°С‚СЊ Р·Р°РЅРѕРІРѕ',
        'bad_link': 'РќРµРІРµСЂРЅР°СЏ СЃСЃС‹Р»РєР°.',
        'stopped': 'РўРµСЃС‚ РѕСЃС‚Р°РЅРѕРІР»РµРЅ.',
        'image_question': 'Р’РѕРїСЂРѕСЃ СЃ РєР°СЂС‚РёРЅРєРѕР№',
        'explanation_prefix': 'РџРѕСЏСЃРЅРµРЅРёРµ: {text}',
        'choose_answer': 'Р’С‹Р±РµСЂРёС‚Рµ РѕС‚РІРµС‚',
        'importing_quiz': 'РРјРїРѕСЂС‚РёСЂСѓСЋ С‚РµСЃС‚...',
        'import_failed': 'РќРµ СѓРґР°Р»РѕСЃСЊ РёРјРїРѕСЂС‚РёСЂРѕРІР°С‚СЊ С‚РµСЃС‚ РёР· С„Р°Р№Р»Р°. РџСЂРѕРІРµСЂСЊС‚Рµ С„РѕСЂРјР°С‚.',
        'import_ok': 'РўРµСЃС‚ РёРјРїРѕСЂС‚РёСЂРѕРІР°РЅ!\nРўРµСЃС‚: {title}\nР’РѕРїСЂРѕСЃРѕРІ: {n}\nID: {id}',
        'import_partial': 'Р’ С„Р°Р№Р»Рµ РЅР°Р№РґРµРЅРѕ {found} РІРѕРїСЂРѕСЃРѕРІ (Р·Р°РїСЂРѕС€РµРЅРѕ: {wanted}). РЈРІРµР»РёС‡СЊС‚Рµ РґРёР°РїР°Р·РѕРЅ СЃС‚СЂР°РЅРёС† РёР»Рё РїСЂРѕРІРµСЂСЊС‚Рµ С„РѕСЂРјР°С‚.',
        'edit_menu': 'РўРµСЃС‚: {title}\nР’РѕРїСЂРѕСЃРѕРІ: {count}\nРќР° РІРѕРїСЂРѕСЃ: {sec}s\nID: {id}\n\nР§С‚Рѕ СЂРµРґР°РєС‚РёСЂСѓРµРј?',
        'edit_answers_choose_question': 'Р РµРґР°РєС‚РёСЂРѕРІР°РЅРёРµ РѕС‚РІРµС‚РѕРІ: РІС‹Р±РµСЂРёС‚Рµ РЅРѕРјРµСЂ РІРѕРїСЂРѕСЃР° ({count}).',
        'edit_answers_choose_correct': 'Р’С‹Р±РµСЂРёС‚Рµ РЅРѕРјРµСЂ РїСЂР°РІРёР»СЊРЅРѕРіРѕ РѕС‚РІРµС‚Р°:',
        'edit_answers_updated': 'РЎРѕС…СЂР°РЅРµРЅРѕ.',
        'edit_title_prompt': 'РћС‚РїСЂР°РІСЊС‚Рµ РЅРѕРІРѕРµ РЅР°Р·РІР°РЅРёРµ:',
        'edit_time_prompt': 'РћС‚РїСЂР°РІСЊС‚Рµ РЅРѕРІРѕРµ РІСЂРµРјСЏ РёР»Рё РІС‹Р±РµСЂРёС‚Рµ 20/30/40/50 СЃРµРєСѓРЅРґ (5..600):',
        'edit_saved': 'РЎРѕС…СЂР°РЅРµРЅРѕ.',
        'edit_creator_only': 'Р РµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РјРѕР¶РµС‚ С‚РѕР»СЊРєРѕ Р°РІС‚РѕСЂ С‚РµСЃС‚Р°.',
        'choose_difficulty': 'Р’С‹Р±РµСЂРёС‚Рµ СЃР»РѕР¶РЅРѕСЃС‚СЊ:',
        'difficulty_invalid': 'РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РІС‹Р±РµСЂРёС‚Рµ СЃР»РѕР¶РЅРѕСЃС‚СЊ: Р›РµРіРєРѕ | РЎСЂРµРґРЅРµ | РЎР»РѕР¶РЅРѕ | РЎРјРµС€Р°РЅРѕ',
        'btn_diff_easy': 'Р›РµРіРєРѕ',
        'btn_diff_medium': 'РЎСЂРµРґРЅРµ',
        'btn_diff_hard': 'РЎР»РѕР¶РЅРѕ',
        'btn_diff_mixed': 'РЎРјРµС€Р°РЅРѕ (РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ)',
        'btn_premium': 'РџСЂРµРјРёСѓРј',
        'premium_choose_plan': 'Р’С‹Р±РµСЂРёС‚Рµ РїСЂРµРјРёСѓРј-РїР°РєРµС‚:',
        'premium_status_free': 'Р‘РµСЃРїР»Р°С‚РЅР°СЏ СЂР°Р·РѕРІР°СЏ РєРІРѕС‚Р°: Р¤Р°Р№Р»С‹ {f_left}/{f_total}, РўРµРјС‹ {t_left}/{t_total}\\nРћРєРЅРѕ: {days} РґРµРЅСЊ (РёСЃС‚РµРєР°РµС‚: {until})',
        'premium_status_premium': 'РџСЂРµРјРёСѓРј Р°РєС‚РёРІРµРЅ (РґРѕ: {until})\\nР¤Р°Р№Р»С‹: {f_left}/{f_total}\\nРўРµРјС‹: {t_left}/{t_total}',
        'premium_send_screenshot': 'РћС‚РїСЂР°РІСЊС‚Рµ С‡РµРє РёР»Рё СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹. РџРѕРґС…РѕРґРёС‚ РёР·РѕР±СЂР°Р¶РµРЅРёРµ, PDF РёР»Рё С‚РµРєСЃС‚ shared receipt.',
        'receipt_checking': 'РџСЂРѕРІРµСЂСЏСЋ С‡РµРє...',
        'premium_received': 'РџСЂРёРЅСЏС‚Рѕ. РђРґРјРёРЅ РїСЂРѕРІРµСЂРёС‚ Рё Р°РєС‚РёРІРёСЂСѓРµС‚ РїСЂРµРјРёСѓРј.',
        'premium_need_image': 'РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ С‡РµРє РёР»Рё СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹. РџРѕРґС…РѕРґРёС‚ РёР·РѕР±СЂР°Р¶РµРЅРёРµ, PDF РёР»Рё С‚РµРєСЃС‚ shared receipt.',
        'limit_free_reached': 'Р‘РµСЃРїР»Р°С‚РЅР°СЏ РєРІРѕС‚Р° Р·Р°РєРѕРЅС‡РёР»Р°СЃСЊ. РљСѓРїРёС‚Рµ РїСЂРµРјРёСѓРј.',
        'limit_premium_reached': 'РџСЂРµРјРёСѓРј-РєРІРѕС‚Р° Р·Р°РєРѕРЅС‡РёР»Р°СЃСЊ. РџСЂРµРјРёСѓРј РґРѕ: {until}\\nРљСѓРїРёС‚Рµ РїР°РєРµС‚, С‡С‚РѕР±С‹ РїСЂРѕРґР»РёС‚СЊ.',
        'premium_plan_1d': '1 РґРµРЅСЊ',
        'premium_plan_7d': '7 РґРЅРµР№',
        'premium_plan_30d': '1 РјРµСЃСЏС†',
        'btn_premium_approve': 'РћРґРѕР±СЂРёС‚СЊ',
        'btn_premium_reject': 'РћС‚РєР»РѕРЅРёС‚СЊ',
        'premium_approved_user': 'РџСЂРµРјРёСѓРј Р°РєС‚РёРІРёСЂРѕРІР°РЅ. Р”Рѕ: {until}',
        'premium_rejected_user': 'Р—Р°РїСЂРѕСЃ РЅР° РїСЂРµРјРёСѓРј РѕС‚РєР»РѕРЅРµРЅ.',
        'admin_only': 'РўРѕР»СЊРєРѕ РґР»СЏ Р°РґРјРёРЅРѕРІ.',
        'topic_too_long': 'РўРµРјР° СЃР»РёС€РєРѕРј РґР»РёРЅРЅР°СЏ. РњР°РєСЃ: {n} СЃРёРјРІРѕР»РѕРІ.',
        'btn_pay': 'РћРїР»Р°С‚РёС‚СЊ',
        'payment_card_missing': 'РљР°СЂС‚Р° РґР»СЏ РѕРїР»Р°С‚С‹ РЅРµ РЅР°СЃС‚СЂРѕРµРЅР°. РћР±СЂР°С‚РёС‚РµСЃСЊ Рє Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂСѓ.',
        'payment_card_info': 'РљР°СЂС‚Р° РґР»СЏ РѕРїР»Р°С‚С‹:\\n{card}\\nР”РµСЂР¶Р°С‚РµР»СЊ: {holder}\\n\\nРџР°РєРµС‚: {plan}\\nР¦РµРЅР°: {price} UZS\\n\\nРћРїР»Р°С‚РёС‚Рµ Рё РѕС‚РїСЂР°РІСЊС‚Рµ С‡РµРє/СЃРєСЂРёРЅС€РѕС‚.',
        'premium_plan_details': 'Р’С‹Р±СЂР°РЅРЅС‹Р№ РїР°РєРµС‚: {plan}\\nР”Р»РёС‚РµР»СЊРЅРѕСЃС‚СЊ: {days} РґРµРЅСЊ\\nР¦РµРЅР°: {price} UZS\\nРЎРєРёРґРєР°: -{disc}%\\n\\nР›РёРјРёС‚С‹: Р¤Р°Р№Р»С‹ {files}, РўРµРјС‹ {topics}\\n\\nРќР°Р¶РјРёС‚Рµ `РћРїР»Р°С‚РёС‚СЊ`, С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.',
    },
    'en': {
        'ref_rewarded': "Referral bonus granted: +{files} files, +{topics} topics.",
        'ref_progress': "Referral: {n}/3 (need {need} more).",
        'ref_info': "Referral link:\n{link}\n\nStatus:\n- Total starts: {total}\n- Joined channel: {qualified}\n- Pending: {pending}\n\nBonus: when 3 new users join the channel, you get +2 file uploads and +1 topic quiz.\nTo next bonus: {to_next}",
        'btn_referral': "Referral",
        'menu': 'Menu:',
        'about_menu': 'Menu:',
        'menu_help': 'Menu sections:\n- Upload file: send a file or an e-book and the bot will create a quiz.\n- Quiz by topic: type a topic, choose difficulty/count/time and get a quiz.\n- New quiz: create a quiz manually by adding questions and options.\n- Interface language: change the language of buttons and messages.\n- Premium: increase limits and manage premium access.',
        'must_join_channel': 'To use the bot, join the channel: {channel}\\nThen tap "Check".',
        'sub_required_alert': 'Join the channel to use the bot.',
        'sub_check_fail': 'You are not subscribed to the channel yet.',
        'sub_check_ok': 'Thanks! You can use the bot now.',
        'btn_join_channel': 'Join channel',
        'btn_check_sub': 'Check',
        'ui_lang_choose': 'Choose UI language:',
        'ui_lang_saved': 'Saved. UI language: {lang_name}',
        'ai_lang_choose': 'Choose AI translation language (default):',
        'ai_lang_saved': 'Default AI translation language: {lang_name}',
        'upload_hint': 'Send a file (.pdf / .docx / .pptx / .txt).\nFor PDF/PPTX, choosing a page range is required.',
        'upload_hint_noai': 'Send a quiz file (.txt / .json / .docx / .pptx / .pdf).\nThe file must already contain questions: question + A/B/C/D options + Answer.\nExample:\n1) Question?\nA) ...\nB) ...\nC) ...\nD) ...\nAnswer: B',
        'topic_prompt': 'Which topic should I generate a quiz for?\nType the topic.\nExample: `SQL JOIN`',
        'file_topic_prompt': 'Optional: which topic inside the uploaded file should I focus on?\nType the topic.\nExample: `SQL JOIN`',
        'topic_searching': 'Searching for a source (book/article)...',
        'topic_source_found': 'Source found: {title}\nNow choose quiz settings.',
        'topic_source_not_found': "Couldn't find a reliable source. For best results, upload the book file (.pdf/.docx/.txt) or be more specific.",
        'btn_topic_continue_anyway': 'Continue anyway',
        'quiz_not_found': 'Quiz not found.',
        'quiz_card': 'Quiz: {title}\nQuestions: {count}\nPer question: {sec}s\nID: {id}',
        'quiz_brief': 'Quiz: {title}\nQuestions: {count}\nID: {id}',
        'quiz_no_questions': 'This quiz has no questions.',
        'no_quizzes_yet': "You don't have any quizzes yet. Send a file or create a new quiz.",
        'more_quizzes': '{n} more quiz(zes).',
        'chat_has_active_quiz': 'This chat already has an active quiz. Wait for it to finish or ask the owner to /cancel.',
        'quiz_started_private': 'Quiz started: {count} questions, {sec}s each (about {est}). Quiz ID: {quiz_id}\nMoves to the next question right after you answer.\nSpeed up: Next button. Stop: /cancel',
        'quiz_finished': 'Quiz finished.',
        'quiz_paused_inactive': 'You missed 3 questions in a row. Quiz paused.\nPress Resume to continue.',
        'quiz_resumed_private': 'Quiz resumed. Remaining: {remaining} questions, {sec}s each (about {est}).',
        'quiz_stopped_no_participants': 'All participants became inactive (3 missed questions). Quiz stopped.',
        'resume_not_found': 'Resume session not found. Start the quiz again.',
        'resumed_short': 'Resumed.',
        'stats_title': 'Statistics: {title}',
        'stats_no_attempts': 'No one has taken this quiz yet.',
        'stats_creator_only': 'Only the quiz owner can view statistics.',
        'scoreboard_title': 'Results:',
        'participants_joined': 'Participants (Join): {n}',
        'total_questions': 'Total questions: {n}',
        'skipped': 'skipped {n}',
        'scoreboard_more': '... and {n} more users',
        'scoreboard_row': '{i}) {name}: {correct}/{answered} correct, time {t}s, avg {avg}s{extra}',
        'lobby_ready': 'Quiz ready: {total} questions, {sec}s each.\nPress `Join` to participate.\nIf all joined users answer, the next question shows immediately.\nParticipants: {n}.',
        'quiz_id_line': 'Quiz ID: {id}',
        'lobby_joined': 'Joined: {name}',
        'lobby_already_joined': 'You already joined.',
        'lobby_not_found': 'Quiz not found.',
        'quiz_cancelled': 'Quiz was cancelled.',
        'cannot_join_started': "The quiz has started; you can't join now.",
        'joined_ok': 'Joined.',
        'already_started': 'Already started.',
        'started_ok': 'Starting.',
        'group_started_status': 'Quiz started: {count} questions, {sec}s each (about {est}).\nParticipants: {n}.\nSpeed up: Next (creator only). Stop: /cancel',
        'lobby_creator_only': 'Creator only.',
        'quiz_not_started': "The quiz hasn't started yet.",
        'next_question': 'Next question.',
        'lobby_started': 'Quiz started.',
        'btn_join': 'Join',
        'btn_start': 'Start',
        'btn_stop': 'Stop',
        'btn_next': 'Next',
        'btn_resume': 'Resume',
        'btn_upload': 'Upload file',
        'btn_topic': 'Topic quiz',
        'btn_newquiz': 'New quiz',
        'btn_mytests': 'My quizzes',
        'btn_cancel_active': 'Stop active quiz',
        'btn_ui_lang': 'Interface language',
        'btn_ai_lang': 'AI translation language',
        'btn_start_quiz': 'Start quiz',
        'btn_retry_quiz': 'Try again',
        'btn_share_quiz': 'Share quiz',
        'btn_start_group': 'Start in group',
        'btn_stats': 'рџ“Љ Statistics',
        'btn_edit_quiz': 'Edit',
        'btn_export_docx': 'Quiz file (.docx)',
        'btn_edit_title': 'Edit title',
        'btn_edit_time': 'Edit time',
        'btn_edit_answers': 'Edit answers',
        'btn_prev_page': 'в¬…пёЏ Prev',
        'btn_next_page': 'Next вћЎпёЏ',
        'btn_back': 'Back',
        'btn_cancel': 'Cancel',
        'btn_topic_optional': 'Topic (opt.)',
        'btn_pages_optional': 'Pages',
        'btn_no_translate': 'No translation (source)',
        'lang_source': 'Source (no translation)',
        'btn_translate_default': 'Translate (default: {name})',
        'btn_translate_choose': 'Other language',
        'btn_translate': 'Translate',
        'ai_disabled': 'AI features are disabled. (BOT_MODE=noai)',
        'choose_count': 'Choose the question count using the buttons.',
        'choose_time': 'Seconds per question? Tap one of the 20/30/40/50/60/70 buttons.',
        'need_translation': 'Need translation?',
        'shuffle_prompt_ai': 'Choose the shuffle mode:',
        'shuffle_prompt_manual': 'Choose the shuffle mode for this quiz:',
        'btn_shuffle_questions': 'Questions only',
        'btn_shuffle_answers': 'Answers only',
        'btn_shuffle_both': 'Both',
        'btn_shuffle_keep': 'Keep order',
        'shuffle_strategy_prompt': 'How should random order behave?',
        'btn_shuffle_saved_once': 'Shuffle once and save',
        'btn_shuffle_every_run': 'Reshuffle every attempt',
        'choose_translation_lang': 'Which language should I translate to?',
        'pages_prompt': 'Which page range should I use? (1..{total})\nExample: `20-30` or `20`',
        'pages_required': 'Page range selection is required.',
        'ai_partial': 'Note: requested {wanted}, created {made}.',
        'pages_invalid': 'Invalid page range. Use 1..{total}. Example: 20-30.',
        'current_pages': 'Current pages: {p_from}-{p_to}',
        'pages_set': 'Pages saved: {p_from}-{p_to}',
        'pages_cleared': 'Page range cleared (all pages).',
        'ai_working': 'AI is generating the quiz, please wait...',
        'topic_line': 'Topic: {topic}\n',
        'pages_line': 'Pages: {p_from}-{p_to}\n',
        'done_line': 'Done: pages {p_from}-{p_to}, question {n}.\n',
        'ai_quiz_ready': 'Quiz is ready!\nQuiz: {title}\n{topic_line}Questions: {count}\nPer question: {sec}s (about {est})\nID: {id}\n\nPress `Start quiz`.',
        'err_ai': 'Error: {err}',
        'err_unexpected': 'Unexpected error: {err}',
        'err_rate_limit': 'AI service rate limit reached. Please try again in 1-2 minutes.',
        'err_unexpected_safe': 'An unexpected error occurred. Please wait a moment and try again.',
        'no_input_for_ai': 'No input found. Send a file or use /topic.',
        'session_missing': 'Session not found. The bot may have restarted or you might be running two instances. Start again from /menu.',
        'session_owner_only': 'Only the session owner can choose.',
        'choose_one_count': 'Please choose a single question count: {options}',
        'choose_one_sec': 'Please choose a single seconds value: {options}',
        'current_topic': 'Current topic: {topic}',
        'count_invalid': 'Please choose the question count using the buttons.',
        'time_invalid': 'Please choose only one of the 20/30/40/50/60/70 buttons.',
        'accepted': 'Accepted.',
        'count_chosen': 'Question count: {n}.',
        'file_type_only': 'Only .pdf, .docx, .pptx, .png, .jpg, .txt, .md or .json files are supported.',
        'file_too_large': 'File is too large. Max: {mb} MB.',
        'file_received_downloading': 'File received. Downloading...',
        'extracting_text': 'Extracting text...',
        'text_too_short': 'Text is too short.\nSend TXT/DOCX or check if the PDF has selectable text.',
        'scan_pdf_need_gemini': 'This looks like a scan (no text) or an image.\nImage-based quiz requires vision: OpenAI or Gemini.\n`.env` -> `OPENAI_API_KEY=...` (or `GEMINI_API_KEY=...`) and `AI_PROVIDER=auto|openai|gemini`.',
        'scan_pdf_rendering': 'Scanned PDF. Rendering pages to images...',
        'scan_pdf_no_images': "Couldn't extract images from the PDF. Send another file.",
        'scan_pdf_choose_count': "Scanned PDF: {pages} pages.\nI'll make 1 question per page.\nChoose the question count using the buttons.",
        'cancelled': 'Cancelled.',
        'saved_short': 'Saved.',
        'stopped_n': 'Stopped active quizzes: {n}.',
        'invalid_button': 'Invalid button.',
        'error_short': 'Error.',
        'rate_limited': 'Too many requests. Wait {sec}s.',
        'chat_not_found': 'Chat not found.',
        'starting': 'Starting...',
        'bot_username_missing': 'Bot username not found.',
        'share_link': 'Share link:\n{link}',
        'group_start_link': 'Start in group link:\n{link}',
        'export_working': 'Generating quiz file...',
        'export_failed': 'Failed to create file: {err}',
        'export_docx_caption': 'Quiz file (.docx): {title}\\nID: {id}\\nAnswers are at the end.',
        'export_answer_key_title': 'Answer key',
        'export_sent_private_notice': 'Quiz file was sent to your private chat.',
        'manual_title_prompt': 'Enter the new quiz title:',
        'manual_title_required': 'Please enter a quiz title:',
        'manual_first_question': 'Send question 1.\n- You can type text\n- Or send an image (caption will be used as the question text)',
        'manual_image_not_found': 'Image not found. Please send again.',
        'manual_send_4_options': 'Send 4 options (each on a new line):',
        'manual_send_image_or_text': 'Send an image (.jpg/.png/.webp) or type the question text.',
        'manual_send_question_or_image': 'Send the question text or an image.',
        'manual_has_image': 'Does the question have an image? Send it or type /skip.',
        'manual_image_or_skip': 'Send an image (.jpg/.png/.webp) or /skip.',
        'manual_image_or_skip_short': 'Send an image or /skip.',
        'manual_need_4_lines': 'Please send exactly 4 options (4 lines).',
        'manual_choose_correct': 'Choose the correct option number:',
        'manual_callback_error': 'Callback error.',
        'manual_question_missing': 'Question data not found. Start again with /newquiz.',
        'manual_saved_total': 'Question saved. Total: {n}.',
        'manual_next_question': 'Send the next question (text or image):',
        'manual_empty': 'The quiz is empty. Start again with /newquiz.',
        'manual_created': 'Quiz created! ID: {id}. Questions: {n}.',
        'manual_draft_found': 'You have a saved manual quiz draft. Continue?',
        'btn_manual_add_more': 'Add question',
        'btn_manual_finish': 'Finish',
        'btn_manual_continue': 'Continue',
        'btn_manual_restart': 'Start over',
        'bad_link': 'Invalid link.',
        'stopped': 'Quiz stopped.',
        'image_question': 'Image question',
        'explanation_prefix': 'Explanation: {text}',
        'choose_answer': 'Choose an answer',
        'importing_quiz': 'Importing the quiz...',
        'import_failed': "Couldn't import the quiz from the file. Please check the format.",
        'import_ok': 'Quiz imported!\nQuiz: {title}\nQuestions: {n}\nID: {id}',
        'import_partial': 'Found {found} questions in the file (requested: {wanted}). Expand the page range or check the format.',
        'edit_menu': 'Quiz: {title}\nQuestions: {count}\nPer question: {sec}s\nID: {id}\n\nWhat do you want to edit?',
        'edit_answers_choose_question': 'Edit answers: choose a question number ({count}).',
        'edit_answers_choose_correct': 'Choose the correct answer number:',
        'edit_answers_updated': 'Saved.',
        'edit_title_prompt': 'Send the new title:',
        'edit_time_prompt': 'Choose the new time using 20/30/40/50/60/70 buttons:',
        'edit_saved': 'Saved.',
        'edit_creator_only': 'Only the quiz creator can edit this quiz.',
        'choose_difficulty': 'Choose difficulty:',
        'difficulty_invalid': 'Please choose a difficulty: Easy | Medium | Hard | Mixed',
        'btn_diff_easy': 'Easy',
        'btn_diff_medium': 'Medium',
        'btn_diff_hard': 'Hard',
        'btn_diff_mixed': 'Mixed (default)',
        'btn_premium': 'Premium',
        'premium_choose_plan': 'Choose a Premium plan:',
        'premium_status_free': 'Free one-time quota: Files {f_left}/{f_total}, Topics {t_left}/{t_total}\nWindow: {days} day (expires: {until})',
        'premium_status_premium': 'Premium active (until: {until})\nFiles: {f_left}/{f_total}\nTopics: {t_left}/{t_total}',
        'premium_send_screenshot': 'Send the payment receipt or screenshot. Image, PDF, or shared receipt text is accepted.',
        'receipt_checking': 'Checking the receipt...',
        'premium_received': 'Received. Admin will review and activate Premium.',
        'premium_need_image': 'Send the payment receipt or screenshot. Image, PDF, or shared receipt text is accepted.',
        'limit_free_reached': 'Free quota is finished. Buy Premium.',
        'limit_premium_reached': 'Premium quota is finished. Premium until: {until}\nBuy a plan to extend it.',
        'premium_plan_1d': '1 day',
        'premium_plan_7d': '7 days',
        'premium_plan_30d': '1 month',
        'btn_premium_approve': 'Approve',
        'btn_premium_reject': 'Reject',
        'premium_approved_user': 'Premium activated. Until: {until}',
        'premium_rejected_user': 'Premium request was rejected.',
        'admin_only': 'Admins only.',
        'btn_admin_users': 'Bot statistics',
        'admin_users_stats': 'Bot statistics\n\nTotal users: {total}\nJoined in the last 24 hours: {joined}\nActive users in the last 24 hours: {active}\nTotal quizzes: {quizzes}\nQuiz attempts in the last 24 hours: {attempts}',
        'topic_too_long': 'Topic is too long. Max: {n} characters.',
        'btn_pay': 'Pay',
        'payment_card_missing': 'Payment card is not configured. Contact admin.',
        'payment_card_info': 'Payment card:\n{card}\nHolder: {holder}\n\nPlan: {plan}\nPrice: {price} UZS\n\nPay and then send the receipt or screenshot.',
        'premium_plan_details': 'Selected plan: {plan}\nDuration: {days} day\nPrice: {price} UZS\nDiscount: -{disc}%\n\nLimits: Files {files}, Topics {topics}\n\nPress `Pay` to continue.',
    },
    'de': {},
    'tr': {},
    'kk': {},
    'ar': {},
    'zh': {},
    'ko': {},
}


# --- Load extra UI locales from JSON files (handlers/utils/locales/*.json) ---

def _load_json_locales() -> None:
    locales_dir = Path(__file__).resolve().parent / 'locales'
    if not locales_dir.exists():
        return
    for code in ('de', 'tr', 'kk', 'ar', 'zh', 'ko'):
        fp = locales_dir / f'{code}.json'
        if not fp.exists():
            continue
        data = None
        try:
            data = json.loads(fp.read_text(encoding='utf-8'))
        except Exception:
            try:
                data = json.loads(fp.read_text(encoding='utf-8-sig'))
            except Exception:
                data = None
        if isinstance(data, dict):
            _STRINGS[code] = {str(k): str(v) for k, v in data.items()}


_load_json_locales()


_STRING_OVERRIDES: Dict[str, Dict[str, str]] = {
    "uz": {
        "topic_prompt": "Qaysi mavzu asosida test tuzishni xohlaysiz?\nMavzuni kiriting.\nMisol: `Davlatlar poytaxtlari`",
        "file_topic_prompt": "Yuklangan fayl ichidan qaysi mavzuga e'tibor qaratishni xohlaysiz? (ixtiyoriy)\nMavzuni kiriting.\nMisol: `Davlatlar poytaxtlari`",
        "choose_translation_lang": "Qaysi tilga tarjima qilishni xohlaysiz?",
        "pages_prompt": "Qaysi sahifalar oralig'i asosida test tuzishni xohlaysiz? (1..{total})\nMisol: `20-30` yoki `20`",
        "scan_pdf_choose_count": "PDF skan: {pages} ta sahifa.\nHar bir sahifadan 1 tadan savol tuzishim mumkin.\nSavol sonini tugmalardan tanlang.",
        "premium_choose_plan": "Premium tariflardan birini tanlang:",
        "premium_status_free": "Bepul tarif (bir martalik):\nFayl limiti: {f_left}/{f_total}\nMavzu limiti: {t_left}/{t_total}\nAmal qilish muddati: {days} kun (tugash: {until})",
        "premium_status_premium": "Premium tarif faol.\nAmal qilish muddati: {until}\nFayl limiti: {f_left}/{f_total}\nMavzu limiti: {t_left}/{t_total}",
        "premium_send_screenshot": "Iltimos, to'lov cheki yoki skrinshotini yuboring. Rasm, PDF yoki ulashilgan chek matni qabul qilinadi.",
        "receipt_checking": "To'lov cheki tekshirilmoqda...",
        "premium_received": "So'rovingiz qabul qilindi. Administrator to'lovni tekshirgach, Premium tarifni faollashtiradi.",
        "premium_need_image": "Iltimos, to'lov cheki yoki skrinshotini yuboring. Rasm, PDF yoki ulashilgan chek matni qabul qilinadi.",
        "limit_free_reached": "Bepul limit yakunlandi. Davom etish uchun Premium tarifni tanlang.",
        "limit_premium_reached": "Premium limitdan foydalanib bo'lindi.\nPremium amal qilish muddati: {until}\nDavom etish uchun tarifni uzaytiring.",
        "premium_approved_user": "Premium tarif faollashtirildi.\nAmal qilish muddati: {until}",
        "premium_rejected_user": "Premium bo'yicha so'rovingiz rad etildi.",
        "payment_card_missing": "To'lov kartasi ma'lumotlari sozlanmagan. Iltimos, administrator bilan bog'laning.",
        "payment_card_info": "To'lov uchun karta ma'lumotlari:\n{card}\nKarta egasi: {holder}\n\nTanlangan tarif: {plan}\nTo'lov summasi: {price} UZS\n\nTo'lovni amalga oshirgach, chek yoki skrinshot yuboring.",
        "premium_plan_details": "Tanlangan tarif: {plan}\nDavomiyligi: {days} kun\nNarxi: {price} UZS\nChegirma: -{disc}%\n\nLimitlar:\nFayl: {files}\nMavzu: {topics}\n\nDavom etish uchun `To'lov qilish` tugmasini bosing.",
    },
    "ru": {
        "topic_prompt": "РџРѕ РєР°РєРѕР№ С‚РµРјРµ РІС‹ С…РѕС‚РёС‚Рµ СЃРѕР·РґР°С‚СЊ С‚РµСЃС‚?\nРќР°РїРёС€РёС‚Рµ С‚РµРјСѓ.\nРџСЂРёРјРµСЂ: `РЎС‚РѕР»РёС†С‹ РіРѕСЃСѓРґР°СЂСЃС‚РІ`",
        "file_topic_prompt": "РќР° РєР°РєРѕР№ С‚РµРјРµ РІРЅСѓС‚СЂРё Р·Р°РіСЂСѓР¶РµРЅРЅРѕРіРѕ С„Р°Р№Р»Р° РІС‹ С…РѕС‚РёС‚Рµ СЃРѕСЃСЂРµРґРѕС‚РѕС‡РёС‚СЊСЃСЏ? (РЅРµРѕР±СЏР·Р°С‚РµР»СЊРЅРѕ)\nРќР°РїРёС€РёС‚Рµ С‚РµРјСѓ.\nРџСЂРёРјРµСЂ: `РЎС‚РѕР»РёС†С‹ РіРѕСЃСѓРґР°СЂСЃС‚РІ`",
        "choose_translation_lang": "РќР° РєР°РєРѕР№ СЏР·С‹Рє РІС‹ С…РѕС‚РёС‚Рµ РїРµСЂРµРІРµСЃС‚Рё С‚РµСЃС‚?",
        "pages_prompt": "РџРѕ РєР°РєРѕРјСѓ РґРёР°РїР°Р·РѕРЅСѓ СЃС‚СЂР°РЅРёС† РІС‹ С…РѕС‚РёС‚Рµ СЃРѕР·РґР°С‚СЊ С‚РµСЃС‚? (1..{total})\nРџСЂРёРјРµСЂ: `20-30` РёР»Рё `20`",
        "scan_pdf_choose_count": "РЎРєР°РЅ PDF: {pages} СЃС‚СЂ.\nРЇ РјРѕРіСѓ СЃРѕР·РґР°С‚СЊ РїРѕ 1 РІРѕРїСЂРѕСЃСѓ СЃ РєР°Р¶РґРѕР№ СЃС‚СЂР°РЅРёС†С‹.\nР’С‹Р±РµСЂРёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ РєРЅРѕРїРєР°РјРё.",
        "premium_choose_plan": "Р’С‹Р±РµСЂРёС‚Рµ РѕРґРёРЅ РёР· РїСЂРµРјРёСѓРј-С‚Р°СЂРёС„РѕРІ:",
        "premium_status_free": "Р‘РµСЃРїР»Р°С‚РЅС‹Р№ С‚Р°СЂРёС„ (РѕРґРЅРѕСЂР°Р·РѕРІС‹Р№):\nР›РёРјРёС‚ РїРѕ С„Р°Р№Р»Р°Рј: {f_left}/{f_total}\nР›РёРјРёС‚ РїРѕ С‚РµРјР°Рј: {t_left}/{t_total}\nРЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ: {days} РґРЅ. (РґРѕ: {until})",
        "premium_status_premium": "РџСЂРµРјРёСѓРј-С‚Р°СЂРёС„ Р°РєС‚РёРІРµРЅ.\nРЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ: {until}\nР›РёРјРёС‚ РїРѕ С„Р°Р№Р»Р°Рј: {f_left}/{f_total}\nР›РёРјРёС‚ РїРѕ С‚РµРјР°Рј: {t_left}/{t_total}",
        "premium_send_screenshot": "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ С‡РµРє РёР»Рё СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹. РџРѕРґРѕР№РґСѓС‚ РёР·РѕР±СЂР°Р¶РµРЅРёРµ, PDF РёР»Рё С‚РµРєСЃС‚ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ РїРµСЂРµРІРѕРґР°.",
        "receipt_checking": "Р§РµРє РїСЂРѕРІРµСЂСЏРµС‚СЃСЏ...",
        "premium_received": "Р’Р°С€ Р·Р°РїСЂРѕСЃ РїСЂРёРЅСЏС‚. РџРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё РѕРїР»Р°С‚С‹ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂ Р°РєС‚РёРІРёСЂСѓРµС‚ РїСЂРµРјРёСѓРј-С‚Р°СЂРёС„.",
        "premium_need_image": "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ С‡РµРє РёР»Рё СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹. РџРѕРґРѕР№РґСѓС‚ РёР·РѕР±СЂР°Р¶РµРЅРёРµ, PDF РёР»Рё С‚РµРєСЃС‚ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ РїРµСЂРµРІРѕРґР°.",
        "limit_free_reached": "Р‘РµСЃРїР»Р°С‚РЅС‹Р№ Р»РёРјРёС‚ РёСЃС‡РµСЂРїР°РЅ. Р§С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ, РІС‹Р±РµСЂРёС‚Рµ РїСЂРµРјРёСѓРј-С‚Р°СЂРёС„.",
        "limit_premium_reached": "РџСЂРµРјРёСѓРј-Р»РёРјРёС‚ РёСЃС‡РµСЂРїР°РЅ.\nРџСЂРµРјРёСѓРј РґРµР№СЃС‚РІСѓРµС‚ РґРѕ: {until}\nР§С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ, РїСЂРѕРґР»РёС‚Рµ С‚Р°СЂРёС„.",
        "premium_approved_user": "РџСЂРµРјРёСѓРј-С‚Р°СЂРёС„ Р°РєС‚РёРІРёСЂРѕРІР°РЅ.\nР”РµР№СЃС‚РІСѓРµС‚ РґРѕ: {until}",
        "premium_rejected_user": "Р’Р°С€ Р·Р°РїСЂРѕСЃ РЅР° РїРѕРґРєР»СЋС‡РµРЅРёРµ РїСЂРµРјРёСѓРј-С‚Р°СЂРёС„Р° Р±С‹Р» РѕС‚РєР»РѕРЅРµРЅ.",
        "payment_card_missing": "РџР»Р°С‚РµР¶РЅР°СЏ РєР°СЂС‚Р° РЅРµ РЅР°СЃС‚СЂРѕРµРЅР°. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕР±СЂР°С‚РёС‚РµСЃСЊ Рє Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂСѓ.",
        "payment_card_info": "Р”Р°РЅРЅС‹Рµ РєР°СЂС‚С‹ РґР»СЏ РѕРїР»Р°С‚С‹:\n{card}\nР”РµСЂР¶Р°С‚РµР»СЊ РєР°СЂС‚С‹: {holder}\n\nР’С‹Р±СЂР°РЅРЅС‹Р№ С‚Р°СЂРёС„: {plan}\nРЎСѓРјРјР° Рє РѕРїР»Р°С‚Рµ: {price} UZS\n\nРџРѕСЃР»Рµ РѕРїР»Р°С‚С‹ РѕС‚РїСЂР°РІСЊС‚Рµ С‡РµРє РёР»Рё СЃРєСЂРёРЅС€РѕС‚.",
        "premium_plan_details": "Р’С‹Р±СЂР°РЅРЅС‹Р№ С‚Р°СЂРёС„: {plan}\nРЎСЂРѕРє РґРµР№СЃС‚РІРёСЏ: {days} РґРЅ.\nРЎС‚РѕРёРјРѕСЃС‚СЊ: {price} UZS\nРЎРєРёРґРєР°: -{disc}%\n\nР›РёРјРёС‚С‹:\nР¤Р°Р№Р»С‹: {files}\nРўРµРјС‹: {topics}\n\nРќР°Р¶РјРёС‚Рµ `РћРїР»Р°С‚РёС‚СЊ`, С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ.",
    },
    "en": {
        "topic_prompt": "Which topic would you like the quiz to be based on?\nPlease enter the topic.\nExample: `World Capitals`",
        "file_topic_prompt": "Which topic within the uploaded file would you like the quiz to focus on? (optional)\nPlease enter the topic.\nExample: `World Capitals`",
        "choose_translation_lang": "Which language would you like the quiz translated into?",
        "pages_prompt": "Which page range would you like the quiz to be based on? (1..{total})\nExample: `20-30` or `20`",
        "scan_pdf_choose_count": "Scanned PDF: {pages} pages.\nI can generate 1 question from each page.\nChoose the question count using the buttons.",
        "premium_choose_plan": "Please choose a Premium plan:",
        "premium_status_free": "Free plan (one-time):\nFile limit: {f_left}/{f_total}\nTopic limit: {t_left}/{t_total}\nValidity: {days} day(s) (expires: {until})",
        "premium_status_premium": "Premium plan is active.\nValid until: {until}\nFile limit: {f_left}/{f_total}\nTopic limit: {t_left}/{t_total}",
        "premium_send_screenshot": "Please send the payment receipt or screenshot. An image, PDF, or shared receipt text is accepted.",
        "receipt_checking": "Checking the payment receipt...",
        "premium_received": "Your request has been received. An administrator will review the payment and activate Premium.",
        "premium_need_image": "Please send the payment receipt or screenshot. An image, PDF, or shared receipt text is accepted.",
        "limit_free_reached": "Your free limit has been reached. Please choose a Premium plan to continue.",
        "limit_premium_reached": "Your Premium limit has been reached.\nPremium is active until: {until}\nPlease extend your plan to continue.",
        "premium_approved_user": "Your Premium plan has been activated.\nValid until: {until}",
        "premium_rejected_user": "Your Premium request has been rejected.",
        "payment_card_missing": "Payment card details are not configured. Please contact the administrator.",
        "payment_card_info": "Payment card details:\n{card}\nCard holder: {holder}\n\nSelected plan: {plan}\nAmount to pay: {price} UZS\n\nAfter payment, please send the receipt or screenshot.",
        "premium_plan_details": "Selected plan: {plan}\nDuration: {days} day(s)\nPrice: {price} UZS\nDiscount: -{disc}%\n\nLimits:\nFiles: {files}\nTopics: {topics}\n\nPress `Pay` to continue.",
    },
    "de": {
        "choose_count": "WГ¤hlen Sie die Anzahl der Fragen Гјber die SchaltflГ¤chen.",
        "choose_time": "Wie viele Sekunden pro Frage? Tippen Sie eine der Schaltfl?chen 20/30/40/50/60/70.",
        "need_translation": "Гњbersetzung nГ¶tig?",
        "shuffle_prompt_manual": "WГ¤hlen Sie den Mischmodus fГјr diesen Test:",
        "btn_shuffle_questions": "Nur Fragen",
        "btn_shuffle_answers": "Nur Antworten",
        "btn_shuffle_both": "Beides",
        "btn_shuffle_keep": "Reihenfolge behalten",
        "shuffle_strategy_prompt": "Wie soll die Zufallsreihenfolge funktionieren?",
        "btn_shuffle_saved_once": "Einmal mischen und speichern",
        "btn_shuffle_every_run": "Bei jedem Versuch neu mischen",
        "session_missing": "Sitzung nicht gefunden. Starten Sie erneut Гјber /menu.",
        "session_owner_only": "Nur der Sitzungsinhaber kann auswГ¤hlen.",
        "count_invalid": "Bitte wГ¤hlen Sie die Anzahl der Fragen Гјber die SchaltflГ¤chen.",
        "time_invalid": "Bitte w?hlen Sie nur eine der Schaltfl?chen 20/30/40/50/60/70.",
        "accepted": "Гњbernommen.",
        "count_chosen": "Fragenzahl: {n}.",
        "invalid_button": "UngГјltige SchaltflГ¤che.",
        "edit_time_prompt": "Senden Sie die neue Zeit oder wГ¤hlen Sie 20/30/40/50 Sekunden (5..600):",
        "topic_prompt": "Zu welchem Thema mГ¶chten Sie einen Test erstellen?\nBitte geben Sie das Thema ein.\nBeispiel: `HauptstГ¤dte der Welt`",
        "file_topic_prompt": "Auf welches Thema innerhalb der hochgeladenen Datei mГ¶chten Sie sich konzentrieren? (optional)\nBitte geben Sie das Thema ein.\nBeispiel: `HauptstГ¤dte der Welt`",
        "choose_translation_lang": "In welche Sprache mГ¶chten Sie den Test Гјbersetzen?",
        "pages_prompt": "Auf welchem Seitenbereich soll der Test basieren? (1..{total})\nBeispiel: `20-30` oder `20`",
        "scan_pdf_choose_count": "Gescanntes PDF: {pages} Seiten.\nIch kann pro Seite 1 Frage erstellen.\nWГ¤hlen Sie die Anzahl der Fragen Гјber die SchaltflГ¤chen.",
        "premium_choose_plan": "Bitte wГ¤hlen Sie einen Premium-Tarif:",
        "premium_status_free": "Kostenloser Tarif (einmalig):\nDateilimit: {f_left}/{f_total}\nThemenlimit: {t_left}/{t_total}\nGГјltigkeit: {days} Tag(e) (bis: {until})",
        "premium_status_premium": "Premium-Tarif ist aktiv.\nGГјltig bis: {until}\nDateilimit: {f_left}/{f_total}\nThemenlimit: {t_left}/{t_total}",
        "premium_send_screenshot": "Bitte senden Sie den Zahlungsbeleg oder einen Screenshot. Bild, PDF oder geteilter Belegtext werden akzeptiert.",
        "receipt_checking": "Zahlungsbeleg wird geprГјft...",
        "premium_received": "Ihre Anfrage wurde erhalten. Nach PrГјfung der Zahlung wird der Premium-Tarif aktiviert.",
        "premium_need_image": "Bitte senden Sie den Zahlungsbeleg oder einen Screenshot. Bild, PDF oder geteilter Belegtext werden akzeptiert.",
        "limit_free_reached": "Ihr kostenloses Kontingent ist aufgebraucht. Bitte wГ¤hlen Sie einen Premium-Tarif, um fortzufahren.",
        "limit_premium_reached": "Ihr Premium-Kontingent ist aufgebraucht.\nPremium ist gГјltig bis: {until}\nBitte verlГ¤ngern Sie Ihren Tarif, um fortzufahren.",
        "premium_approved_user": "Ihr Premium-Tarif wurde aktiviert.\nGГјltig bis: {until}",
        "premium_rejected_user": "Ihre Premium-Anfrage wurde abgelehnt.",
        "payment_card_missing": "Die Zahlungskarte ist nicht konfiguriert. Bitte wenden Sie sich an den Administrator.",
        "payment_card_info": "Zahlungsdaten:\n{card}\nKarteninhaber: {holder}\n\nGewГ¤hlter Tarif: {plan}\nZahlungsbetrag: {price} UZS\n\nSenden Sie nach der Zahlung den Beleg oder Screenshot.",
        "premium_plan_details": "GewГ¤hlter Tarif: {plan}\nLaufzeit: {days} Tag(e)\nPreis: {price} UZS\nRabatt: -{disc}%\n\nLimits:\nDateien: {files}\nThemen: {topics}\n\nDrГјcken Sie `Bezahlen`, um fortzufahren.",
        "admin_only": "Nur Admins.",
        "btn_export_docx": "Quiz-Datei (.docx)",
        "export_working": "Quiz-Datei wird erstellt...",
        "export_failed": "Datei konnte nicht erstellt werden: {err}",
        "export_docx_caption": "Quiz-Datei (.docx): {title}\nID: {id}\nDie Antworten stehen am Ende.",
        "export_answer_key_title": "LГ¶sungsschlГјssel",
        "export_sent_private_notice": "Die Quiz-Datei wurde an Ihren privaten Chat gesendet.",
        "topic_too_long": "Das Thema ist zu lang. Maximal: {n} Zeichen.",
        "manual_draft_found": "Sie haben einen gespeicherten Quiz-Entwurf. Fortsetzen?",
        "btn_manual_continue": "Fortsetzen",
        "btn_manual_restart": "Neu starten",
        "btn_edit_answers": "Antworten bearbeiten",
        "btn_prev_page": "в¬…пёЏ ZurГјck",
        "btn_next_page": "Weiter вћЎпёЏ",
        "edit_answers_choose_question": "Antworten bearbeiten: WГ¤hlen Sie eine Fragennummer ({count}).",
        "edit_answers_choose_correct": "WГ¤hlen Sie die Nummer der richtigen Antwort:",
        "edit_answers_updated": "Gespeichert.",
    },
    "tr": {
        "choose_count": "Soru sayД±sД±nД± dГјДџmelerden seГ§in.",
        "choose_time": "Her soru iГ§in kaГ§ saniye? 20/30/40/50 seГ§in ya da sayД± gГ¶nderin (5..600).",
        "need_translation": "Г‡eviri gerekli mi?",
        "shuffle_prompt_manual": "Bu test iГ§in karД±ЕџtД±rma tГјrГјnГј seГ§in:",
        "btn_shuffle_questions": "Sadece sorular",
        "btn_shuffle_answers": "Sadece cevaplar",
        "btn_shuffle_both": "Д°kisi de",
        "btn_shuffle_keep": "SД±rayД± koru",
        "shuffle_strategy_prompt": "Rastgele sД±ra nasД±l Г§alД±ЕџsД±n?",
        "btn_shuffle_saved_once": "Bir kez karД±ЕџtД±r ve kaydet",
        "btn_shuffle_every_run": "Her denemede yeniden karД±ЕџtД±r",
        "session_missing": "Oturum bulunamadД±. /menu Гјzerinden yeniden baЕџlayД±n.",
        "session_owner_only": "YalnД±zca oturum sahibi seГ§im yapabilir.",
        "count_invalid": "LГјtfen soru sayД±sД±nД± dГјДџmelerden seГ§in.",
        "time_invalid": "Saniyeyi sayД± olarak gГ¶nderin (5..600) veya 20/30/40/50 dГјДџmelerinden birine dokunun.",
        "accepted": "Kaydedildi.",
        "count_chosen": "Soru sayД±sД±: {n}.",
        "invalid_button": "GeГ§ersiz dГјДџme.",
        "edit_time_prompt": "Yeni sГјreyi gГ¶nderin veya 20/30/40/50 saniye seГ§in (5..600):",
        "topic_prompt": "Testi hangi konuya gГ¶re oluЕџturmak istersiniz?\nLГјtfen konuyu yazД±n.\nГ–rnek: `DГјnya BaЕџkentleri`",
        "file_topic_prompt": "YГјklenen dosya iГ§inde hangi konuya odaklanmamД± istersiniz? (isteДџe baДџlД±)\nLГјtfen konuyu yazД±n.\nГ–rnek: `DГјnya BaЕџkentleri`",
        "choose_translation_lang": "Testin hangi dile Г§evrilmesini istersiniz?",
        "pages_prompt": "Testin hangi sayfa aralД±ДџД±na gГ¶re oluЕџturulmasД±nД± istersiniz? (1..{total})\nГ–rnek: `20-30` veya `20`",
        "scan_pdf_choose_count": "TaranmД±Еџ PDF: {pages} sayfa.\nHer sayfadan 1 soru oluЕџturabilirim.\nSoru sayД±sД±nД± dГјДџmelerden seГ§in.",
        "premium_choose_plan": "LГјtfen bir Premium plan seГ§in:",
        "premium_status_free": "Гњcretsiz plan (tek seferlik):\nDosya limiti: {f_left}/{f_total}\nKonu limiti: {t_left}/{t_total}\nGeГ§erlilik sГјresi: {days} gГјn (bitiЕџ: {until})",
        "premium_status_premium": "Premium plan aktif.\nGeГ§erlilik sГјresi: {until}\nDosya limiti: {f_left}/{f_total}\nKonu limiti: {t_left}/{t_total}",
        "premium_send_screenshot": "LГјtfen Г¶deme dekontunu veya ekran gГ¶rГјntГјsГјnГј gГ¶nderin. GГ¶rsel, PDF veya paylaЕџД±lan dekont metni kabul edilir.",
        "receipt_checking": "Г–deme dekontu kontrol ediliyor...",
        "premium_received": "Talebiniz alД±ndД±. Г–deme kontrol edildikten sonra Premium plan etkinleЕџtirilecektir.",
        "premium_need_image": "LГјtfen Г¶deme dekontunu veya ekran gГ¶rГјntГјsГјnГј gГ¶nderin. GГ¶rsel, PDF veya paylaЕџД±lan dekont metni kabul edilir.",
        "limit_free_reached": "Гњcretsiz limitiniz doldu. Devam etmek iГ§in lГјtfen bir Premium plan seГ§in.",
        "limit_premium_reached": "Premium limitiniz doldu.\nPremium geГ§erlilik sГјresi: {until}\nDevam etmek iГ§in planД±nД±zД± uzatД±n.",
        "premium_approved_user": "Premium planД±nД±z etkinleЕџtirildi.\nGeГ§erlilik sГјresi: {until}",
        "premium_rejected_user": "Premium talebiniz reddedildi.",
        "payment_card_missing": "Г–deme kartД± bilgileri yapД±landД±rД±lmamД±Еџ. LГјtfen yГ¶neticiyle iletiЕџime geГ§in.",
        "payment_card_info": "Г–deme kartД± bilgileri:\n{card}\nKart sahibi: {holder}\n\nSeГ§ilen plan: {plan}\nГ–denecek tutar: {price} UZS\n\nГ–demeden sonra dekontu veya ekran gГ¶rГјntГјsГјnГј gГ¶nderin.",
        "premium_plan_details": "SeГ§ilen plan: {plan}\nSГјre: {days} gГјn\nFiyat: {price} UZS\nД°ndirim: -{disc}%\n\nLimitler:\nDosya: {files}\nKonu: {topics}\n\nDevam etmek iГ§in `Г–deme yap` dГјДџmesine basД±n.",
        "admin_only": "YalnД±zca yГ¶neticiler.",
        "btn_export_docx": "Test dosyasД± (.docx)",
        "export_working": "Test dosyasД± hazД±rlanД±yor...",
        "export_failed": "Dosya oluЕџturulamadД±: {err}",
        "export_docx_caption": "Test dosyasД± (.docx): {title}\nID: {id}\nCevaplar dosyanД±n sonundadД±r.",
        "export_answer_key_title": "Cevap anahtarД±",
        "export_sent_private_notice": "Test dosyasД± Г¶zel sohbetinize gГ¶nderildi.",
        "topic_too_long": "Konu Г§ok uzun. Maksimum: {n} karakter.",
        "manual_draft_found": "KaydedilmiЕџ bir test taslaДџД±nД±z var. Devam edilsin mi?",
        "btn_manual_continue": "Devam et",
        "btn_manual_restart": "Yeniden baЕџla",
        "btn_edit_answers": "CevaplarД± dГјzenle",
        "btn_prev_page": "в¬…пёЏ Г–nceki",
        "btn_next_page": "Sonraki вћЎпёЏ",
        "edit_answers_choose_question": "CevaplarД± dГјzenle: soru numarasД±nД± seГ§in ({count}).",
        "edit_answers_choose_correct": "DoДџru cevap numarasД±nД± seГ§in:",
        "edit_answers_updated": "Kaydedildi.",
    },
    "kk": {
        "choose_count": "РЎТ±СЂР°Т› СЃР°РЅС‹РЅ С‚ТЇР№РјРµР»РµСЂ Р°СЂТ›С‹Р»С‹ С‚Р°ТЈРґР°ТЈС‹Р·.",
        "choose_time": "УСЂ СЃТ±СЂР°Т›Т›Р° РЅРµС€Рµ СЃРµРєСѓРЅРґ? 20/30/40/50 С‚Р°ТЈРґР°ТЈС‹Р· РЅРµРјРµСЃРµ СЃР°РЅ Р¶С–Р±РµСЂС–ТЈС–Р· (5..600).",
        "need_translation": "РђСѓРґР°СЂРјР° РєРµСЂРµРє РїРµ?",
        "shuffle_prompt_manual": "РћСЃС‹ С‚РµСЃС‚ ТЇС€С–РЅ Р°СЂР°Р»Р°СЃС‚С‹СЂСѓ С‚ТЇСЂС–РЅ С‚Р°ТЈРґР°ТЈС‹Р·:",
        "btn_shuffle_questions": "РўРµРє СЃТ±СЂР°Т›С‚Р°СЂ",
        "btn_shuffle_answers": "РўРµРє Р¶Р°СѓР°РїС‚Р°СЂ",
        "btn_shuffle_both": "Р•РєРµСѓС– РґРµ",
        "btn_shuffle_keep": "Р РµС‚С–РЅ СЃР°Т›С‚Р°Сѓ",
        "shuffle_strategy_prompt": "РљРµР·РґРµР№СЃРѕТ› СЂРµС‚С‚С– Т›Р°Р»Р°Р№ Т›РѕР»РґР°РЅР°Р№С‹Т›?",
        "btn_shuffle_saved_once": "Р‘С–СЂ СЂРµС‚ Р°СЂР°Р»Р°СЃС‚С‹СЂС‹Рї СЃР°Т›С‚Р°Сѓ",
        "btn_shuffle_every_run": "УСЂ У™СЂРµРєРµС‚С‚Рµ Т›Р°Р№С‚Р° Р°СЂР°Р»Р°СЃС‚С‹СЂСѓ",
        "session_missing": "РЎРµСЃСЃРёСЏ С‚Р°Р±С‹Р»РјР°РґС‹. /menu Р°СЂТ›С‹Р»С‹ Т›Р°Р№С‚Р° Р±Р°СЃС‚Р°ТЈС‹Р·.",
        "session_owner_only": "РўРµРє СЃРµСЃСЃРёСЏ РёРµСЃС– С‚Р°ТЈРґР°Р№ Р°Р»Р°РґС‹.",
        "count_invalid": "РЎТ±СЂР°Т› СЃР°РЅС‹РЅ С‚ТЇР№РјРµР»РµСЂ Р°СЂТ›С‹Р»С‹ С‚Р°ТЈРґР°ТЈС‹Р·.",
        "time_invalid": "РЎРµРєСѓРЅРґС‚С‹ СЃР°РЅРјРµРЅ Р¶С–Р±РµСЂС–ТЈС–Р· (5..600) РЅРµРјРµСЃРµ 20/30/40/50 С‚ТЇР№РјРµР»РµСЂС–РЅС–ТЈ Р±С–СЂС–РЅ Р±Р°СЃС‹ТЈС‹Р·.",
        "accepted": "ТљР°Р±С‹Р»РґР°РЅРґС‹.",
        "count_chosen": "РЎТ±СЂР°Т› СЃР°РЅС‹: {n}.",
        "invalid_button": "ТљР°С‚Рµ С‚ТЇР№РјРµ.",
        "edit_time_prompt": "Р–Р°ТЈР° СѓР°Т›С‹С‚С‚С‹ Р¶С–Р±РµСЂС–ТЈС–Р· РЅРµРјРµСЃРµ 20/30/40/50 СЃРµРєСѓРЅРґ С‚Р°ТЈРґР°ТЈС‹Р· (5..600):",
        "btn_premium": "РџСЂРµРјРёСѓРј",
        "topic_prompt": "ТљР°Р№ С‚Р°Т›С‹СЂС‹Рї РЅРµРіС–Р·С–РЅРґРµ С‚РµСЃС‚ Т›Т±СЂР°СЃС‚С‹СЂТ“С‹ТЈС‹Р· РєРµР»РµРґС–?\nРўР°Т›С‹СЂС‹РїС‚С‹ Р¶Р°Р·С‹ТЈС‹Р·.\nРњС‹СЃР°Р»: `УР»РµРј Р°СЃС‚Р°РЅР°Р»Р°СЂС‹`",
        "file_topic_prompt": "Р–ТЇРєС‚РµР»РіРµРЅ С„Р°Р№Р» С–С€С–РЅРґРµРіС– Т›Р°Р№ С‚Р°Т›С‹СЂС‹РїТ›Р° РЅР°Р·Р°СЂ Р°СѓРґР°СЂТ“С‹ТЈС‹Р· РєРµР»РµРґС–? (Т›Р°Р»Р°СѓС‹ТЈС‹Р·С€Р°)\nРўР°Т›С‹СЂС‹РїС‚С‹ Р¶Р°Р·С‹ТЈС‹Р·.\nРњС‹СЃР°Р»: `УР»РµРј Р°СЃС‚Р°РЅР°Р»Р°СЂС‹`",
        "choose_translation_lang": "РўРµСЃС‚С‚С– Т›Р°Р№ С‚С–Р»РіРµ Р°СѓРґР°СЂТ“С‹ТЈС‹Р· РєРµР»РµРґС–?",
        "pages_prompt": "РўРµСЃС‚С‚С– Т›Р°Р№ Р±РµС‚С‚РµСЂ Р°СЂР°Р»С‹Т“С‹ РЅРµРіС–Р·С–РЅРґРµ Т›Т±СЂР°СЃС‚С‹СЂТ“С‹ТЈС‹Р· РєРµР»РµРґС–? (1..{total})\nРњС‹СЃР°Р»: `20-30` РЅРµРјРµСЃРµ `20`",
        "scan_pdf_choose_count": "РЎРєР°РЅ PDF: {pages} Р±РµС‚.\nУСЂ Р±РµС‚С‚РµРЅ 1 СЃТ±СЂР°Т› Т›Т±СЂР°СЃС‚С‹СЂР° Р°Р»Р°РјС‹РЅ.\nРЎТ±СЂР°Т› СЃР°РЅС‹РЅ С‚ТЇР№РјРµР»РµСЂ Р°СЂТ›С‹Р»С‹ С‚Р°ТЈРґР°ТЈС‹Р·.",
        "premium_choose_plan": "РџСЂРµРјРёСѓРј Р¶РѕСЃРїР°СЂР»Р°СЂС‹РЅС‹ТЈ Р±С–СЂС–РЅ С‚Р°ТЈРґР°ТЈС‹Р·:",
        "premium_status_free": "РўРµРіС–РЅ Р¶РѕСЃРїР°СЂ (Р±С–СЂ СЂРµС‚С‚С–Рє):\nР¤Р°Р№Р» Р»РёРјРёС‚С–: {f_left}/{f_total}\nРўР°Т›С‹СЂС‹Рї Р»РёРјРёС‚С–: {t_left}/{t_total}\nР–Р°СЂР°РјРґС‹Р»С‹Т› РјРµСЂР·С–РјС–: {days} РєТЇРЅ (Р°СЏТ›С‚Р°Р»СѓС‹: {until})",
        "premium_status_premium": "РџСЂРµРјРёСѓРј Р¶РѕСЃРїР°СЂ Р±РµР»СЃРµРЅРґС–.\nР–Р°СЂР°РјРґС‹Р»С‹Т› РјРµСЂР·С–РјС–: {until}\nР¤Р°Р№Р» Р»РёРјРёС‚С–: {f_left}/{f_total}\nРўР°Т›С‹СЂС‹Рї Р»РёРјРёС‚С–: {t_left}/{t_total}",
        "premium_send_screenshot": "РўУ©Р»РµРј С‡РµРіС–РЅ РЅРµРјРµСЃРµ СЃРєСЂРёРЅС€РѕС‚С‹РЅ Р¶С–Р±РµСЂС–ТЈС–Р·. РЎСѓСЂРµС‚, PDF РЅРµРјРµСЃРµ С‚У©Р»РµРјРґС– СЂР°СЃС‚Р°Сѓ РјУ™С‚С–РЅС– Т›Р°Р±С‹Р»РґР°РЅР°РґС‹.",
        "receipt_checking": "РўУ©Р»РµРј С‡РµРіС– С‚РµРєСЃРµСЂС–Р»СѓРґРµ...",
        "premium_received": "РЎТ±СЂР°СѓС‹ТЈС‹Р· Т›Р°Р±С‹Р»РґР°РЅРґС‹. РўУ©Р»РµРј С‚РµРєСЃРµСЂС–Р»РіРµРЅРЅРµРЅ РєРµР№С–РЅ РџСЂРµРјРёСѓРј Р¶РѕСЃРїР°СЂ Р±РµР»СЃРµРЅРґС–СЂС–Р»РµРґС–.",
        "premium_need_image": "РўУ©Р»РµРј С‡РµРіС–РЅ РЅРµРјРµСЃРµ СЃРєСЂРёРЅС€РѕС‚С‹РЅ Р¶С–Р±РµСЂС–ТЈС–Р·. РЎСѓСЂРµС‚, PDF РЅРµРјРµСЃРµ С‚У©Р»РµРјРґС– СЂР°СЃС‚Р°Сѓ РјУ™С‚С–РЅС– Т›Р°Р±С‹Р»РґР°РЅР°РґС‹.",
        "limit_free_reached": "РўРµРіС–РЅ Р»РёРјРёС‚ Р°СЏТ›С‚Р°Р»РґС‹. Р–Р°Р»Т“Р°СЃС‚С‹СЂСѓ ТЇС€С–РЅ РџСЂРµРјРёСѓРј Р¶РѕСЃРїР°СЂРґС‹ С‚Р°ТЈРґР°ТЈС‹Р·.",
        "limit_premium_reached": "РџСЂРµРјРёСѓРј Р»РёРјРёС‚С– Р°СЏТ›С‚Р°Р»РґС‹.\nРџСЂРµРјРёСѓРј Р¶Р°СЂР°РјРґС‹Р»С‹Т› РјРµСЂР·С–РјС–: {until}\nР–Р°Р»Т“Р°СЃС‚С‹СЂСѓ ТЇС€С–РЅ Р¶РѕСЃРїР°СЂС‹ТЈС‹Р·РґС‹ Т±Р·Р°СЂС‚С‹ТЈС‹Р·.",
        "premium_plan_1d": "1 РєТЇРЅ",
        "premium_plan_7d": "7 РєТЇРЅ",
        "premium_plan_30d": "1 Р°Р№",
        "btn_premium_approve": "Р Р°СЃС‚Р°Сѓ",
        "btn_premium_reject": "ТљР°Р±С‹Р»РґР°РјР°Сѓ",
        "premium_approved_user": "РџСЂРµРјРёСѓРј Р¶РѕСЃРїР°СЂС‹ТЈС‹Р· Р±РµР»СЃРµРЅРґС–СЂС–Р»РґС–.\nР–Р°СЂР°РјРґС‹Р»С‹Т› РјРµСЂР·С–РјС–: {until}",
        "premium_rejected_user": "РџСЂРµРјРёСѓРј СЃТ±СЂР°СѓС‹ТЈС‹Р· Т›Р°Р±С‹Р»РґР°РЅР±Р°РґС‹.",
        "btn_pay": "РўУ©Р»РµРј Р¶Р°СЃР°Сѓ",
        "payment_card_missing": "РўУ©Р»РµРј РєР°СЂС‚Р°СЃС‹РЅС‹ТЈ РґРµСЂРµРєС‚РµСЂС– Р±Р°РїС‚Р°Р»РјР°Т“Р°РЅ. УРєС–РјС€С–РіРµ С…Р°Р±Р°СЂР»Р°СЃС‹ТЈС‹Р·.",
        "payment_card_info": "РўУ©Р»РµРј РєР°СЂС‚Р°СЃС‹РЅС‹ТЈ РґРµСЂРµРєС‚РµСЂС–:\n{card}\nРљР°СЂС‚Р° РёРµСЃС–: {holder}\n\nРўР°ТЈРґР°Р»Т“Р°РЅ Р¶РѕСЃРїР°СЂ: {plan}\nРўУ©Р»РµРј СЃРѕРјР°СЃС‹: {price} UZS\n\nРўУ©Р»РµРј Р¶Р°СЃР°Т“Р°РЅРЅР°РЅ РєРµР№С–РЅ С‡РµРє РЅРµРјРµСЃРµ СЃРєСЂРёРЅС€РѕС‚ Р¶С–Р±РµСЂС–ТЈС–Р·.",
        "premium_plan_details": "РўР°ТЈРґР°Р»Т“Р°РЅ Р¶РѕСЃРїР°СЂ: {plan}\nТ°Р·Р°Т›С‚С‹Т“С‹: {days} РєТЇРЅ\nР‘Р°Т“Р°СЃС‹: {price} UZS\nР–РµТЈС–Р»РґС–Рє: -{disc}%\n\nР›РёРјРёС‚С‚РµСЂ:\nР¤Р°Р№Р»: {files}\nРўР°Т›С‹СЂС‹Рї: {topics}\n\nР–Р°Р»Т“Р°СЃС‚С‹СЂСѓ ТЇС€С–РЅ `РўУ©Р»РµРј Р¶Р°СЃР°Сѓ` С‚ТЇР№РјРµСЃС–РЅ Р±Р°СЃС‹ТЈС‹Р·.",
        "admin_only": "РўРµРє У™РєС–РјС€С–РіРµ Р°СЂРЅР°Р»Т“Р°РЅ.",
        "btn_export_docx": "РўРµСЃС‚ С„Р°Р№Р»С‹ (.docx)",
        "export_working": "РўРµСЃС‚ С„Р°Р№Р»С‹ РґР°Р№С‹РЅРґР°Р»СѓРґР°...",
        "export_failed": "Р¤Р°Р№Р»РґС‹ Р¶Р°СЃР°Сѓ РјТЇРјРєС–РЅ Р±РѕР»РјР°РґС‹: {err}",
        "export_docx_caption": "РўРµСЃС‚ С„Р°Р№Р»С‹ (.docx): {title}\nID: {id}\nР–Р°СѓР°РїС‚Р°СЂ С„Р°Р№Р»РґС‹ТЈ СЃРѕТЈС‹РЅРґР° Р±РµСЂС–Р»РіРµРЅ.",
        "export_answer_key_title": "Р–Р°СѓР°РїС‚Р°СЂ РєС–Р»С‚С–",
        "export_sent_private_notice": "РўРµСЃС‚ С„Р°Р№Р»С‹ Р¶РµРєРµ С‡Р°С‚Т›Р° Р¶С–Р±РµСЂС–Р»РґС–.",
        "topic_too_long": "РўР°Т›С‹СЂС‹Рї С‚С‹Рј Т±Р·С‹РЅ. Р•ТЈ РєУ©Р±С–: {n} С‚Р°ТЈР±Р°.",
        "manual_draft_found": "РЎС–Р·РґРµ СЃР°Т›С‚Р°Р»Т“Р°РЅ Т›РѕР»РјРµРЅ Р¶Р°СЃР°Р»Т“Р°РЅ С‚РµСЃС‚ РЅРѕР±Р°Р№С‹ Р±Р°СЂ. Р–Р°Р»Т“Р°СЃС‚С‹СЂР°Р№С‹Т› РїР°?",
        "btn_manual_continue": "Р–Р°Р»Т“Р°СЃС‚С‹СЂСѓ",
        "btn_manual_restart": "ТљР°Р№С‚Р° Р±Р°СЃС‚Р°Сѓ",
        "btn_edit_answers": "Р–Р°СѓР°РїС‚Р°СЂРґС‹ У©ТЈРґРµСѓ",
        "btn_prev_page": "в¬…пёЏ РђР»РґС‹ТЈТ“С‹",
        "btn_next_page": "РљРµР»РµСЃС– вћЎпёЏ",
        "edit_answers_choose_question": "Р–Р°СѓР°РїС‚Р°СЂРґС‹ У©ТЈРґРµСѓ: СЃТ±СЂР°Т› РЅУ©РјС–СЂС–РЅ С‚Р°ТЈРґР°ТЈС‹Р· ({count}).",
        "edit_answers_choose_correct": "Р”Т±СЂС‹СЃ Р¶Р°СѓР°Рї РЅУ©РјС–СЂС–РЅ С‚Р°ТЈРґР°ТЈС‹Р·:",
        "edit_answers_updated": "РЎР°Т›С‚Р°Р»РґС‹.",
    },
    "ar": {
        "choose_count": "Ш§Ш®ШЄШ± Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш© ШЁШ§ШіШЄШ®ШЇШ§Щ… Ш§Щ„ШЈШІШ±Ш§Ш±.",
        "choose_time": "ЩѓЩ… Ш«Ш§Щ†ЩЉШ© Щ„ЩѓЩ„ ШіШ¤Ш§Щ„Шџ Ш§Ш¶ШєШ· 20/30/40/50 ШЈЩ€ ШЈШ±ШіЩ„ Ш±Щ‚Щ…Щ‹Ш§ (5..600).",
        "need_translation": "Щ‡Щ„ ШЄШ­ШЄШ§Ш¬ ШҐЩ„Щ‰ ШЄШ±Ш¬Щ…Ш©Шџ",
        "shuffle_prompt_manual": "Ш§Ш®ШЄШ± Ш·Ш±ЩЉЩ‚Ш© Ш§Щ„Ш®Щ„Ш· Щ„Щ‡Ш°Ш§ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш±:",
        "btn_shuffle_questions": "Ш§Щ„ШЈШіШ¦Щ„Ш© ЩЃЩ‚Ш·",
        "btn_shuffle_answers": "Ш§Щ„ШҐШ¬Ш§ШЁШ§ШЄ ЩЃЩ‚Ш·",
        "btn_shuffle_both": "ЩѓЩ„Ш§Щ‡Щ…Ш§",
        "btn_shuffle_keep": "ШЁШЇЩ€Щ† Ш®Щ„Ш·",
        "shuffle_strategy_prompt": "ЩѓЩЉЩЃ ЩЉШ¬ШЁ ШЈЩ† ЩЉШ№Щ…Щ„ Ш§Щ„ШЄШ±ШЄЩЉШЁ Ш§Щ„Ш№ШґЩ€Ш§Ш¦ЩЉШџ",
        "btn_shuffle_saved_once": "Ш§Ш®Щ„Ш· Щ…Ш±Ш© Щ€Ш§Ш­ШЇШ© Щ€Ш§Ш­ЩЃШё",
        "btn_shuffle_every_run": "ШЈШ№ШЇ Ш§Щ„Ш®Щ„Ш· ЩЃЩЉ ЩѓЩ„ Щ…Ш­Ш§Щ€Щ„Ш©",
        "session_missing": "Щ„Щ… ЩЉШЄЩ… Ш§Щ„Ш№Ш«Щ€Ш± Ш№Щ„Щ‰ Ш§Щ„Ш¬Щ„ШіШ©. Ш§ШЁШЇШЈ Щ…Щ† Ш¬ШЇЩЉШЇ Ш№ШЁШ± /menu.",
        "session_owner_only": "ЩЃЩ‚Ш· ШµШ§Ш­ШЁ Ш§Щ„Ш¬Щ„ШіШ© ЩЉЩ…ЩѓЩ†Щ‡ Ш§Щ„Ш§Ш®ШЄЩЉШ§Ш±.",
        "count_invalid": "ЩЉШ±Ш¬Щ‰ Ш§Ш®ШЄЩЉШ§Ш± Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш© ШЁШ§ШіШЄШ®ШЇШ§Щ… Ш§Щ„ШЈШІШ±Ш§Ш±.",
        "time_invalid": "ШЈШ±ШіЩ„ Ш§Щ„Ш«Щ€Ш§Щ†ЩЉ ЩѓШ±Щ‚Щ… (5..600) ШЈЩ€ Ш§Ш¶ШєШ· ШЈШ­ШЇ ШЈШІШ±Ш§Ш± 20/30/40/50.",
        "accepted": "ШЄЩ… Ш§Щ„Ш­ЩЃШё.",
        "count_chosen": "Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш©: {n}.",
        "invalid_button": "ШІШ± ШєЩЉШ± ШµШ§Щ„Ш­.",
        "edit_time_prompt": "ШЈШ±ШіЩ„ Ш§Щ„Щ€Щ‚ШЄ Ш§Щ„Ш¬ШЇЩЉШЇ ШЈЩ€ Ш§Ш®ШЄШ± 20/30/40/50 Ш«Ш§Щ†ЩЉШ© (5..600):",
        "btn_premium": "ШЁШ±ЩЉЩ…ЩЉЩ€Щ…",
        "topic_prompt": "Щ…Ш§ Ш§Щ„Щ…Щ€Ш¶Щ€Ш№ Ш§Щ„Ш°ЩЉ ШЄШ±ШєШЁ ЩЃЩЉ ШҐЩ†ШґШ§ШЎ Ш§Ш®ШЄШЁШ§Ш± Ш№Щ„Щ‰ ШЈШіШ§ШіЩ‡Шџ\nЩЉШ±Ш¬Щ‰ ЩѓШЄШ§ШЁШ© Ш§Щ„Щ…Щ€Ш¶Щ€Ш№.\nЩ…Ш«Ш§Щ„: `Ш№Щ€Ш§ШµЩ… Ш§Щ„Ш№Ш§Щ„Щ…`",
        "file_topic_prompt": "Щ…Ш§ Ш§Щ„Щ…Щ€Ш¶Щ€Ш№ Ш§Щ„Ш°ЩЉ ШЄШ±ШєШЁ ЩЃЩЉ Ш§Щ„ШЄШ±ЩѓЩЉШІ Ш№Щ„ЩЉЩ‡ ШЇШ§Ш®Щ„ Ш§Щ„Щ…Щ„ЩЃ Ш§Щ„Щ…Ш±ЩЃЩ€Ш№Шџ (Ш§Ш®ШЄЩЉШ§Ш±ЩЉ)\nЩЉШ±Ш¬Щ‰ ЩѓШЄШ§ШЁШ© Ш§Щ„Щ…Щ€Ш¶Щ€Ш№.\nЩ…Ш«Ш§Щ„: `Ш№Щ€Ш§ШµЩ… Ш§Щ„Ш№Ш§Щ„Щ…`",
        "choose_translation_lang": "ШҐЩ„Щ‰ ШЈЩЉ Щ„ШєШ© ШЄШ±ШєШЁ ЩЃЩЉ ШЄШ±Ш¬Щ…Ш© Ш§Щ„Ш§Ш®ШЄШЁШ§Ш±Шџ",
        "pages_prompt": "Ш№Щ„Щ‰ ШЈЩЉ Щ†Ш·Ш§Щ‚ ШµЩЃШ­Ш§ШЄ ШЄШ±ШєШЁ ЩЃЩЉ ШҐЩ†ШґШ§ШЎ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш±Шџ (1..{total})\nЩ…Ш«Ш§Щ„: `20-30` ШЈЩ€ `20`",
        "scan_pdf_choose_count": "Щ…Щ„ЩЃ PDF Щ…Щ…ШіЩ€Ш­ Ш¶Щ€Ш¦ЩЉЩ‹Ш§: {pages} ШµЩЃШ­Ш©.\nЩЉЩ…ЩѓЩ†Щ†ЩЉ ШҐЩ†ШґШ§ШЎ ШіШ¤Ш§Щ„ Щ€Ш§Ш­ШЇ Щ…Щ† ЩѓЩ„ ШµЩЃШ­Ш©.\nШ§Ш®ШЄШ± Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш© ШЁШ§ШіШЄШ®ШЇШ§Щ… Ш§Щ„ШЈШІШ±Ш§Ш±.",
        "premium_choose_plan": "ЩЉШ±Ш¬Щ‰ Ш§Ш®ШЄЩЉШ§Ш± ШҐШ­ШЇЩ‰ ШЁШ§Щ‚Ш§ШЄ ШЁШ±ЩЉЩ…ЩЉЩ€Щ…:",
        "premium_status_free": "Ш§Щ„Ш®Ш·Ш© Ш§Щ„Щ…Ш¬Ш§Щ†ЩЉШ© (Щ…Ш±Ш© Щ€Ш§Ш­ШЇШ©):\nШ­ШЇ Ш§Щ„Щ…Щ„ЩЃШ§ШЄ: {f_left}/{f_total}\nШ­ШЇ Ш§Щ„Щ…Щ€Ш§Ш¶ЩЉШ№: {t_left}/{t_total}\nЩ…ШЇШ© Ш§Щ„ШµЩ„Ш§Ш­ЩЉШ©: {days} ЩЉЩ€Щ… (ЩЉЩ†ШЄЩ‡ЩЉ ЩЃЩЉ: {until})",
        "premium_status_premium": "Ш®Ш·Ш© ШЁШ±ЩЉЩ…ЩЉЩ€Щ… Щ…ЩЃШ№Щ„Ш©.\nШµШ§Щ„Ш­Ш© Ш­ШЄЩ‰: {until}\nШ­ШЇ Ш§Щ„Щ…Щ„ЩЃШ§ШЄ: {f_left}/{f_total}\nШ­ШЇ Ш§Щ„Щ…Щ€Ш§Ш¶ЩЉШ№: {t_left}/{t_total}",
        "premium_send_screenshot": "ЩЉШ±Ш¬Щ‰ ШҐШ±ШіШ§Щ„ ШҐЩЉШµШ§Щ„ Ш§Щ„ШЇЩЃШ№ ШЈЩ€ Щ„Щ‚Ш·Ш© Ш§Щ„ШґШ§ШґШ©. ЩЉШЄЩ… Щ‚ШЁЩ€Щ„ Ш§Щ„ШµЩ€Ш±Ш© ШЈЩ€ Щ…Щ„ЩЃ PDF ШЈЩ€ Щ†Шµ ШҐШ«ШЁШ§ШЄ Ш§Щ„ШЇЩЃШ№.",
        "receipt_checking": "ЩЉШЄЩ… Ш§Щ„ШЄШ­Щ‚Щ‚ Щ…Щ† ШҐЩЉШµШ§Щ„ Ш§Щ„ШЇЩЃШ№...",
        "premium_received": "ШЄЩ… Ш§ШіШЄЩ„Ш§Щ… Ш·Щ„ШЁЩѓ. ШіЩЉЩ‚Щ€Щ… Ш§Щ„Щ…ШґШ±ЩЃ ШЁЩ…Ш±Ш§Ш¬Ш№Ш© Ш§Щ„ШЇЩЃШ№ Щ€ШЄЩЃШ№ЩЉЩ„ ШЁШ§Щ‚Ш© ШЁШ±ЩЉЩ…ЩЉЩ€Щ….",
        "premium_need_image": "ЩЉШ±Ш¬Щ‰ ШҐШ±ШіШ§Щ„ ШҐЩЉШµШ§Щ„ Ш§Щ„ШЇЩЃШ№ ШЈЩ€ Щ„Щ‚Ш·Ш© Ш§Щ„ШґШ§ШґШ©. ЩЉШЄЩ… Щ‚ШЁЩ€Щ„ Ш§Щ„ШµЩ€Ш±Ш© ШЈЩ€ Щ…Щ„ЩЃ PDF ШЈЩ€ Щ†Шµ ШҐШ«ШЁШ§ШЄ Ш§Щ„ШЇЩЃШ№.",
        "limit_free_reached": "Щ„Щ‚ШЇ Ш§Щ†ШЄЩ‡Щ‰ Ш§Щ„Ш­ШЇ Ш§Щ„Щ…Ш¬Ш§Щ†ЩЉ. Щ„Щ…ШЄШ§ШЁШ№Ш© Ш§Щ„Ш§ШіШЄШ®ШЇШ§Щ…ШЊ ЩЉШ±Ш¬Щ‰ Ш§Ш®ШЄЩЉШ§Ш± ШЁШ§Щ‚Ш© ШЁШ±ЩЉЩ…ЩЉЩ€Щ….",
        "limit_premium_reached": "Щ„Щ‚ШЇ Ш§Щ†ШЄЩ‡Щ‰ Ш­ШЇ ШЁШ±ЩЉЩ…ЩЉЩ€Щ….\nШµЩ„Ш§Ш­ЩЉШ© ШЁШ±ЩЉЩ…ЩЉЩ€Щ… Ш­ШЄЩ‰: {until}\nЩЉШ±Ш¬Щ‰ ШЄЩ…ШЇЩЉШЇ Ш§Щ„ШЁШ§Щ‚Ш© Щ„Щ„Щ…ШЄШ§ШЁШ№Ш©.",
        "premium_plan_1d": "ЩЉЩ€Щ… Щ€Ш§Ш­ШЇ",
        "premium_plan_7d": "7 ШЈЩЉШ§Щ…",
        "premium_plan_30d": "ШґЩ‡Ш± Щ€Ш§Ш­ШЇ",
        "btn_premium_approve": "Щ…Щ€Ш§ЩЃЩ‚Ш©",
        "btn_premium_reject": "Ш±ЩЃШ¶",
        "premium_approved_user": "ШЄЩ… ШЄЩЃШ№ЩЉЩ„ ШЁШ§Щ‚Ш© ШЁШ±ЩЉЩ…ЩЉЩ€Щ….\nШµШ§Щ„Ш­Ш© Ш­ШЄЩ‰: {until}",
        "premium_rejected_user": "ШЄЩ… Ш±ЩЃШ¶ Ш·Щ„ШЁ ШЁШ±ЩЉЩ…ЩЉЩ€Щ… Ш§Щ„Ш®Ш§Шµ ШЁЩѓ.",
        "btn_pay": "Ш§Щ„ШЇЩЃШ№",
        "payment_card_missing": "ШЁЩЉШ§Щ†Ш§ШЄ ШЁШ·Ш§Щ‚Ш© Ш§Щ„ШЇЩЃШ№ ШєЩЉШ± Щ…Щ‡ЩЉШЈШ©. ЩЉШ±Ш¬Щ‰ Ш§Щ„ШЄЩ€Ш§ШµЩ„ Щ…Ш№ Ш§Щ„Щ…ШґШ±ЩЃ.",
        "payment_card_info": "ШЁЩЉШ§Щ†Ш§ШЄ ШЁШ·Ш§Щ‚Ш© Ш§Щ„ШЇЩЃШ№:\n{card}\nШ§ШіЩ… ШµШ§Ш­ШЁ Ш§Щ„ШЁШ·Ш§Щ‚Ш©: {holder}\n\nШ§Щ„ШЁШ§Щ‚Ш© Ш§Щ„Щ…Ш®ШЄШ§Ш±Ш©: {plan}\nШ§Щ„Щ…ШЁЩ„Шє Ш§Щ„Щ…Ш·Щ„Щ€ШЁ: {price} UZS\n\nШЁШ№ШЇ ШҐШЄЩ…Ш§Щ… Ш§Щ„ШЇЩЃШ№ШЊ ЩЉШ±Ш¬Щ‰ ШҐШ±ШіШ§Щ„ Ш§Щ„ШҐЩЉШµШ§Щ„ ШЈЩ€ Щ„Щ‚Ш·Ш© Ш§Щ„ШґШ§ШґШ©.",
        "premium_plan_details": "Ш§Щ„ШЁШ§Щ‚Ш© Ш§Щ„Щ…Ш®ШЄШ§Ш±Ш©: {plan}\nШ§Щ„Щ…ШЇШ©: {days} ЩЉЩ€Щ…\nШ§Щ„ШіШ№Ш±: {price} UZS\nШ§Щ„Ш®ШµЩ…: -{disc}%\n\nШ§Щ„Ш­ШЇЩ€ШЇ:\nШ§Щ„Щ…Щ„ЩЃШ§ШЄ: {files}\nШ§Щ„Щ…Щ€Ш§Ш¶ЩЉШ№: {topics}\n\nШ§Ш¶ШєШ· Ш№Щ„Щ‰ `Ш§Щ„ШЇЩЃШ№` Щ„Щ„Щ…ШЄШ§ШЁШ№Ш©.",
        "admin_only": "Щ„Щ„Щ…ШґШ±ЩЃЩЉЩ† ЩЃЩ‚Ш·.",
        "btn_export_docx": "Щ…Щ„ЩЃ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш± (.docx)",
        "export_working": "Ш¬Ш§Ш±ЩЌ ШҐШ№ШЇШ§ШЇ Щ…Щ„ЩЃ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш±...",
        "export_failed": "ШЄШ№Ш°Ш± ШҐЩ†ШґШ§ШЎ Ш§Щ„Щ…Щ„ЩЃ: {err}",
        "export_docx_caption": "Щ…Щ„ЩЃ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш± (.docx): {title}\nШ§Щ„Щ…Ш№Ш±Щ‘ЩЃ: {id}\nШ§Щ„ШҐШ¬Ш§ШЁШ§ШЄ Щ…Щ€Ш¬Щ€ШЇШ© ЩЃЩЉ Щ†Щ‡Ш§ЩЉШ© Ш§Щ„Щ…Щ„ЩЃ.",
        "export_answer_key_title": "Щ…ЩЃШЄШ§Ш­ Ш§Щ„ШҐШ¬Ш§ШЁШ§ШЄ",
        "export_sent_private_notice": "ШЄЩ… ШҐШ±ШіШ§Щ„ Щ…Щ„ЩЃ Ш§Щ„Ш§Ш®ШЄШЁШ§Ш± ШҐЩ„Щ‰ Ш§Щ„ШЇШ±ШЇШґШ© Ш§Щ„Ш®Ш§ШµШ© ШЁЩѓ.",
        "topic_too_long": "Ш§Щ„Щ…Щ€Ш¶Щ€Ш№ Ш·Щ€ЩЉЩ„ Ш¬ШЇЩ‹Ш§. Ш§Щ„Ш­ШЇ Ш§Щ„ШЈЩ‚ШµЩ‰: {n} Ш­Ш±ЩЃЩ‹Ш§.",
        "manual_draft_found": "Щ„ШЇЩЉЩѓ Щ…ШіЩ€ШЇШ© Ш§Ш®ШЄШЁШ§Ш± Щ…Ш­ЩЃЩ€ШёШ©. Щ‡Щ„ ШЄШ±ЩЉШЇ Ш§Щ„Щ…ШЄШ§ШЁШ№Ш©Шџ",
        "btn_manual_continue": "Щ…ШЄШ§ШЁШ№Ш©",
        "btn_manual_restart": "Ш§ШЁШЇШЈ Щ…Щ† Ш¬ШЇЩЉШЇ",
        "btn_edit_answers": "ШЄШ№ШЇЩЉЩ„ Ш§Щ„ШҐШ¬Ш§ШЁШ§ШЄ",
        "btn_prev_page": "в¬…пёЏ Ш§Щ„ШіШ§ШЁЩ‚",
        "btn_next_page": "Ш§Щ„ШЄШ§Щ„ЩЉ вћЎпёЏ",
        "edit_answers_choose_question": "ШЄШ№ШЇЩЉЩ„ Ш§Щ„ШҐШ¬Ш§ШЁШ§ШЄ: Ш§Ш®ШЄШ± Ш±Щ‚Щ… Ш§Щ„ШіШ¤Ш§Щ„ ({count}).",
        "edit_answers_choose_correct": "Ш§Ш®ШЄШ± Ш±Щ‚Щ… Ш§Щ„ШҐШ¬Ш§ШЁШ© Ш§Щ„ШµШ­ЩЉШ­Ш©:",
        "edit_answers_updated": "ШЄЩ… Ш§Щ„Ш­ЩЃШё.",
    },
    "zh": {
        "choose_count": "иЇ·дЅїз”ЁжЊ‰й’®йЂ‰ж‹©йўз›®ж•°й‡ЏгЂ‚",
        "choose_time": "жЇЏйЃ“йўе¤ље°‘з§’пјџз‚№е‡» 20/30/40/50пјЊж€–еЏ‘йЂЃж•°е­—пј€5..600пј‰гЂ‚",
        "need_translation": "йњЂи¦Ѓзї»иЇ‘еђ—пјџ",
        "shuffle_prompt_manual": "иЇ·йЂ‰ж‹©иї™д»Ѕжµ‹иЇ•зљ„ж‰“д№±ж–№ејЏпјљ",
        "btn_shuffle_questions": "еЏЄж‰“д№±йўз›®",
        "btn_shuffle_answers": "еЏЄж‰“д№±з­”жЎ€",
        "btn_shuffle_both": "дё¤иЂ…йѓЅж‰“д№±",
        "btn_shuffle_keep": "дїќжЊЃеЋџйЎєеєЏ",
        "shuffle_strategy_prompt": "йљЏжњєйЎєеєЏеє”е¦‚дЅ•з”џж•€пјџ",
        "btn_shuffle_saved_once": "йљЏжњєдёЂж¬Ўе№¶дїќе­",
        "btn_shuffle_every_run": "жЇЏж¬ЎдЅњз­”йѓЅй‡Ќж–°йљЏжњє",
        "session_missing": "жњЄж‰ѕе€°дјљиЇќгЂ‚иЇ·йЂљиї‡ /menu й‡Ќж–°ејЂе§‹гЂ‚",
        "session_owner_only": "еЏЄжњ‰дјљиЇќж‰Ђжњ‰иЂ…еЏЇд»Ґж“ЌдЅњгЂ‚",
        "count_invalid": "иЇ·йЂ‰ж‹©жЊ‰й’®дё­зљ„йўз›®ж•°й‡ЏгЂ‚",
        "time_invalid": "иЇ·еЏ‘йЂЃз§’ж•°ж•°е­—пј€5..600пј‰пјЊж€–з‚№е‡» 20/30/40/50 жЊ‰й’®д№‹дёЂгЂ‚",
        "accepted": "е·Ідїќе­гЂ‚",
        "count_chosen": "йўз›®ж•°й‡Џпјљ{n}гЂ‚",
        "invalid_button": "ж— ж•€жЊ‰й’®гЂ‚",
        "edit_time_prompt": "иЇ·еЏ‘йЂЃж–°зљ„ж—¶й—ґпјЊж€–йЂ‰ж‹© 20/30/40/50 з§’пј€5..600пј‰пјљ",
        "btn_premium": "й«зє§з‰€",
        "topic_prompt": "ж‚ЁеёЊжњ›ж №жЌ®е“ЄдёЄдё»йўз”џж€ђжµ‹йЄЊпјџ\nиЇ·иѕ“е…Ґдё»йўгЂ‚\nз¤єдѕ‹: `дё–з•Њеђ„е›Ѕй¦–йѓЅ`",
        "file_topic_prompt": "ж‚ЁеёЊжњ›й‡Ќз‚№е›ґз»•дёЉдј ж–‡д»¶дё­зљ„е“ЄдёЄдё»йўз”џж€ђжµ‹йЄЊпјџпј€еЏЇйЂ‰пј‰\nиЇ·иѕ“е…Ґдё»йўгЂ‚\nз¤єдѕ‹: `дё–з•Њеђ„е›Ѕй¦–йѓЅ`",
        "choose_translation_lang": "ж‚ЁеёЊжњ›е°†жµ‹йЄЊзї»иЇ‘ж€ђе“Єз§ЌиЇ­иЁЂпјџ",
        "pages_prompt": "ж‚ЁеёЊжњ›ж №жЌ®е“ЄдёЄйЎµз ЃиЊѓе›ґз”џж€ђжµ‹йЄЊпјџ(1..{total})\nз¤єдѕ‹: `20-30` ж€– `20`",
        "scan_pdf_choose_count": "ж‰«жЏЏз‰€ PDFпјље…± {pages} йЎµгЂ‚\nж€‘еЏЇд»Ґд»ЋжЇЏдёЂйЎµз”џж€ђ 1 йЃ“йўз›®гЂ‚\nиЇ·дЅїз”ЁжЊ‰й’®йЂ‰ж‹©йўз›®ж•°й‡ЏгЂ‚",
        "premium_choose_plan": "иЇ·йЂ‰ж‹©дёЂдёЄй«зє§з‰€еҐ—й¤ђпјљ",
        "premium_status_free": "е…Ќиґ№еҐ—й¤ђпј€дёЂж¬ЎжЂ§пј‰пјљ\nж–‡д»¶йўќеє¦пјљ{f_left}/{f_total}\nдё»йўйўќеє¦пјљ{t_left}/{t_total}\nжњ‰ж•€жњџпјљ{days} е¤©пј€е€°жњџпјљ{until}пј‰",
        "premium_status_premium": "й«зє§з‰€еҐ—й¤ђе·ІејЂйЂљгЂ‚\nжњ‰ж•€жњџи‡іпјљ{until}\nж–‡д»¶йўќеє¦пјљ{f_left}/{f_total}\nдё»йўйўќеє¦пјљ{t_left}/{t_total}",
        "premium_send_screenshot": "иЇ·еЏ‘йЂЃд»ж¬ѕе‡­иЇЃж€–ж€Єе›ѕгЂ‚ж”ЇжЊЃе›ѕз‰‡гЂЃPDF ж€–д»ж¬ѕе‡­иЇЃж–‡жњ¬гЂ‚",
        "receipt_checking": "ж­ЈењЁж ёеЇ№д»ж¬ѕе‡­иЇЃ...",
        "premium_received": "ж‚Ёзљ„з”іиЇ·е·Іж”¶е€°гЂ‚з®Ўзђ†е‘е®Ўж ёд»ж¬ѕеђЋе°†дёєж‚ЁејЂйЂљй«зє§з‰€гЂ‚",
        "premium_need_image": "иЇ·еЏ‘йЂЃд»ж¬ѕе‡­иЇЃж€–ж€Єе›ѕгЂ‚ж”ЇжЊЃе›ѕз‰‡гЂЃPDF ж€–д»ж¬ѕе‡­иЇЃж–‡жњ¬гЂ‚",
        "limit_free_reached": "ж‚Ёзљ„е…Ќиґ№йўќеє¦е·Із”Ёе®ЊгЂ‚иЇ·ејЂйЂљй«зє§з‰€д»Ґз»§з»­дЅїз”ЁгЂ‚",
        "limit_premium_reached": "ж‚Ёзљ„й«зє§з‰€йўќеє¦е·Із”Ёе®ЊгЂ‚\nй«зє§з‰€жњ‰ж•€жњџи‡іпјљ{until}\nе¦‚йњЂз»§з»­дЅїз”ЁпјЊиЇ·з»­иґ№еҐ—й¤ђгЂ‚",
        "premium_plan_1d": "1 е¤©",
        "premium_plan_7d": "7 е¤©",
        "premium_plan_30d": "1 дёЄжњ€",
        "btn_premium_approve": "йЂљиї‡",
        "btn_premium_reject": "ж‹’з»ќ",
        "premium_approved_user": "ж‚Ёзљ„й«зє§з‰€е·ІејЂйЂљгЂ‚\nжњ‰ж•€жњџи‡іпјљ{until}",
        "premium_rejected_user": "ж‚Ёзљ„й«зє§з‰€з”іиЇ·е·Іиў«ж‹’з»ќгЂ‚",
        "btn_pay": "ж”Їд»",
        "payment_card_missing": "жњЄй…ЌзЅ®ж”¶ж¬ѕеЌЎдїЎжЃЇгЂ‚иЇ·иЃ”зі»з®Ўзђ†е‘гЂ‚",
        "payment_card_info": "ж”¶ж¬ѕеЌЎдїЎжЃЇпјљ\n{card}\nжЊЃеЌЎдєєпјљ{holder}\n\nж‰ЂйЂ‰еҐ—й¤ђпјљ{plan}\nеє”д»й‡‘йўќпјљ{price} UZS\n\nд»ж¬ѕеђЋиЇ·еЏ‘йЂЃе‡­иЇЃж€–ж€Єе›ѕгЂ‚",
        "premium_plan_details": "ж‰ЂйЂ‰еҐ—й¤ђпјљ{plan}\nж—¶й•їпјљ{days} е¤©\nд»·ж јпјљ{price} UZS\nдјжѓ пјљ-{disc}%\n\nйўќеє¦пјљ\nж–‡д»¶пјљ{files}\nдё»йўпјљ{topics}\n\nз‚№е‡» `ж”Їд»` з»§з»­гЂ‚",
        "admin_only": "д»…й™ђз®Ўзђ†е‘гЂ‚",
        "btn_export_docx": "жµ‹йЄЊж–‡д»¶ (.docx)",
        "export_working": "ж­ЈењЁз”џж€ђжµ‹йЄЊж–‡д»¶...",
        "export_failed": "ж–‡д»¶е€›е»єе¤±иґҐ: {err}",
        "export_docx_caption": "жµ‹йЄЊж–‡д»¶ (.docx): {title}\nID: {id}\nз­”жЎ€дЅЌдєЋж–‡д»¶жњ«е°ѕгЂ‚",
        "export_answer_key_title": "з­”жЎ€й”®",
        "export_sent_private_notice": "жµ‹йЄЊж–‡д»¶е·ІеЏ‘йЂЃе€°ж‚Ёзљ„з§ЃиЃЉгЂ‚",
        "topic_too_long": "дё»йўиї‡й•їгЂ‚жњЂе¤§й•їеє¦: {n} дёЄе­—з¬¦гЂ‚",
        "manual_draft_found": "дЅ жњ‰дёЂдёЄе·Ідїќе­зљ„ж‰‹еЉЁжµ‹йЄЊиЌ‰зЁїгЂ‚з»§з»­еђ—пјџ",
        "btn_manual_continue": "з»§з»­",
        "btn_manual_restart": "й‡Ќж–°ејЂе§‹",
        "btn_edit_answers": "зј–иѕ‘з­”жЎ€",
        "btn_prev_page": "в¬…пёЏ дёЉдёЂйЎµ",
        "btn_next_page": "дё‹дёЂйЎµ вћЎпёЏ",
        "edit_answers_choose_question": "зј–иѕ‘з­”жЎ€пјљиЇ·йЂ‰ж‹©йўеЏ·пј€{count}пј‰гЂ‚",
        "edit_answers_choose_correct": "иЇ·йЂ‰ж‹©ж­ЈзЎ®з­”жЎ€зј–еЏ·пјљ",
        "edit_answers_updated": "е·Ідїќе­гЂ‚",
    },
    "ko": {
        "choose_count": "лІ„нЉјмњјлЎњ л¬ём њ м€лҐј м„ нѓќн•м„ёмљ”.",
        "choose_time": "л¬ён•­л‹№ лЄ‡ мґ€лЎњ н• к№Њмљ”? 20/30/40/50мќ„ л€„лҐґк±°л‚ м€«мћђлҐј ліґл‚ґм„ёмљ” (5..600).",
        "need_translation": "лІ€м—­мќґ н•„мљ”н•м‹ к°Ђмљ”?",
        "shuffle_prompt_manual": "мќґ н…ЊмЉ¤нЉёмќ м„ћкё° л°©м‹ќмќ„ м„ нѓќн•м„ёмљ”:",
        "btn_shuffle_questions": "л¬ём њл§Њ",
        "btn_shuffle_answers": "л‹µл§Њ",
        "btn_shuffle_both": "л‘ л‹¤",
        "btn_shuffle_keep": "м€њм„њ мњ м§Ђ",
        "shuffle_strategy_prompt": "лћњлЌ¤ м€њм„њлҐј м–ґл–»кІЊ м Ѓмљ©н• к№Њмљ”?",
        "btn_shuffle_saved_once": "н•њ лІ€ м„ћм–ґм„њ м ЂмћҐ",
        "btn_shuffle_every_run": "л§¤ м‹њлЏ„л§€л‹¤ л‹¤м‹њ м„ћкё°",
        "session_missing": "м„ём…мќ„ м°ѕмќ„ м€ м—†мЉµл‹€л‹¤. /menuм—ђм„њ л‹¤м‹њ м‹њмћ‘н•м„ёмљ”.",
        "session_owner_only": "м„ём… м†Њмњ мћђл§Њ м„ нѓќн•  м€ мћ€мЉµл‹€л‹¤.",
        "count_invalid": "лІ„нЉјмњјлЎњ л¬ём њ м€лҐј м„ нѓќн•ґ мЈјм„ёмљ”.",
        "time_invalid": "мґ€лҐј м€«мћђлЎњ ліґл‚ґк±°л‚ (5..600) 20/30/40/50 лІ„нЉј м¤‘ н•л‚лҐј л€„лҐґм„ёмљ”.",
        "accepted": "м ЂмћҐлђм—€мЉµл‹€л‹¤.",
        "count_chosen": "л¬ён•­ м€: {n}.",
        "invalid_button": "мћлЄ»лђњ лІ„нЉјмћ…л‹€л‹¤.",
        "edit_time_prompt": "мѓ€ м‹њк°„мќ„ ліґл‚ґк±°л‚ 20/30/40/50мґ€лҐј м„ нѓќн•м„ёмљ” (5..600):",
        "btn_premium": "н”„л¦¬лЇём—„",
        "topic_prompt": "м–ґл–¤ мЈјм њлҐј кё°м¤ЂмњјлЎњ нЂґм¦€лҐј мѓќм„±н•м‹њкІ мЉµл‹€к№Њ?\nмЈјм њлҐј мћ…л Ґн•ґ мЈјм„ёмљ”.\nм€: `м„ёкі„ м€лЏ„`",
        "file_topic_prompt": "м—…лЎњл“њн•њ нЊЊмќјм—ђм„њ м–ґл–¤ мЈјм њм—ђ м§‘м¤‘н•м—¬ нЂґм¦€лҐј мѓќм„±н•м‹њкІ мЉµл‹€к№Њ? (м„ нѓќ)\nмЈјм њлҐј мћ…л Ґн•ґ мЈјм„ёмљ”.\nм€: `м„ёкі„ м€лЏ„`",
        "choose_translation_lang": "нЂґм¦€лҐј м–ґл–¤ м–ём–ґлЎњ лІ€м—­н•м‹њкІ мЉµл‹€к№Њ?",
        "pages_prompt": "м–ґл–¤ нЋмќґм§Ђ лІ”мњ„лҐј кё°м¤ЂмњјлЎњ нЂґм¦€лҐј мѓќм„±н•м‹њкІ мЉµл‹€к№Њ? (1..{total})\nм€: `20-30` лђлЉ” `20`",
        "scan_pdf_choose_count": "мЉ¤мє”лђњ PDF: {pages}нЋмќґм§Ђ.\nк°Ѓ нЋмќґм§Ђм—ђм„њ 1к°њмќ л¬ём њлҐј мѓќм„±н•  м€ мћ€мЉµл‹€л‹¤.\nлІ„нЉјмњјлЎњ л¬ём њ м€лҐј м„ нѓќн•м„ёмљ”.",
        "premium_choose_plan": "н”„л¦¬лЇём—„ мљ”кё€м њлҐј м„ нѓќн•ґ мЈјм„ёмљ”:",
        "premium_status_free": "л¬ґлЈЊ мљ”кё€м њ(1нљЊ):\nнЊЊмќј н•њлЏ„: {f_left}/{f_total}\nмЈјм њ н•њлЏ„: {t_left}/{t_total}\nмќґмљ© кё°к°„: {days}мќј (л§ЊлЈЊ: {until})",
        "premium_status_premium": "н”„л¦¬лЇём—„ мљ”кё€м њк°Ђ н™њм„±н™”лђм–ґ мћ€мЉµл‹€л‹¤.\nмќґмљ© кё°к°„: {until}\nнЊЊмќј н•њлЏ„: {f_left}/{f_total}\nмЈјм њ н•њлЏ„: {t_left}/{t_total}",
        "premium_send_screenshot": "кІ°м њ мЃм€м¦ќ лђлЉ” мЉ¤нЃ¬л¦°мѓ·мќ„ ліґл‚ґ мЈјм„ёмљ”. мќґлЇём§Ђ, PDF лђлЉ” кІ°м њ н™•мќё н…ЌмЉ¤нЉёлҐј м‚¬мљ©н•  м€ мћ€мЉµл‹€л‹¤.",
        "receipt_checking": "кІ°м њ мЃм€м¦ќмќ„ н™•мќён•лЉ” м¤‘мћ…л‹€л‹¤...",
        "premium_received": "мљ”мІ­мќґ м ‘м€лђм—€мЉµл‹€л‹¤. кґЂл¦¬мћђ н™•мќё н›„ н”„л¦¬лЇём—„мќґ н™њм„±н™”лђ©л‹€л‹¤.",
        "premium_need_image": "кІ°м њ мЃм€м¦ќ лђлЉ” мЉ¤нЃ¬л¦°мѓ·мќ„ ліґл‚ґ мЈјм„ёмљ”. мќґлЇём§Ђ, PDF лђлЉ” кІ°м њ н™•мќё н…ЌмЉ¤нЉёлҐј м‚¬мљ©н•  м€ мћ€мЉµл‹€л‹¤.",
        "limit_free_reached": "л¬ґлЈЊ н•њлЏ„лҐј лЄЁл‘ђ м‚¬мљ©н–€мЉµл‹€л‹¤. кі„м†Ќн•л ¤л©ґ н”„л¦¬лЇём—„ мљ”кё€м њлҐј м„ нѓќн•ґ мЈјм„ёмљ”.",
        "limit_premium_reached": "н”„л¦¬лЇём—„ н•њлЏ„лҐј лЄЁл‘ђ м‚¬мљ©н–€мЉµл‹€л‹¤.\nн”„л¦¬лЇём—„ мќґмљ© кё°к°„: {until}\nкі„м†Ќн•л ¤л©ґ мљ”кё€м њлҐј м—°мћҐн•ґ мЈјм„ёмљ”.",
        "premium_plan_1d": "1мќј",
        "premium_plan_7d": "7мќј",
        "premium_plan_30d": "1к°њм›”",
        "btn_premium_approve": "мЉ№мќё",
        "btn_premium_reject": "к±°м €",
        "premium_approved_user": "н”„л¦¬лЇём—„ мљ”кё€м њк°Ђ н™њм„±н™”лђм—€мЉµл‹€л‹¤.\nмќґмљ© кё°к°„: {until}",
        "premium_rejected_user": "н”„л¦¬лЇём—„ мљ”мІ­мќґ к±°м €лђм—€мЉµл‹€л‹¤.",
        "btn_pay": "кІ°м њн•кё°",
        "payment_card_missing": "кІ°м њ м№ґл“њ м •ліґк°Ђ м„¤м •лђм–ґ мћ€м§Ђ м•ЉмЉµл‹€л‹¤. кґЂл¦¬мћђм—ђкІЊ л¬ёмќн•ґ мЈјм„ёмљ”.",
        "payment_card_info": "кІ°м њ м№ґл“њ м •ліґ:\n{card}\nм€кё€мЈј: {holder}\n\nм„ нѓќн•њ мљ”кё€м њ: {plan}\nкІ°м њ кё€м•Ў: {price} UZS\n\nкІ°м њ н›„ мЃм€м¦ќ лђлЉ” мЉ¤нЃ¬л¦°мѓ·мќ„ ліґл‚ґ мЈјм„ёмљ”.",
        "premium_plan_details": "м„ нѓќн•њ мљ”кё€м њ: {plan}\nкё°к°„: {days}мќј\nк°ЂкІ©: {price} UZS\nн• мќё: -{disc}%\n\nн•њлЏ„:\nнЊЊмќј: {files}\nмЈјм њ: {topics}\n\nкі„м†Ќн•л ¤л©ґ `кІ°м њн•кё°`лҐј л€Њлџ¬ мЈјм„ёмљ”.",
        "admin_only": "кґЂл¦¬мћђ м „мљ©мћ…л‹€л‹¤.",
        "btn_export_docx": "нЂґм¦€ нЊЊмќј (.docx)",
        "export_working": "нЂґм¦€ нЊЊмќјмќ„ мѓќм„±н•лЉ” м¤‘мћ…л‹€л‹¤...",
        "export_failed": "нЊЊмќјмќ„ мѓќм„±н•м§Ђ лЄ»н–€мЉµл‹€л‹¤: {err}",
        "export_docx_caption": "нЂґм¦€ нЊЊмќј (.docx): {title}\nID: {id}\nм •л‹µмќЂ нЊЊмќјмќ л§€м§Ђл§‰м—ђ мћ€мЉµл‹€л‹¤.",
        "export_answer_key_title": "м •л‹µн‘њ",
        "export_sent_private_notice": "нЂґм¦€ нЊЊмќјмќґ к°њмќё м±„нЊ…мњјлЎњ м „м†Ўлђм—€мЉµл‹€л‹¤.",
        "topic_too_long": "мЈјм њк°Ђ л„€л¬ґ к№Ѓл‹€л‹¤. мµњлЊЂ: {n}мћђ.",
        "manual_draft_found": "м ЂмћҐлђњ м€лЏ™ нЂґм¦€ мґ€м•€мќґ мћ€мЉµл‹€л‹¤. кі„м†Ќн• к№Њмљ”?",
        "btn_manual_continue": "кі„м†Ќ",
        "btn_manual_restart": "мІмќЊл¶Ђн„°",
        "btn_edit_answers": "м •л‹µ нЋём§‘",
        "btn_prev_page": "в¬…пёЏ мќґм „",
        "btn_next_page": "л‹¤мќЊ вћЎпёЏ",
        "edit_answers_choose_question": "м •л‹µ нЋём§‘: л¬ём њ лІ€нёлҐј м„ нѓќн•м„ёмљ” ({count}).",
        "edit_answers_choose_correct": "м •л‹µ лІ€нёлҐј м„ нѓќн•м„ёмљ”:",
        "edit_answers_updated": "м ЂмћҐлђЁ.",
    },
}

_FILE_AND_ERROR_OVERRIDES: Dict[str, Dict[str, str]] = {
    "uz": {
        "upload_hint": "Fayl yuboring (.pdf / .docx / .pptx / .xlsx / .txt).\nPDF/PPTX bo'lsa sahifa oralig'ini tanlash majburiy.",
        "upload_hint_noai": "Test faylini yuboring (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nFaylda tayyor savollar bo'lishi kerak: savol + 4 ta variant.\nXLSX uchun ustun tartibi: Savol | To'g'ri javob | Noto'g'ri | Noto'g'ri | Noto'g'ri.",
        "file_type_only": "Faqat .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md yoki .json fayl yuboring.",
        "err_too_many_questions": "Savollar soni juda ko'p.\nIltimos, savollar sonini va sahifa oralig'ini qisqartirgan holda qayta urinib ko'ring.",
    },
    "ru": {
        "upload_hint": "РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» (.pdf / .docx / .pptx / .xlsx / .txt).\nР”Р»СЏ PDF/PPTX РІС‹Р±РѕСЂ РґРёР°РїР°Р·РѕРЅР° СЃС‚СЂР°РЅРёС† РѕР±СЏР·Р°С‚РµР»РµРЅ.",
        "upload_hint_noai": "РћС‚РїСЂР°РІСЊС‚Рµ С„Р°Р№Р» СЃ С‚РµСЃС‚РѕРј (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nР’ С„Р°Р№Р»Рµ РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ РіРѕС‚РѕРІС‹Рµ РІРѕРїСЂРѕСЃС‹: РІРѕРїСЂРѕСЃ + 4 РІР°СЂРёР°РЅС‚Р°.\nР”Р»СЏ XLSX РїРѕСЂСЏРґРѕРє СЃС‚РѕР»Р±С†РѕРІ: Р’РѕРїСЂРѕСЃ | РџСЂР°РІРёР»СЊРЅС‹Р№ РѕС‚РІРµС‚ | РќРµРїСЂР°РІРёР»СЊРЅС‹Р№ | РќРµРїСЂР°РІРёР»СЊРЅС‹Р№ | РќРµРїСЂР°РІРёР»СЊРЅС‹Р№.",
        "file_type_only": "РћС‚РїСЂР°РІСЊС‚Рµ С‚РѕР»СЊРєРѕ С„Р°Р№Р» .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md РёР»Рё .json.",
        "err_too_many_questions": "РЎР»РёС€РєРѕРј РјРЅРѕРіРѕ РІРѕРїСЂРѕСЃРѕРІ.\nРџРѕР¶Р°Р»СѓР№СЃС‚Р°, СѓРјРµРЅСЊС€РёС‚Рµ РєРѕР»РёС‡РµСЃС‚РІРѕ РІРѕРїСЂРѕСЃРѕРІ Рё РґРёР°РїР°Р·РѕРЅ СЃС‚СЂР°РЅРёС† Рё РїРѕРїСЂРѕР±СѓР№С‚Рµ СЃРЅРѕРІР°.",
    },
    "en": {
        "upload_hint": "Send a file (.pdf / .docx / .pptx / .xlsx / .txt).\nFor PDF/PPTX, choosing a page range is required.",
        "upload_hint_noai": "Send a quiz file (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nThe file should already contain ready questions: question + 4 options.\nFor XLSX use columns: Question | Correct answer | Wrong | Wrong | Wrong.",
        "file_type_only": "Please send only a .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md, or .json file.",
        "err_too_many_questions": "There are too many questions.\nPlease reduce the question count and page range, then try again.",
    },
    "de": {
        "upload_hint": "Sende eine Datei (.pdf / .docx / .pptx / .xlsx / .txt).\nFГјr PDF/PPTX ist ein Seitenbereich erforderlich.",
        "upload_hint_noai": "Sende eine Quiz-Datei (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nDie Datei sollte fertige Fragen enthalten: Frage + 4 Antwortoptionen.\nFГјr XLSX nutze die Spalten: Frage | Richtige Antwort | Falsch | Falsch | Falsch.",
        "file_type_only": "Bitte sende nur eine Datei im Format .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md oder .json.",
        "err_too_many_questions": "Es gibt zu viele Fragen.\nBitte verringere die Fragenzahl und den Seitenbereich und versuche es erneut.",
    },
    "tr": {
        "upload_hint": "Bir dosya gГ¶nderin (.pdf / .docx / .pptx / .xlsx / .txt).\nPDF/PPTX iГ§in sayfa aralД±ДџД± seГ§imi zorunludur.",
        "upload_hint_noai": "Bir test dosyasД± gГ¶nderin (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nDosyada hazД±r sorular bulunmalД±dД±r: soru + 4 seГ§enek.\nXLSX iГ§in sГјtun dГјzeni: Soru | DoДџru cevap | YanlД±Еџ | YanlД±Еџ | YanlД±Еџ.",
        "file_type_only": "LГјtfen yalnД±zca .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md veya .json dosyasД± gГ¶nderin.",
        "err_too_many_questions": "Soru sayД±sД± Г§ok fazla.\nLГјtfen soru sayД±sД±nД± ve sayfa aralД±ДџД±nД± azaltД±p tekrar deneyin.",
    },
    "kk": {
        "upload_hint": "Р¤Р°Р№Р» Р¶С–Р±РµСЂС–ТЈС–Р· (.pdf / .docx / .pptx / .xlsx / .txt).\nPDF/PPTX ТЇС€С–РЅ Р±РµС‚ Р°СЂР°Р»С‹Т“С‹РЅ С‚Р°ТЈРґР°Сѓ РјС–РЅРґРµС‚С‚С–.",
        "upload_hint_noai": "РўРµСЃС‚ С„Р°Р№Р»С‹РЅ Р¶С–Р±РµСЂС–ТЈС–Р· (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nР¤Р°Р№Р»РґР° РґР°Р№С‹РЅ СЃТ±СЂР°Т›С‚Р°СЂ Р±РѕР»СѓС‹ РєРµСЂРµРє: СЃТ±СЂР°Т› + 4 РЅТ±СЃТ›Р°.\nXLSX ТЇС€С–РЅ Р±Р°Т“Р°РЅ СЂРµС‚С–: РЎТ±СЂР°Т› | Р”Т±СЂС‹СЃ Р¶Р°СѓР°Рї | ТљР°С‚Рµ | ТљР°С‚Рµ | ТљР°С‚Рµ.",
        "file_type_only": "РўРµРє .pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md РЅРµРјРµСЃРµ .json С„Р°Р№Р»С‹РЅ Р¶С–Р±РµСЂС–ТЈС–Р·.",
        "err_too_many_questions": "РЎТ±СЂР°Т› СЃР°РЅС‹ С‚С‹Рј РєУ©Рї.\nРЎТ±СЂР°Т› СЃР°РЅС‹РЅ Р¶У™РЅРµ Р±РµС‚ Р°СЂР°Р»С‹Т“С‹РЅ Т›С‹СЃТ›Р°СЂС‚С‹Рї, Т›Р°Р№С‚Р° РєУ©СЂС–ТЈС–Р·.",
    },
    "ar": {
        "upload_hint": "ШЈШ±ШіЩ„ Щ…Щ„ЩЃЩ‹Ш§ (.pdf / .docx / .pptx / .xlsx / .txt).\nШЁШ§Щ„Щ†ШіШЁШ© ШҐЩ„Щ‰ PDF/PPTX ЩЉШ¬ШЁ Ш§Ш®ШЄЩЉШ§Ш± Щ†Ш·Ш§Щ‚ Ш§Щ„ШµЩЃШ­Ш§ШЄ.",
        "upload_hint_noai": "ШЈШ±ШіЩ„ Щ…Щ„ЩЃ Ш§Ш®ШЄШЁШ§Ш± (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nЩЉШ¬ШЁ ШЈЩ† ЩЉШ­ШЄЩ€ЩЉ Ш§Щ„Щ…Щ„ЩЃ Ш№Щ„Щ‰ ШЈШіШ¦Щ„Ш© Ш¬Ш§Щ‡ШІШ©: ШіШ¤Ш§Щ„ + 4 Ш®ЩЉШ§Ш±Ш§ШЄ.\nЩЃЩЉ XLSX ЩЉЩѓЩ€Щ† ШЄШ±ШЄЩЉШЁ Ш§Щ„ШЈШ№Щ…ШЇШ©: Ш§Щ„ШіШ¤Ш§Щ„ | Ш§Щ„ШҐШ¬Ш§ШЁШ© Ш§Щ„ШµШ­ЩЉШ­Ш© | Ш®Ш·ШЈ | Ш®Ш·ШЈ | Ш®Ш·ШЈ.",
        "file_type_only": "ЩЉШ±Ш¬Щ‰ ШҐШ±ШіШ§Щ„ Щ…Щ„ЩЃ ШЁШµЩЉШєШ© .pdf ШЈЩ€ .docx ШЈЩ€ .pptx ШЈЩ€ .xlsx ШЈЩ€ .png ШЈЩ€ .jpg ШЈЩ€ .txt ШЈЩ€ .md ШЈЩ€ .json ЩЃЩ‚Ш·.",
        "err_too_many_questions": "Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш© ЩѓШЁЩЉШ± Ш¬ШЇЩ‹Ш§.\nЩЉШ±Ш¬Щ‰ ШЄЩ‚Щ„ЩЉЩ„ Ш№ШЇШЇ Ш§Щ„ШЈШіШ¦Щ„Ш© Щ€Щ†Ш·Ш§Щ‚ Ш§Щ„ШµЩЃШ­Ш§ШЄ Ш«Щ… Ш§Щ„Щ…Ш­Ш§Щ€Щ„Ш© Щ…Ш±Ш© ШЈШ®Ш±Щ‰.",
    },
    "zh": {
        "upload_hint": "еЏ‘йЂЃж–‡д»¶ (.pdf / .docx / .pptx / .xlsx / .txt)гЂ‚\nPDF/PPTX еї…йЎ»йЂ‰ж‹©йЎµз ЃиЊѓе›ґгЂ‚",
        "upload_hint_noai": "еЏ‘йЂЃжµ‹йЄЊж–‡д»¶ (.txt / .json / .docx / .pptx / .pdf / .xlsx)гЂ‚\nж–‡д»¶еє”еЊ…еђ«зЋ°ж€ђйўз›®пјљйўз›® + 4 дёЄйЂ‰йЎ№гЂ‚\nXLSX е€—йЎєеєЏпјљйўз›® | ж­ЈзЎ®з­”жЎ€ | й”™иЇЇ | й”™иЇЇ | й”™иЇЇгЂ‚",
        "file_type_only": "иЇ·д»…еЏ‘йЂЃ .pdfгЂЃ.docxгЂЃ.pptxгЂЃ.xlsxгЂЃ.pngгЂЃ.jpgгЂЃ.txtгЂЃ.md ж€– .json ж–‡д»¶гЂ‚",
        "err_too_many_questions": "йўз›®ж•°й‡Џиї‡е¤љгЂ‚\nиЇ·е‡Џе°‘йўз›®ж•°й‡Џе’ЊйЎµз ЃиЊѓе›ґеђЋй‡ЌиЇ•гЂ‚",
    },
    "ko": {
        "upload_hint": "нЊЊмќјмќ„ ліґл‚ґм„ёмљ” (.pdf / .docx / .pptx / .xlsx / .txt).\nPDF/PPTXмќ кІЅмљ° нЋмќґм§Ђ лІ”мњ„лҐј л°л“њм‹њ м„ нѓќн•ґм•ј н•©л‹€л‹¤.",
        "upload_hint_noai": "нЂґм¦€ нЊЊмќјмќ„ ліґл‚ґм„ёмљ” (.txt / .json / .docx / .pptx / .pdf / .xlsx).\nнЊЊмќјм—ђлЉ” мќґлЇё л¬ён•­мќґ мћ€м–ґм•ј н•©л‹€л‹¤: м§€л¬ё + 4к°њ м„ нѓќм§Ђ.\nXLSX м—ґ м€њм„њ: м§€л¬ё | м •л‹µ | м¤л‹µ | м¤л‹µ | м¤л‹µ.",
        "file_type_only": ".pdf, .docx, .pptx, .xlsx, .png, .jpg, .txt, .md лђлЉ” .json нЊЊмќјл§Њ ліґл‚ґмЈјм„ёмљ”.",
        "err_too_many_questions": "л¬ён•­ м€к°Ђ л„€л¬ґ л§ЋмЉµл‹€л‹¤.\nл¬ён•­ м€м™Ђ нЋмќґм§Ђ лІ”мњ„лҐј м¤„мќё л’¤ л‹¤м‹њ м‹њлЏ„н•ґ мЈјм„ёмљ”.",
    },
}





_TIME_BUTTON_ONLY_OVERRIDES: Dict[str, Dict[str, str]] = {
    "uz": {
        "choose_time": "Har bir savol uchun vaqtni tanlang:",
        "edit_time_prompt": "20/30/40/50/60/70 soniyadan birini tanlang:",
    },
    "ru": {
        "choose_time": "???????? ????? ?? ?????? ??????:",
        "edit_time_prompt": "???????? 20/30/40/50/60/70 ??????:",
    },
    "en": {
        "choose_time": "Choose the time for each question:",
        "edit_time_prompt": "Choose 20/30/40/50/60/70 seconds:",
    },
    "de": {
        "choose_time": "W?hlen Sie die Zeit f?r jede Frage:",
        "edit_time_prompt": "W?hlen Sie 20/30/40/50/60/70 Sekunden:",
    },
    "tr": {
        "choose_time": "Her soru i?in s?reyi se?in:",
        "edit_time_prompt": "20/30/40/50/60/70 saniyeden birini se?in:",
    },
    "kk": {
        "choose_time": "?? ??????? ????????? ??????? ????????:",
        "edit_time_prompt": "20/30/40/50/60/70 ????????? ????? ????????:",
    },
    "ar": {
        "choose_time": "???? ????? ??? ????:",
        "edit_time_prompt": "???? 20/30/40/50/60/70 ?????:",
    },
    "zh": {
        "choose_time": "??????????",
        "edit_time_prompt": "??? 20/30/40/50/60/70 ??",
    },
    "ko": {
        "choose_time": "? ??? ??? ?????:",
        "edit_time_prompt": "20/30/40/50/60/70? ? ??? ?????:",
    },
}


_GROUP_AND_BONUS_OVERRIDES: Dict[str, Dict[str, str]] = {
    "uz": {
        "btn_open_private": "Botga o'tish",
        "private_only_group": "Bu buyruqni bot bilan shaxsiy chatda bajaring.",
        "btn_referral": "Referral bonusi",
        "btn_channel_bonus": "Kanal bonusi",
        "channel_bonus_unavailable": "Kanal bonusi hozircha sozlanmagan.",
        "channel_bonus_prompt": "Kanalga a'zo bo'lsangiz, bonus sifatida 1 ta fayl va 1 ta mavzu testi beriladi.\nKanal: {channel}",
        "channel_bonus_check_fail": "Avval kanalga a'zo bo'ling, keyin qayta tekshiring.",
        "channel_bonus_claimed": "Kanal bonusi allaqachon berilgan.",
        "channel_bonus_granted": "Bonus berildi: +1 fayl va +1 mavzu testi.",
        "btn_bonuses": "Bonuslar",
        "bonuses_menu_text": "Bonuslar bo'limi.\n- Referral bonusi: havolangiz orqali do'st taklif qiling.\n- Kanal bonusi: kanalga a'zo bo'lib +1 fayl va +1 mavzu testi oling.",
        "btn_admin_broadcast_users": "Barcha foydalanuvchilarga xabar",
        "btn_admin_broadcast_groups": "Barcha guruhlarga xabar",
        "admin_broadcast_panel": "Admin yuborish bo'limi. Kerakli yo'nalishni tanlang.",
        "admin_broadcast_users_prompt": "Barcha foydalanuvchilarga yuboriladigan xabar matnini yuboring.",
        "admin_broadcast_groups_prompt": "Barcha guruhlarga yuboriladigan xabar matnini yuboring.",
        "admin_broadcast_text_required": "Iltimos, yuborish uchun matn kiriting.",
        "admin_broadcast_sending": "Xabar yuborilmoqda... Jami qabul qiluvchilar: {total}",
        "admin_broadcast_done": "Yuborish yakunlandi. Jami: {total}, yetdi: {sent}, xato: {failed}.",
        "admin_broadcast_no_targets": "Yuborish uchun chatlar topilmadi.",
        "admin_broadcast_cancelled": "Xabar yuborish bekor qilindi.",
    },
    "ru": {
        "btn_open_private": "??????? ????",
        "private_only_group": "??????????, ??????????? ??? ??????? ? ?????? ???? ? ?????.",
        "btn_referral": "??????????? ?????",
        "btn_channel_bonus": "????? ?? ?????",
        "channel_bonus_unavailable": "????? ?? ????? ???? ?? ????????.",
        "channel_bonus_prompt": "??????????? ?? ????? ? ???????? ?????: +1 ???????? ????? ? +1 ???? ?? ????.\n?????: {channel}",
        "channel_bonus_check_fail": "??????? ??????????? ?? ?????, ????? ????????? ?????.",
        "channel_bonus_claimed": "????? ?? ????? ??? ???????.",
        "channel_bonus_granted": "????? ???????: +1 ???? ? +1 ???? ?? ????.",
        "btn_bonuses": "??????",
        "bonuses_menu_text": "?????? ???????.\n- ??????????? ?????: ??????????? ?????? ?? ????? ??????.\n- ????? ?? ?????: ??????????? ?? ????? ? ???????? +1 ???? ? +1 ???? ?? ????.",
        "btn_admin_broadcast_users": "????????? ???? ?????????????",
        "btn_admin_broadcast_groups": "????????? ???? ???????",
        "admin_broadcast_panel": "?????? ???????? ??? ??????????????. ???????? ???????????.",
        "admin_broadcast_users_prompt": "????????? ????? ????????? ??? ???? ?????????????.",
        "admin_broadcast_groups_prompt": "????????? ????? ????????? ??? ???? ?????.",
        "admin_broadcast_text_required": "??????????, ????????? ????? ?????????.",
        "admin_broadcast_sending": "???? ????????... ????? ???????????: {total}",
        "admin_broadcast_done": "???????? ?????????. ?????: {total}, ??????????: {sent}, ??????: {failed}.",
        "admin_broadcast_no_targets": "???? ??? ???????? ?? ???????.",
        "admin_broadcast_cancelled": "???????? ????????.",
    },
    "en": {
        "btn_open_private": "Open bot chat",
        "private_only_group": "Please use this command in the bot's private chat.",
        "btn_referral": "Referral bonus",
        "btn_channel_bonus": "Channel bonus",
        "channel_bonus_unavailable": "Channel bonus is not configured yet.",
        "channel_bonus_prompt": "Join the channel to get a bonus: +1 file upload and +1 topic quiz.\nChannel: {channel}",
        "channel_bonus_check_fail": "Please join the channel first, then check again.",
        "channel_bonus_claimed": "Channel bonus has already been claimed.",
        "channel_bonus_granted": "Bonus granted: +1 file and +1 topic quiz.",
        "btn_bonuses": "Bonuses",
        "bonuses_menu_text": "Bonuses section.\n- Referral bonus: invite friends with your referral link.\n- Channel bonus: join the channel and get +1 file and +1 topic quiz.",
        "btn_admin_broadcast_users": "Message all users",
        "btn_admin_broadcast_groups": "Message all groups",
        "admin_broadcast_panel": "Admin broadcast section. Choose where to send the message.",
        "admin_broadcast_users_prompt": "Send the message text for all users.",
        "admin_broadcast_groups_prompt": "Send the message text for all groups.",
        "admin_broadcast_text_required": "Please send the message text.",
        "admin_broadcast_sending": "Sending message... Total recipients: {total}",
        "admin_broadcast_done": "Broadcast finished. Total: {total}, delivered: {sent}, failed: {failed}.",
        "admin_broadcast_no_targets": "No chats found for broadcasting.",
        "admin_broadcast_cancelled": "Broadcast cancelled.",
    },
    "de": {
        "btn_open_private": "Bot privat ?ffnen",
        "private_only_group": "Bitte nutzen Sie diesen Befehl im privaten Chat mit dem Bot.",
        "btn_referral": "Empfehlungsbonus",
        "btn_channel_bonus": "Kanalbonus",
        "channel_bonus_unavailable": "Der Kanalbonus ist noch nicht eingerichtet.",
        "channel_bonus_prompt": "Treten Sie dem Kanal bei und erhalten Sie einen Bonus: +1 Datei und +1 Themenquiz.\nKanal: {channel}",
        "channel_bonus_check_fail": "Bitte treten Sie zuerst dem Kanal bei und pr?fen Sie dann erneut.",
        "channel_bonus_claimed": "Der Kanalbonus wurde bereits beansprucht.",
        "channel_bonus_granted": "Bonus erhalten: +1 Datei und +1 Themenquiz.",
        "btn_bonuses": "Boni",
        "bonuses_menu_text": "Bonusbereich.\n- Empfehlungsbonus: Laden Sie Freunde ?ber Ihren Link ein.\n- Kanalbonus: Treten Sie dem Kanal bei und erhalten Sie +1 Datei und +1 Themenquiz.",
        "btn_admin_broadcast_users": "Nachricht an alle Nutzer",
        "btn_admin_broadcast_groups": "Nachricht an alle Gruppen",
        "admin_broadcast_panel": "Admin-Broadcast-Bereich. W?hlen Sie das Ziel aus.",
        "admin_broadcast_users_prompt": "Senden Sie den Nachrichtentext f?r alle Nutzer.",
        "admin_broadcast_groups_prompt": "Senden Sie den Nachrichtentext f?r alle Gruppen.",
        "admin_broadcast_text_required": "Bitte senden Sie einen Nachrichtentext.",
        "admin_broadcast_sending": "Nachricht wird gesendet... Empf?nger insgesamt: {total}",
        "admin_broadcast_done": "Versand abgeschlossen. Insgesamt: {total}, zugestellt: {sent}, fehlgeschlagen: {failed}.",
        "admin_broadcast_no_targets": "Keine Chats f?r den Versand gefunden.",
        "admin_broadcast_cancelled": "Versand abgebrochen.",
    },
    "tr": {
        "btn_open_private": "Botu a?",
        "private_only_group": "L?tfen bu komutu botun ?zel sohbetinde kullan?n.",
        "btn_referral": "Referans bonusu",
        "btn_channel_bonus": "Kanal bonusu",
        "channel_bonus_unavailable": "Kanal bonusu hen?z ayarlanmad?.",
        "channel_bonus_prompt": "Kanala kat?l?n ve bonus al?n: +1 dosya y?kleme ve +1 konu testi.\nKanal: {channel}",
        "channel_bonus_check_fail": "?nce kanala kat?l?n, sonra tekrar kontrol edin.",
        "channel_bonus_claimed": "Kanal bonusu zaten al?nd?.",
        "channel_bonus_granted": "Bonus verildi: +1 dosya ve +1 konu testi.",
        "btn_bonuses": "Bonuslar",
        "bonuses_menu_text": "Bonuslar b?l?m?.\n- Referans bonusu: arkada?lar?n?z? ba?lant?n?zla davet edin.\n- Kanal bonusu: kanala kat?l?n ve +1 dosya ile +1 konu testi kazan?n.",
        "btn_admin_broadcast_users": "T?m kullan?c?lara mesaj",
        "btn_admin_broadcast_groups": "T?m gruplara mesaj",
        "admin_broadcast_panel": "Y?netici duyuru b?l?m?. Hedefi se?in.",
        "admin_broadcast_users_prompt": "T?m kullan?c?lara g?nderilecek mesaj? yollay?n.",
        "admin_broadcast_groups_prompt": "T?m gruplara g?nderilecek mesaj? yollay?n.",
        "admin_broadcast_text_required": "L?tfen g?nderilecek mesaj? yaz?n.",
        "admin_broadcast_sending": "Mesaj g?nderiliyor... Toplam al?c?: {total}",
        "admin_broadcast_done": "G?nderim tamamland?. Toplam: {total}, ula?t?: {sent}, hata: {failed}.",
        "admin_broadcast_no_targets": "G?nderim i?in sohbet bulunamad?.",
        "admin_broadcast_cancelled": "G?nderim iptal edildi.",
    },
    "kk": {
        "btn_open_private": "????? ???",
        "private_only_group": "??? ???????? ?????? ???? ????? ?????????.",
        "btn_referral": "Referral ??????",
        "btn_channel_bonus": "???? ??????",
        "channel_bonus_unavailable": "???? ?????? ??? ???????????.",
        "channel_bonus_prompt": "?????? ???????, ????? ??????: +1 ???? ?????? ???? +1 ?????????? ????.\n????: {channel}",
        "channel_bonus_check_fail": "??????? ?????? ???????, ????? ????? ??????????.",
        "channel_bonus_claimed": "???? ?????? ????? ????????.",
        "channel_bonus_granted": "????? ???????: +1 ???? ???? +1 ?????????? ????.",
        "btn_bonuses": "????????",
        "bonuses_menu_text": "???????? ??????.\n- Referral ??????: ???????????? ?????????? ?????? ?????????.\n- ???? ??????: ?????? ???????, +1 ???? ???? +1 ?????????? ???? ??????.",
        "btn_admin_broadcast_users": "?????? ?????????????? ?????",
        "btn_admin_broadcast_groups": "?????? ???????? ?????",
        "admin_broadcast_panel": "?????? ?????? ??????. ??????? ??????? ????????.",
        "admin_broadcast_users_prompt": "?????? ?????????????? ??????????? ????? ??????? ?????????.",
        "admin_broadcast_groups_prompt": "?????? ???????? ??????????? ????? ??????? ?????????.",
        "admin_broadcast_text_required": "??????????? ??????? ?????????.",
        "admin_broadcast_sending": "????? ??????????... ?????? ????? ????: {total}",
        "admin_broadcast_done": "?????? ????????. ???????: {total}, ?????: {sent}, ????: {failed}.",
        "admin_broadcast_no_targets": "???????? ???????? ?????? ?????????.",
        "admin_broadcast_cancelled": "?????? ??????????.",
    },
    "ar": {
        "btn_open_private": "??? ?????",
        "private_only_group": "???? ??????? ??? ????? ?? ???????? ?????? ?? ?????.",
        "btn_referral": "?????? ???????",
        "btn_channel_bonus": "?????? ??????",
        "channel_bonus_unavailable": "?????? ?????? ??? ????? ???.",
        "channel_bonus_prompt": "???? ??? ?????? ????? ??? ??????: +1 ??? ??? ? +1 ?????? ??????.\n??????: {channel}",
        "channel_bonus_check_fail": "???? ???????? ??? ?????? ????? ?? ????? ??????.",
        "channel_bonus_claimed": "?? ?????? ??? ?????? ?????? ??????.",
        "channel_bonus_granted": "?? ??? ????????: +1 ??? ? +1 ?????? ??????.",
        "btn_bonuses": "????????",
        "bonuses_menu_text": "??? ????????.\n- ?????? ???????: ???? ??????? ??? ???? ??????? ????? ??.\n- ?????? ??????: ???? ??? ?????? ????? ??? +1 ??? ? +1 ?????? ??????.",
        "btn_admin_broadcast_users": "????? ??? ???? ??????????",
        "btn_admin_broadcast_groups": "????? ??? ???? ?????????",
        "admin_broadcast_panel": "??? ??????? ??????. ???? ??????.",
        "admin_broadcast_users_prompt": "???? ?? ??????? ????? ??????????.",
        "admin_broadcast_groups_prompt": "???? ?? ??????? ????? ?????????.",
        "admin_broadcast_text_required": "???? ????? ?? ???????.",
        "admin_broadcast_sending": "???? ???????... ?????? ?????????: {total}",
        "admin_broadcast_done": "????? ???????. ????????: {total}? ?? ???????: {sent}? ???: {failed}.",
        "admin_broadcast_no_targets": "?? ??? ?????? ??? ??????? ???????.",
        "admin_broadcast_cancelled": "?? ????? ???????.",
    },
    "zh": {
        "btn_open_private": "?????",
        "private_only_group": "???????????????",
        "btn_referral": "????",
        "btn_channel_bonus": "????",
        "channel_bonus_unavailable": "?????????",
        "channel_bonus_prompt": "???????????+1 ?????? +1 ??????\n???{channel}",
        "channel_bonus_check_fail": "?????????????",
        "channel_bonus_claimed": "????????",
        "channel_bonus_granted": "??????+1 ?????? +1 ??????",
        "btn_bonuses": "??",
        "bonuses_menu_text": "?????\n- ??????????????????\n- ????????????? +1 ?????? +1 ??????",
        "btn_admin_broadcast_users": "?????????",
        "btn_admin_broadcast_groups": "?????????",
        "admin_broadcast_panel": "????????????????",
        "admin_broadcast_users_prompt": "????????????????",
        "admin_broadcast_groups_prompt": "????????????????",
        "admin_broadcast_text_required": "????????????",
        "admin_broadcast_sending": "??????... ??????{total}",
        "admin_broadcast_done": "????????{total}????{sent}????{failed}?",
        "admin_broadcast_no_targets": "?????????",
        "admin_broadcast_cancelled": "??????",
    },
    "ko": {
        "btn_open_private": "? ??",
        "private_only_group": "? ??? ??? ?? ???? ??? ???.",
        "btn_referral": "?? ???",
        "btn_channel_bonus": "?? ???",
        "channel_bonus_unavailable": "?? ???? ?? ???? ?????.",
        "channel_bonus_prompt": "??? ???? ???? ?? ? ????: ?? +1, ?? ?? +1.\n??: {channel}",
        "channel_bonus_check_fail": "?? ??? ??? ? ?? ??? ???.",
        "channel_bonus_claimed": "?? ???? ?? ???????.",
        "channel_bonus_granted": "???? ???????: ?? +1, ?? ?? +1.",
        "btn_bonuses": "???",
        "bonuses_menu_text": "??? ?????.\n- ?? ???: ?? ??? ??? ?????.\n- ?? ???: ??? ???? ?? +1, ?? ?? +1? ????.",
        "btn_admin_broadcast_users": "?? ????? ???",
        "btn_admin_broadcast_groups": "?? ??? ???",
        "admin_broadcast_panel": "??? ?? ?????. ?? ??? ?????.",
        "admin_broadcast_users_prompt": "?? ????? ?? ??? ??? ????.",
        "admin_broadcast_groups_prompt": "?? ??? ?? ??? ??? ????.",
        "admin_broadcast_text_required": "?? ??? ???? ??? ???.",
        "admin_broadcast_sending": "??? ?? ?... ? ???: {total}",
        "admin_broadcast_done": "??? ???????. ??: {total}, ??: {sent}, ??: {failed}.",
        "admin_broadcast_no_targets": "??? ??? ????.",
        "admin_broadcast_cancelled": "??? ???????.",
    },
}


_FILE_DIFFICULTY_OVERRIDES = {
    "uz": {
        "choose_difficulty_optional_file": "Qiyinlik darajasini tanlang (ixtiyoriy). Tanlangan darajaga qarab testlar tuziladi:",
        "btn_diff_skip": "O'tkazib yuborish",
    },
    "ru": {
        "choose_difficulty_optional_file": "???????? ??????? ????????? (?????????????). ???? ????? ????????? ? ?????? ?????????? ??????:",
        "btn_diff_skip": "??????????",
    },
    "en": {
        "choose_difficulty_optional_file": "Choose difficulty (optional). The quiz will be generated based on the selected level:",
        "btn_diff_skip": "Skip",
    },
    "de": {
        "choose_difficulty_optional_file": "W?hlen Sie den Schwierigkeitsgrad (optional). Der Test wird entsprechend erstellt:",
        "btn_diff_skip": "?berspringen",
    },
    "tr": {
        "choose_difficulty_optional_file": "Zorluk seviyesini se?in (iste?e ba?l?). Test se?ti?iniz seviyeye g?re haz?rlan?r:",
        "btn_diff_skip": "Ge?",
    },
    "kk": {
        "choose_difficulty_optional_file": "??????? ???????? ???????? (???????? ????). ???? ????????? ???????? ??? ?????????????:",
        "btn_diff_skip": "??????? ??????",
    },
    "ar": {
        "choose_difficulty_optional_file": "???? ????? ??????? (???????). ???? ????? ???????? ??? ??????? ??????:",
        "btn_diff_skip": "????",
    },
    "zh": {
        "choose_difficulty_optional_file": "?????????????????????",
        "btn_diff_skip": "??",
    },
    "ko": {
        "choose_difficulty_optional_file": "???? ?????(?? ??). ??? ??? ?? ??? ?????:",
        "btn_diff_skip": "????",
    },
}


_MISSING_LOCALIZATION_OVERRIDES = {'ru': {'admin_users_stats': '?????????? ??????????????\n'
                             '\n'
                             '????? ?????????????: {total}\n'
                             '????? ?? 24 ????: {joined}\n'
                             '???????? ?? 24 ????: {active}\n'
                             '????? ??????: {quizzes}\n'
                             '??????? ?? 24 ????: {attempts}',
        'btn_admin_users': '?????????? ??????',
        'btn_check_sub': '?????????',
        'btn_join_channel': '??????????? ?? ?????',
        'menu_help': '??????? ????:\n'
                     '- ???????? ?????: ?????????? ????? ?? ????? ??? ??????????? ?????.\n'
                     '- ???? ?? ????: ??????? ????, ?????????, ?????????? ? ?????.\n'
                     '- ????? ????: ?????????? ???? ???????.\n'
                     '- ???? ??????????: ????????? ???? ????.\n'
                     '- Premium: ?????????? ??????? ? ??????? ????????.\n'
                     '- ??????: ??????????? ? ?????-??????.',
        'must_join_channel': '????? ???????????? ?????, ??????????? ?? ?????: {channel}\n????? ??????? "?????????".',
        'sub_check_fail': '?? ??? ?? ??????????? ?? ?????.',
        'sub_check_ok': '???????! ?????? ?? ?????? ???????????? ?????.',
        'sub_required_alert': '????? ???????????? ?????, ??????????? ?? ?????.'},
 'de': {'admin_users_stats': 'Admin-Statistik\n'
                             '\n'
                             'Nutzer insgesamt: {total}\n'
                             'Neu in 24 Stunden: {joined}\n'
                             'Aktiv in 24 Stunden: {active}\n'
                             'Tests insgesamt: {quizzes}\n'
                             'Versuche in 24 Stunden: {attempts}',
        'btn_admin_users': 'Admin-Statistik',
        'btn_check_sub': 'Pr?fen',
        'btn_join_channel': 'Kanal beitreten',
        'btn_topic_continue_anyway': 'Trotzdem fortfahren',
        'done_line': 'Abgeschlossen: Seiten {p_from}-{p_to}, Fragen {n}.\n',
        'import_partial': 'Im Dateiinhalt wurden {found} Fragen gefunden (angefordert: {wanted}). Erweitern Sie den Bereich oder pr?fen '
                          'Sie das Format.',
        'menu_help': 'Men?bereiche:\n'
                     '- Datei hochladen: Erstellen Sie Tests aus Dateien oder E-Books.\n'
                     '- Test nach Thema: W?hlen Sie Thema, Schwierigkeit, Anzahl und Zeit.\n'
                     '- Neuer Test: Erstellen Sie Fragen manuell.\n'
                     '- Oberfl?chensprache: ?ndern Sie die Sprache des Bots.\n'
                     '- Premium: Limits erh?hen und Premium kaufen.\n'
                     '- Boni: Empfehlungs- und Kanalboni.',
        'must_join_channel': 'Um den Bot zu nutzen, treten Sie dem Kanal bei: {channel}\nTippen Sie danach auf "Pr?fen".',
        'shuffle_prompt_ai': 'W?hlen Sie die Mischart:',
        'sub_check_fail': 'Sie sind dem Kanal noch nicht beigetreten.',
        'sub_check_ok': 'Danke! Sie k?nnen den Bot jetzt verwenden.',
        'sub_required_alert': 'Bitte treten Sie dem Kanal bei, um den Bot zu nutzen.',
        'topic_searching': 'Quelle wird gesucht (Buch/Artikel)...',
        'topic_source_found': 'Quelle gefunden: {title}\nJetzt w?hlen wir die Testeinstellungen.',
        'topic_source_not_found': 'Keine eindeutige Quelle gefunden. F?r bessere Ergebnisse laden Sie eine Datei hoch oder pr?zisieren Sie '
                                  'das Thema.'},
 'tr': {'admin_users_stats': 'Y?netici istatistikleri\n'
                             '\n'
                             'Toplam kullan?c?: {total}\n'
                             'Son 24 saatte kat?lan: {joined}\n'
                             'Son 24 saatte aktif: {active}\n'
                             'Toplam test: {quizzes}\n'
                             'Son 24 saatte ??z?m: {attempts}',
        'btn_admin_users': 'Y?netici istatistikleri',
        'btn_check_sub': 'Kontrol et',
        'btn_join_channel': 'Kanala kat?l',
        'btn_topic_continue_anyway': 'Yine de devam et',
        'done_line': 'Tamamland?: sayfa {p_from}-{p_to}, soru {n}.\n',
        'import_partial': 'Dosyada {found} soru bulundu (istenen: {wanted}). Aral??? geni?letin veya bi?imi kontrol edin.',
        'menu_help': 'Men? b?l?mleri:\n'
                     '- Dosya y?kleme: dosya veya e-kitaptan test olu?turabilirsiniz.\n'
                     '- Konuya g?re test: konu, zorluk, say? ve s?re se?ebilirsiniz.\n'
                     '- Yeni test: sorular? elle olu?turabilirsiniz.\n'
                     '- Aray?z dili: botun dilini de?i?tirebilirsiniz.\n'
                     '- Premium: limitleri art?rma ve premium alma b?l?m?.\n'
                     '- Bonuslar: referans ve kanal bonuslar?.',
        'must_join_channel': 'Botu kullanmak i?in kanala kat?l?n: {channel}\nArd?ndan "Kontrol et" d??mesine bas?n.',
        'shuffle_prompt_ai': 'Kar??t?rma t?r?n? se?in:',
        'sub_check_fail': 'Hen?z kanala kat?lmad?n?z.',
        'sub_check_ok': 'Te?ekk?rler! Art?k botu kullanabilirsiniz.',
        'sub_required_alert': 'Botu kullanmak i?in kanala kat?l?n.',
        'topic_searching': 'Kaynak aran?yor (kitap/makale)...',
        'topic_source_found': 'Kaynak bulundu: {title}\n?imdi test ayarlar?n? se?elim.',
        'topic_source_not_found': 'Belirgin bir kaynak bulunamad?. Daha iyi sonu? i?in dosya y?kleyin veya konuyu daha net yaz?n.'},
 'kk': {'admin_users_stats': '?????? ????????????\n'
                             '\n'
                             '?????? ???????????: {total}\n'
                             '????? 24 ??????? ????????: {joined}\n'
                             '????? 24 ??????? ????????: {active}\n'
                             '?????? ????: {quizzes}\n'
                             '????? 24 ????????? ???????: {attempts}',
        'btn_admin_users': '?????? ????????????',
        'btn_check_sub': '???????',
        'btn_join_channel': '?????? ??????',
        'btn_topic_continue_anyway': '????? ?? ??????????',
        'done_line': '????????: {p_from}-{p_to} ??????, {n} ?????.\n',
        'import_partial': '??????? {found} ????? ??????? (?????????: {wanted}). ???????? ?????????? ?? ???????? ??????????.',
        'menu_help': '????? ?????????:\n'
                     '- ???? ??????: ???? ?? ?????????? ???????? ???? ????? ??????.\n'
                     '- ??????? ??????? ????: ???????, ???????, ???? ??? ??????? ????????.\n'
                     '- ???? ????: ?????????? ?????? ??????? ???? ???????.\n'
                     '- ????????? ????: ??? ????? ??????????.\n'
                     '- Premium: ??????? ??????? ???? premium ???.\n'
                     '- ????????: referral ???? ???? ??????.',
        'must_join_channel': '????? ????????? ???? ?????? ?????????: {channel}\n????? ????? "???????" ???????? ???????.',
        'shuffle_prompt_ai': '?????????? ????? ????????:',
        'sub_check_fail': '??? ??? ?????? ????????????.',
        'sub_check_ok': '??????! ???? ????? ????????? ??????.',
        'sub_required_alert': '????? ????????? ???? ?????? ?????????.',
        'topic_searching': '???????? ???????? (?????/??????)...',
        'topic_source_found': '???????? ???????: {title}\n???? ???? ????????????? ?????????.',
        'topic_source_not_found': '????? ???????? ?????????. ????? ?????? ???? ???? ???????? ?????? ????????? ??????????.'},
 'ar': {'admin_users_stats': '??????? ??????\n'
                             '\n'
                             '?????? ??????????: {total}\n'
                             '????? ???? 24 ????: {joined}\n'
                             '??????? ???? 24 ????: {active}\n'
                             '?????? ??????????: {quizzes}\n'
                             '????????? ???? 24 ????: {attempts}',
        'btn_admin_users': '??????? ??????',
        'btn_check_sub': '????',
        'btn_join_channel': '???????? ??? ??????',
        'btn_topic_continue_anyway': '???????? ??? ???',
        'done_line': '?????: ??????? {p_from}-{p_to}? ??????? {n}.\n',
        'import_partial': '?? ?????? ??? {found} ?????? ?? ????? (???????: {wanted}). ???? ?????? ?? ???? ?? ???????.',
        'menu_help': '????? ???????:\n'
                     '- ??? ???: ???? ???????? ?? ??? ?? ???? ????????.\n'
                     '- ?????? ??? ???????: ???? ??????? ???????? ?????? ??????.\n'
                     '- ?????? ????: ???? ??????? ??????.\n'
                     '- ??? ???????: ???? ??? ?????.\n'
                     '- Premium: ????? ?????? ????? ?????????.\n'
                     '- ????????: ????? ??????? ??????.',
        'must_join_channel': '???????? ?????? ???? ??? ??????: {channel}\n?? ???? "????".',
        'shuffle_prompt_ai': '???? ??? ?????:',
        'sub_check_fail': '??? ?? ???? ??? ?????? ???.',
        'sub_check_ok': '?????! ????? ???? ??????? ?????.',
        'sub_required_alert': '??? ???????? ??? ?????? ???????? ?????.',
        'topic_searching': '???? ????? ?? ???? (????/????)...',
        'topic_source_found': '?? ?????? ??? ??????: {title}\n????? ????? ??????? ????????.',
        'topic_source_not_found': '?? ??? ?????? ??? ???? ????. ?????? ??? ????? ????? ???? ????? ?? ???? ??????? ????.'},
 'zh': {'admin_users_stats': '?????\n\n?????{total}\n24??????{joined}\n24??????{active}\n?????{quizzes}\n24????????{attempts}',
        'btn_admin_users': '?????',
        'btn_check_sub': '??',
        'btn_join_channel': '????',
        'btn_topic_continue_anyway': '????',
        'done_line': '???? {p_from}-{p_to} ???? {n} ??\n',
        'import_partial': '????? {found} ??????{wanted}?????????????',
        'menu_help': '?????\n'
                     '- ???????????????????\n'
                     '- ?????????????????????\n'
                     '- ????????????\n'
                     '- ???????????????\n'
                     '- Premium????????????\n'
                     '- ?????????????',
        'must_join_channel': '?????????????{channel}\n?????????',
        'shuffle_prompt_ai': '????????',
        'sub_check_fail': '?????????',
        'sub_check_ok': '??????????????',
        'sub_required_alert': '?????????????',
        'topic_searching': '?????????/???...',
        'topic_source_found': '??????{title}\n?????????',
        'topic_source_not_found': '?????????????????????????????????'},
 'ko': {'admin_users_stats': '??? ??\n'
                             '\n'
                             '?? ???: {total}\n'
                             '?? 24?? ??: {joined}\n'
                             '?? 24?? ??: {active}\n'
                             '?? ??: {quizzes}\n'
                             '?? 24?? ??: {attempts}',
        'btn_admin_users': '??? ??',
        'btn_check_sub': '??',
        'btn_join_channel': '?? ??',
        'btn_topic_continue_anyway': '??? ??',
        'done_line': '??: {p_from}-{p_to}???, ?? {n}?.\n',
        'import_partial': '???? {found}? ??? ?????(??: {wanted}). ??? ???? ??? ?????.',
        'menu_help': '?? ??:\n'
                     '- ?? ???: ???? ????? ??? ?? ? ????.\n'
                     '- ??? ??: ??, ???, ??, ??? ??? ? ????.\n'
                     '- ? ??: ??? ?? ??? ?? ? ????.\n'
                     '- ????? ??: ?? ??? ??? ? ????.\n'
                     '- Premium: ??? ??? ????? ?????.\n'
                     '- ???: ?? ???? ?? ??????.',
        'must_join_channel': '?? ????? ??? ?????: {channel}\n? ? "??" ??? ????.',
        'shuffle_prompt_ai': '?? ??? ?????:',
        'sub_check_fail': '?? ??? ???? ?????.',
        'sub_check_ok': '?????! ?? ?? ??? ? ????.',
        'sub_required_alert': '?? ????? ??? ???? ???.',
        'topic_searching': '??? ?? ????(?/??)...',
        'topic_source_found': '??? ?????: {title}\n?? ?? ??? ?????.',
        'topic_source_not_found': '??? ??? ?? ?????. ? ?? ??? ?? ??? ?????? ??? ? ????? ?? ???.'}}

def _apply_string_overrides() -> None:
    for code, data in _STRING_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)
    for code, data in _FILE_AND_ERROR_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)
    for code, data in _TIME_BUTTON_ONLY_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)
    for code, data in _GROUP_AND_BONUS_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)
    for code, data in _FILE_DIFFICULTY_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)
    for code, data in _MISSING_LOCALIZATION_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)


_apply_string_overrides()

_FINAL_RUNTIME_OVERRIDES: Dict[str, Dict[str, str]] = {
    "ru": {
        "choose_time": "???????? ????? ??? ??????? ???????:",
        "edit_time_prompt": "???????? 20/30/40/50/60/70 ??????:",
        "btn_open_private": "??????? ????",
        "private_only_group": "??????????, ??????????? ??? ??????? ? ?????? ???? ? ?????.",
        "btn_referral": "??????????? ?????",
        "btn_channel_bonus": "????? ?? ?????",
        "channel_bonus_unavailable": "????? ?? ????? ???? ?? ????????.",
        "channel_bonus_prompt": "??????????? ?? ????? ? ???????? ?????: +1 ???? ? +1 ???? ?? ????.\n?????: {channel}",
        "channel_bonus_check_fail": "??????? ??????????? ?? ?????, ????? ????????? ?????.",
        "channel_bonus_claimed": "????? ?? ????? ??? ???????.",
        "channel_bonus_granted": "????? ????????: +1 ???? ? +1 ???? ?? ????.",
        "btn_bonuses": "??????",
        "bonuses_menu_text": "?????? ???????.\n- ??????????? ?????: ??????????? ?????? ?? ????? ??????.\n- ????? ?? ?????: ??????????? ?? ????? ? ???????? +1 ???? ? +1 ???? ?? ????.",
        "btn_admin_broadcast_users": "????????? ???? ?????????????",
        "btn_admin_broadcast_groups": "????????? ???? ???????",
        "admin_broadcast_panel": "?????? ???????? ??????????????. ???????? ???????????.",
        "admin_broadcast_users_prompt": "????????? ????? ????????? ??? ???? ?????????????.",
        "admin_broadcast_groups_prompt": "????????? ????? ????????? ??? ???? ?????.",
        "admin_broadcast_text_required": "??????????, ????????? ????? ?????????.",
        "admin_broadcast_sending": "???? ????????... ????? ???????????: {total}",
        "admin_broadcast_done": "???????? ?????????. ?????: {total}, ??????????: {sent}, ??????: {failed}.",
        "admin_broadcast_no_targets": "???? ??? ???????? ?? ???????.",
        "admin_broadcast_cancelled": "???????? ????????.",
        "choose_difficulty_optional_file": "???????? ??????? ????????? (?????????????). ???? ????? ?????? ? ?????? ?????????? ??????:",
        "btn_diff_skip": "??????????",
        "admin_users_stats": "?????????? ?????????????\n\n????? ?????????????: {total}\n????? ?? 24 ????: {joined}\n???????? ?? 24 ????: {active}\n????? ??????: {quizzes}\n??????????? ?? 24 ????: {attempts}",
        "btn_admin_users": "?????????? ?????????????",
        "btn_check_sub": "?????????",
        "btn_join_channel": "??????????? ?? ?????",
        "menu_help": "??????? ????:\n- ????????? ????: ????????? ???? ??? ??????????? ?????, ????? ??????? ????.\n- ???? ?? ????: ??????? ????, ???????? ?????????, ?????????? ? ?????.\n- ????? ????: ???????? ???? ???????.\n- ???? ??????????: ???????? ???? ?????????? ????.\n- Premium: ????????? ?????? ? ???????? ???????.\n- ??????: ???????? ???????? ??????.",
        "must_join_channel": "????? ???????????? ?????, ??????????? ?? ?????: {channel}\n????? ??????? \"?????????\".",
        "sub_check_fail": "?? ??? ?? ??????????? ?? ?????.",
        "sub_check_ok": "???????! ?????? ?? ?????? ???????????? ?????.",
        "ref_info": "??????????? ?????:\n{link}\n\n??????????:\n- ????? ??????????: {total}\n- ?????????: {qualified}\n- ? ????????: {pending}\n\n???????: ????? 3 ???????????? ???????????? ???????? ????, ?? ???????? +2 ????? ? +1 ???? ?? ????.\n?? ?????????? ??????: {to_next}",
        "ref_progress": "????????: {n}/3 (???????? {need}).",
        "ref_rewarded": "??????????? ????? ????????: +{files} ??????, +{topics} ???.",
    },
    "de": {
        "choose_time": "Wahlen Sie die Zeit fur jede Frage:",
        "edit_time_prompt": "Wahlen Sie 20/30/40/50/60/70 Sekunden:",
        "btn_open_private": "Bot privat offnen",
        "bonuses_menu_text": "Bonusbereich.\n- Empfehlungsbonus: Laden Sie Freunde uber Ihren Link ein.\n- Kanalbonus: Treten Sie dem Kanal bei und erhalten Sie +1 Datei und +1 Themenquiz.",
        "choose_difficulty_optional_file": "Wahlen Sie den Schwierigkeitsgrad (optional). Der Test wird entsprechend erstellt:",
        "btn_diff_skip": "Uberspringen",
    },
    "tr": {
        "choose_time": "Her soru icin sureyi secin:",
        "edit_time_prompt": "20/30/40/50/60/70 saniyeden birini secin:",
        "btn_open_private": "Botu ac",
        "private_only_group": "Lutfen bu komutu botun ozel sohbetinde kullanin.",
        "bonuses_menu_text": "Bonuslar bolumu.\n- Referans bonusu: arkadaslarinizi baglantinizla davet edin.\n- Kanal bonusu: kanala katilin ve +1 dosya ile +1 konu testi kazanin.",
        "choose_difficulty_optional_file": "Zorluk seviyesini secin (istege bagli). Test sectiginiz seviyeye gore hazirlanir:",
        "btn_diff_skip": "Gec",
    },
    "kk": {
        "choose_time": "Arbir suraq ushin uaqytty tandanyz:",
        "edit_time_prompt": "20/30/40/50/60/70 sekundty tandanyz:",
        "btn_bonuses": "Bonustar",
        "choose_difficulty_optional_file": "Qiindyq dengein tandanyz (mindetti emes). Test tandalgan dengeige saikes qurylady:",
        "btn_diff_skip": "Otikizu",
    },
    "ar": {
        "choose_time": "???? ????? ??? ????:",
        "edit_time_prompt": "???? 20/30/40/50/60/70 ?????:",
        "btn_bonuses": "????????",
        "choose_difficulty_optional_file": "???? ????? ??????? (???????). ???? ????? ???????? ???? ??? ??????? ??????:",
        "btn_diff_skip": "????",
    },
    "zh": {
        "choose_time": "??????????",
        "edit_time_prompt": "??? 20/30/40/50/60/70 ??",
        "btn_bonuses": "??",
        "bonuses_menu_text": "?????\n- ??????????????????\n- ????????????? +1 ???? +1 ??????",
        "choose_difficulty_optional_file": "????????????????????????",
        "btn_diff_skip": "??",
    },
    "ko": {
        "choose_time": "? ??? ??? ?????:",
        "edit_time_prompt": "20/30/40/50/60/70? ? ??? ?????:",
        "btn_bonuses": "???",
        "bonuses_menu_text": "??? ?????.\n- ?? ???: ?? ??? ??? ?????.\n- ?? ???: ??? ???? ?? +1?? ?? ??? +1?? ????.",
        "choose_difficulty_optional_file": "???? ?????(?? ??). ??? ??? ?? ???? ?????:",
        "btn_diff_skip": "????",
    },
}


def _apply_final_runtime_overrides() -> None:
    for code, data in _FINAL_RUNTIME_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)


_apply_final_runtime_overrides()


_FINAL_SAFE_OVERRIDES: Dict[str, Dict[str, str]] = {
    "ru": {
        "choose_time": "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0432\u0440\u0435\u043c\u044f \u0434\u043b\u044f \u043a\u0430\u0436\u0434\u043e\u0433\u043e \u0432\u043e\u043f\u0440\u043e\u0441\u0430:",
        "edit_time_prompt": "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 20/30/40/50/60/70 \u0441\u0435\u043a\u0443\u043d\u0434:",
        "btn_open_private": "\u041e\u0442\u043a\u0440\u044b\u0442\u044c \u0431\u043e\u0442\u0430",
        "private_only_group": "\u041f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430, \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u0443\u0439\u0442\u0435 \u044d\u0442\u0443 \u043a\u043e\u043c\u0430\u043d\u0434\u0443 \u0432 \u043b\u0438\u0447\u043d\u043e\u043c \u0447\u0430\u0442\u0435 \u0441 \u0431\u043e\u0442\u043e\u043c.",
        "btn_referral": "\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0431\u043e\u043d\u0443\u0441",
        "ui_lang_choose": "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u044f\u0437\u044b\u043a \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0430:",
        "ui_lang_saved": "\u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e. \u042f\u0437\u044b\u043a \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0430: {lang_name}",
        "btn_upload": "\u0417\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044c \u0444\u0430\u0439\u043b",
        "btn_topic": "\u0422\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435",
        "btn_newquiz": "\u041d\u043e\u0432\u044b\u0439 \u0442\u0435\u0441\u0442",
        "btn_ui_lang": "\u042f\u0437\u044b\u043a \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0430",
        "btn_premium": "\u041f\u0440\u0435\u043c\u0438\u0443\u043c",
        "btn_channel_bonus": "\u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u043a\u0430\u043d\u0430\u043b",
        "channel_bonus_unavailable": "\u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u043a\u0430\u043d\u0430\u043b \u043f\u043e\u043a\u0430 \u043d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d.",
        "channel_bonus_prompt": "\u041f\u043e\u0434\u043f\u0438\u0448\u0438\u0442\u0435\u0441\u044c \u043d\u0430 \u043a\u0430\u043d\u0430\u043b \u0438 \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u0431\u043e\u043d\u0443\u0441: +1 \u0444\u0430\u0439\u043b \u0438 +1 \u0442\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435.\\n\u041a\u0430\u043d\u0430\u043b: {channel}",
        "channel_bonus_check_fail": "\u0421\u043d\u0430\u0447\u0430\u043b\u0430 \u043f\u043e\u0434\u043f\u0438\u0448\u0438\u0442\u0435\u0441\u044c \u043d\u0430 \u043a\u0430\u043d\u0430\u043b, \u0437\u0430\u0442\u0435\u043c \u043f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u0441\u043d\u043e\u0432\u0430.",
        "channel_bonus_claimed": "\u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u043a\u0430\u043d\u0430\u043b \u0443\u0436\u0435 \u043f\u043e\u043b\u0443\u0447\u0435\u043d.",
        "channel_bonus_granted": "\u0411\u043e\u043d\u0443\u0441 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d: +1 \u0444\u0430\u0439\u043b \u0438 +1 \u0442\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435.",
        "btn_bonuses": "\u0411\u043e\u043d\u0443\u0441\u044b",
        "bonuses_menu_text": "\u0420\u0430\u0437\u0434\u0435\u043b \u0431\u043e\u043d\u0443\u0441\u043e\u0432.\\n- \u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0431\u043e\u043d\u0443\u0441: \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0430\u0439\u0442\u0435 \u0434\u0440\u0443\u0437\u0435\u0439 \u043f\u043e \u0441\u0432\u043e\u0435\u0439 \u0441\u0441\u044b\u043b\u043a\u0435.\\n- \u0411\u043e\u043d\u0443\u0441 \u0437\u0430 \u043a\u0430\u043d\u0430\u043b: \u043f\u043e\u0434\u043f\u0438\u0448\u0438\u0442\u0435\u0441\u044c \u043d\u0430 \u043a\u0430\u043d\u0430\u043b \u0438 \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 +1 \u0444\u0430\u0439\u043b \u0438 +1 \u0442\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435.",
        "btn_admin_broadcast_users": "\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0432\u0441\u0435\u043c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f\u043c",
        "btn_admin_broadcast_groups": "\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435 \u0432\u0441\u0435\u043c \u0433\u0440\u0443\u043f\u043f\u0430\u043c",
        "admin_broadcast_panel": "\u0420\u0430\u0437\u0434\u0435\u043b \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 \u0430\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430. \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u043d\u0430\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435.",
        "admin_broadcast_users_prompt": "\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0442\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0434\u043b\u044f \u0432\u0441\u0435\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439.",
        "admin_broadcast_groups_prompt": "\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0442\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0434\u043b\u044f \u0432\u0441\u0435\u0445 \u0433\u0440\u0443\u043f\u043f.",
        "admin_broadcast_text_required": "\u041f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430, \u043e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0442\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f.",
        "admin_broadcast_sending": "\u0418\u0434\u0435\u0442 \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0430... \u0412\u0441\u0435\u0433\u043e \u043f\u043e\u043b\u0443\u0447\u0430\u0442\u0435\u043b\u0435\u0439: {total}",
        "admin_broadcast_done": "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430. \u0412\u0441\u0435\u0433\u043e: {total}, \u0434\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u043e: {sent}, \u043e\u0448\u0438\u0431\u043e\u043a: {failed}.",
        "admin_broadcast_no_targets": "\u0427\u0430\u0442\u044b \u0434\u043b\u044f \u0440\u0430\u0441\u0441\u044b\u043b\u043a\u0438 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u044b.",
        "admin_broadcast_cancelled": "\u0420\u0430\u0441\u0441\u044b\u043b\u043a\u0430 \u043e\u0442\u043c\u0435\u043d\u0435\u043d\u0430.",
        "choose_difficulty_optional_file": "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0443\u0440\u043e\u0432\u0435\u043d\u044c \u0441\u043b\u043e\u0436\u043d\u043e\u0441\u0442\u0438 (\u043d\u0435\u043e\u0431\u044f\u0437\u0430\u0442\u0435\u043b\u044c\u043d\u043e). \u0422\u0435\u0441\u0442 \u0431\u0443\u0434\u0435\u0442 \u0441\u043e\u0437\u0434\u0430\u043d \u0441 \u0443\u0447\u0435\u0442\u043e\u043c \u0432\u044b\u0431\u0440\u0430\u043d\u043d\u043e\u0433\u043e \u0443\u0440\u043e\u0432\u043d\u044f:",
        "btn_diff_skip": "\u041f\u0440\u043e\u043f\u0443\u0441\u0442\u0438\u0442\u044c",
        "admin_users_stats": "\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439\\n\\n\u0412\u0441\u0435\u0433\u043e \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {total}\\n\u041d\u043e\u0432\u044b\u0445 \u0437\u0430 24 \u0447\u0430\u0441\u0430: {joined}\\n\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0437\u0430 24 \u0447\u0430\u0441\u0430: {active}\\n\u0412\u0441\u0435\u0433\u043e \u0442\u0435\u0441\u0442\u043e\u0432: {quizzes}\\n\u041f\u0440\u043e\u0445\u043e\u0436\u0434\u0435\u043d\u0438\u0439 \u0437\u0430 24 \u0447\u0430\u0441\u0430: {attempts}",
        "btn_admin_users": "\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439",
        "btn_check_sub": "\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c",
        "btn_join_channel": "\u041f\u043e\u0434\u043f\u0438\u0441\u0430\u0442\u044c\u0441\u044f \u043d\u0430 \u043a\u0430\u043d\u0430\u043b",
        "menu_help": "\u0420\u0430\u0437\u0434\u0435\u043b\u044b \u043c\u0435\u043d\u044e:\\n- \u0417\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044c \u0444\u0430\u0439\u043b: \u0437\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u0435 \u0444\u0430\u0439\u043b \u0438\u043b\u0438 \u044d\u043b\u0435\u043a\u0442\u0440\u043e\u043d\u043d\u0443\u044e \u043a\u043d\u0438\u0433\u0443, \u0447\u0442\u043e\u0431\u044b \u0441\u043e\u0437\u0434\u0430\u0442\u044c \u0442\u0435\u0441\u0442.\\n- \u0422\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435: \u0432\u0432\u0435\u0434\u0438\u0442\u0435 \u0442\u0435\u043c\u0443, \u0432\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0441\u043b\u043e\u0436\u043d\u043e\u0441\u0442\u044c, \u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e \u0438 \u0432\u0440\u0435\u043c\u044f.\\n- \u041d\u043e\u0432\u044b\u0439 \u0442\u0435\u0441\u0442: \u0441\u043e\u0437\u0434\u0430\u0439\u0442\u0435 \u0442\u0435\u0441\u0442 \u0432\u0440\u0443\u0447\u043d\u0443\u044e.\\n- \u042f\u0437\u044b\u043a \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0430: \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u0435 \u044f\u0437\u044b\u043a \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0430 \u0431\u043e\u0442\u0430.\\n- Premium: \u0443\u0432\u0435\u043b\u0438\u0447\u044c\u0442\u0435 \u043b\u0438\u043c\u0438\u0442\u044b \u0438 \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 \u043f\u0440\u0435\u043c\u0438\u0443\u043c.\\n- \u0411\u043e\u043d\u0443\u0441\u044b: \u043e\u0442\u043a\u0440\u043e\u0439\u0442\u0435 \u0431\u043e\u043d\u0443\u0441\u043d\u044b\u0439 \u0440\u0430\u0437\u0434\u0435\u043b.",
        "must_join_channel": "\u0427\u0442\u043e\u0431\u044b \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c\u0441\u044f \u0431\u043e\u0442\u043e\u043c, \u043f\u043e\u0434\u043f\u0438\u0448\u0438\u0442\u0435\u0441\u044c \u043d\u0430 \u043a\u0430\u043d\u0430\u043b: {channel}\\n\u0417\u0430\u0442\u0435\u043c \u043d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c\u00bb.",
        "sub_check_fail": "\u0412\u044b \u0435\u0449\u0435 \u043d\u0435 \u043f\u043e\u0434\u043f\u0438\u0441\u0430\u043b\u0438\u0441\u044c \u043d\u0430 \u043a\u0430\u043d\u0430\u043b.",
        "sub_check_ok": "\u0421\u043f\u0430\u0441\u0438\u0431\u043e! \u0422\u0435\u043f\u0435\u0440\u044c \u0432\u044b \u043c\u043e\u0436\u0435\u0442\u0435 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c\u0441\u044f \u0431\u043e\u0442\u043e\u043c.",
        "ref_info": "\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0431\u043e\u043d\u0443\u0441:\\n{link}\\n\\n\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430:\\n- \u0412\u0441\u0435\u0433\u043e \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043e: {total}\\n- \u0417\u0430\u0441\u0447\u0438\u0442\u0430\u043d\u043e: {qualified}\\n- \u0412 \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0438: {pending}\\n\\n\u041f\u0440\u0430\u0432\u0438\u043b\u043e: \u043a\u043e\u0433\u0434\u0430 3 \u043f\u0440\u0438\u0433\u043b\u0430\u0448\u0435\u043d\u043d\u044b\u0445 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f \u0437\u0430\u043f\u0443\u0441\u0442\u044f\u0442 \u0431\u043e\u0442\u0430, \u0432\u044b \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u0435 +2 \u0444\u0430\u0439\u043b\u0430 \u0438 +1 \u0442\u0435\u0441\u0442 \u043f\u043e \u0442\u0435\u043c\u0435.\\n\u0414\u043e \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0433\u043e \u0431\u043e\u043d\u0443\u0441\u0430: {to_next}",
        "ref_progress": "\u041f\u0440\u043e\u0433\u0440\u0435\u0441\u0441: {n}/3 (\u043e\u0441\u0442\u0430\u043b\u043e\u0441\u044c {need}).",
        "ref_rewarded": "\u0420\u0435\u0444\u0435\u0440\u0430\u043b\u044c\u043d\u044b\u0439 \u0431\u043e\u043d\u0443\u0441 \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d: +{files} \u0444\u0430\u0439\u043b\u043e\u0432, +{topics} \u0442\u0435\u043c.",
    },
}


def _apply_final_safe_overrides() -> None:
    for code, data in _FINAL_SAFE_OVERRIDES.items():
        current = _STRINGS.setdefault(code, {})
        current.update(data)


_apply_final_safe_overrides()


def _normalize_loaded_strings() -> None:
    for data in _STRINGS.values():
        if not isinstance(data, dict):
            continue
        for key, value in list(data.items()):
            if isinstance(value, str) and "\\n" in value:
                data[key] = value.replace("\\n", "\n")


_normalize_loaded_strings()


def _repair_mojibake_text(value: str) -> str:
    if not isinstance(value, str) or not value:
        return value
    markers = ("Ð", "Ñ", "Р", "С", "рџ", "Ѓ", "‚", "€", "™", "??")

    def score(s: str) -> int:
        return sum(s.count(m) for m in markers)

    best = value
    best_score = score(value)
    for enc in ("cp1251", "latin1"):
        try:
            cand = value.encode(enc).decode("utf-8")
        except Exception:
            continue
        cand_score = score(cand)
        if cand_score < best_score:
            best = cand
            best_score = cand_score
    return best


def _repair_mojibake_loaded_strings() -> None:
    for data in _STRINGS.values():
        if not isinstance(data, dict):
            continue
        for key, value in list(data.items()):
            if isinstance(value, str):
                data[key] = _repair_mojibake_text(value)


_repair_mojibake_loaded_strings()


def _repair_corrupted_strings() -> None:
    en_base = _STRINGS.get("en", {})
    for data in _STRINGS.values():
        if not isinstance(data, dict):
            continue
        for key, value in list(data.items()):
            if isinstance(value, str) and "????" in value:
                data[key] = en_base.get(key) or value.replace("????", "")
    ru_current = _STRINGS.setdefault("ru", {})
    ru_current["sub_required_alert"] = "??????? ??????????? ?? ?????."


_repair_corrupted_strings()


def _fallback_mojibake_to_english() -> None:
    en_base = _STRINGS.get("en", {})
    markers = ("Ð", "Ñ", "Р", "С", "рџ", "Ѓ", "‚", "€", "™", "??")
    for lang, data in _STRINGS.items():
        if lang == "en" or not isinstance(data, dict):
            continue
        for key, value in list(data.items()):
            if not isinstance(value, str):
                continue
            score = sum(value.count(m) for m in markers)
            if score >= 2 and en_base.get(key):
                data[key] = en_base[key]

    _STRINGS.setdefault("kk", {}).update({
        "btn_premium": "Premium",
        "btn_bonuses": "\u0411\u043e\u043d\u0443\u0441\u0442\u0430\u0440",
        "btn_check_sub": "\u0422\u0435\u043a\u0441\u0435\u0440\u0443",
    })
    _STRINGS.setdefault("ar", {}).update({
        "btn_premium": "بريميوم",
        "btn_bonuses": "المكافآت",
        "btn_check_sub": "تحقق",
    })
    _STRINGS.setdefault("zh", {}).update({
        "btn_premium": "高级版",
        "btn_bonuses": "奖励",
        "btn_diff_skip": "跳过",
        "btn_check_sub": "检查",
    })
    _STRINGS.setdefault("ko", {}).update({
        "btn_premium": "프리미엄",
        "btn_bonuses": "보너스",
        "btn_check_sub": "확인",
    })


_fallback_mojibake_to_english()


_FINAL_CLEAN_OVERRIDES: Dict[str, Dict[str, str]] = {'uz': {'btn_referral': 'Referral bonusi',
        'btn_channel_bonus': 'Kanal bonusi',
        'btn_bonuses': 'Bonuslar',
        'btn_admin_users': 'Statistika foydalanuvchilar',
        'btn_back': 'Orqaga',
        'menu_help': "Menyudagi bo'limlar:\n"
                     '- Fayl yuklash: fayl yoki elektron kitob yuklash orqali test tuzishingiz mumkin.\n'
                     "- Mavzu bo'yicha test: mavzuni yozib, qiyinlik, son va vaqtni tanlab test tuzishingiz mumkin.\n"
                     "- Yangi test: savollarni qo'lda kiritib test tuzishingiz mumkin.\n"
                     "- Interfeys tili: botning interfeys tilini o'zgartirishingiz mumkin.\n"
                     "- Premium: limitlarni oshirish va premium olish bo'limi.\n"
                     "- Bonuslar: referral va kanal bonusi bo'limi.",
        'bonuses_menu_text': "Bonuslar bo'limi.\n"
                             "- Referral bonusi: havolangiz orqali do'st taklif qiling.\n"
                             "- Kanal bonusi: kanalga a'zo bo'lib +1 fayl va +1 mavzu testi oling.",
        'topic_prompt': "Qaysi mavzu bo'yicha test tuzay?\nMavzuni yozing.\nMisol: `Davlatlar poytaxtlari`",
        'file_topic_prompt': "Yuklangan fayl ichidan qaysi mavzuga e'tibor qaratay? (ixtiyoriy)\n"
                             'Mavzuni yozing.\n'
                             'Misol: `Davlatlar poytaxtlari`',
        'need_translation': 'Tarjima kerakmi?',
        'shuffle_strategy_prompt': 'Random tartibni qanday ishlataylik?',
        'choose_translation_lang': 'Qaysi tilga tarjima qilay?',
        'pages_prompt': "📄 Qaysi sahifalar oralig'idan test tuzay? (1..{total})\nMasalan: `20-30` yoki `20`",
        'pages_invalid': "❌ Sahifa oralig'i noto'g'ri.\n1..{total} oralig'ida `20-30` yoki `20` ko'rinishida yuboring.",
        'current_pages': '📌 Hozirgi sahifalar: {p_from}-{p_to}',
        'pages_set': '✅ Sahifalar saqlandi: {p_from}-{p_to}',
        'topic_line': '🧠 Mavzu: {topic}\n',
        'pages_line': '📄 Sahifalar: {p_from}-{p_to}\n',
        'done_line': '✨ Yakun: sahifa {p_from}-{p_to}, savol {n} ta.\n',
        'ai_quiz_ready': '✅ Test tayyor!\n\n📚 Test: {title}\n{topic_line}❓ Savollar: {count} ta\n⏱ Har savol: {sec}s (taxminan {est})\n🆔 ID: {id}\n\n🚀 Boshlash uchun `Testni boshlash` tugmasini bosing.',
        'manual_has_image': 'Savol uchun rasm bormi? Rasm yuboring yoki /skip yozing.',
        'manual_draft_found': "Sizda saqlangan qo'lda tuzilayotgan test bor. Davom ettiraymi?",
        'edit_menu': 'Test: {title}\nSavollar: {count} ta\nHar savol: {sec}s\nID: {id}\n\nNimani tahrirlaymiz?',
        'premium_choose_plan': 'Premium tariflardan birini tanlang:',
        'premium_status_free': 'Bepul tarif (bir martalik):\n'
                               'Fayl limiti: {f_left}/{f_total}\n'
                               'Mavzu limiti: {t_left}/{t_total}\n'
                               'Amal qilish muddati: {days} kun (tugash: {until})',
        'premium_status_premium': 'Premium tarif faol.\n'
                                  'Amal qilish muddati: {until}\n'
                                  'Fayl limiti: {f_left}/{f_total}\n'
                                  'Mavzu limiti: {t_left}/{t_total}',
        'premium_plan_1d': '1 kun',
        'premium_plan_7d': '7 kun',
        'premium_plan_30d': '1 oy',
        'premium_plan_details': 'Tanlangan tarif: {plan}\n'
                                'Davomiyligi: {days} kun\n'
                                'Narxi: {price} UZS\n'
                                'Chegirma: -{disc}%\n'
                                '\n'
                                'Limitlar:\n'
                                'Fayl: {files}\n'
                                'Mavzu: {topics}\n'
                                '\n'
                                "Davom etish uchun `To'lov qilish` tugmasini bosing.",
        'btn_upload': 'Fayl yuklash',
        'btn_topic': "Mavzu bo'yicha test",
        'btn_newquiz': 'Yangi test',
        'btn_ui_lang': 'Interfeys tili',
        'btn_premium': 'Premium'},
 'ru': {'btn_referral': 'Реферальный бонус',
        'btn_channel_bonus': 'Бонус за канал',
        'btn_bonuses': 'Бонусы',
        'btn_admin_users': 'Статистика пользователей',
        'btn_back': 'Назад',
        'menu_help': 'Разделы меню:\n'
                     '- Загрузить файл: загрузите файл или электронную книгу, чтобы создать тест.\n'
                     '- Тест по теме: введите тему, выберите сложность, количество и время.\n'
                     '- Новый тест: создайте тест вручную.\n'
                     '- Язык интерфейса: измените язык кнопок и сообщений бота.\n'
                     '- Премиум: увеличьте лимиты и получите премиум.\n'
                     '- Бонусы: откройте бонусный раздел.',
        'bonuses_menu_text': 'Раздел бонусов.\n'
                             '- Реферальный бонус: приглашайте друзей по своей ссылке.\n'
                             '- Бонус за канал: подпишитесь на канал и получите +1 файл и +1 тест по теме.',
        'topic_prompt': 'По какой теме сделать тест?\nНапишите тему.\nПример: `Столицы государств`',
        'file_topic_prompt': 'На какой теме внутри загруженного файла сфокусироваться? (необязательно)\n'
                             'Напишите тему.\n'
                             'Пример: `Столицы государств`',
        'need_translation': 'Нужен перевод?',
        'shuffle_strategy_prompt': 'Как должен работать случайный порядок?',
        'choose_translation_lang': 'На какой язык перевести?',
        'pages_prompt': 'Какой диапазон страниц использовать? (1..{total})\nПример: `20-30` или `20`',
        'manual_has_image': 'Есть картинка к вопросу? Отправьте её или напишите /skip.',
        'manual_draft_found': 'У вас есть сохранённый черновик теста. Продолжить?',
        'edit_menu': 'Тест: {title}\nВопросов: {count}\nНа вопрос: {sec}s\nID: {id}\n\nЧто редактируем?',
        'premium_choose_plan': 'Выберите один из премиум-тарифов:',
        'premium_status_free': 'Бесплатный тариф (одноразовый):\n'
                               'Лимит по файлам: {f_left}/{f_total}\n'
                               'Лимит по темам: {t_left}/{t_total}\n'
                               'Срок действия: {days} дн. (до: {until})',
        'premium_status_premium': 'Премиум-тариф активен.\n'
                                  'Срок действия: {until}\n'
                                  'Лимит по файлам: {f_left}/{f_total}\n'
                                  'Лимит по темам: {t_left}/{t_total}',
        'premium_plan_1d': '1 день',
        'premium_plan_7d': '7 дней',
        'premium_plan_30d': '1 месяц',
        'premium_plan_details': 'Выбранный тариф: {plan}\n'
                                'Срок действия: {days} дн.\n'
                                'Стоимость: {price} UZS\n'
                                'Скидка: -{disc}%\n'
                                '\n'
                                'Лимиты:\n'
                                'Файлы: {files}\n'
                                'Темы: {topics}\n'
                                '\n'
                                'Нажмите `Оплатить`, чтобы продолжить.',
        'btn_upload': 'Загрузить файл',
        'btn_topic': 'Тест по теме',
        'btn_newquiz': 'Новый тест',
        'btn_ui_lang': 'Язык интерфейса',
        'btn_premium': 'Премиум',
        'btn_join_channel': 'Подписаться на канал',
        'btn_check_sub': 'Проверить'},
 'en': {'btn_referral': 'Referral bonus',
        'btn_channel_bonus': 'Channel bonus',
        'btn_bonuses': 'Bonuses',
        'btn_admin_users': 'User statistics'},
 'de': {'btn_referral': 'Empfehlungsbonus',
        'btn_channel_bonus': 'Kanalbonus',
        'btn_bonuses': 'Boni',
        'btn_admin_users': 'Benutzerstatistik',
        'btn_back': 'Zuruck',
        'menu_help': 'Menubereiche:\n'
                     '- Datei hochladen: Laden Sie eine Datei oder ein E-Book hoch, um einen Test zu erstellen.\n'
                     '- Test nach Thema: Geben Sie ein Thema ein und wahlen Sie Schwierigkeit, Anzahl und Zeit.\n'
                     '- Neuer Test: Erstellen Sie einen Test manuell.\n'
                     '- Oberflachensprache: Andern Sie die Sprache der Bot-Oberflache.\n'
                     '- Premium: Erhohen Sie die Limits und erhalten Sie Premium.\n'
                     '- Boni: Offnen Sie den Bonusbereich.',
        'bonuses_menu_text': 'Bonusbereich.\n'
                             '- Empfehlungsbonus: Laden Sie Freunde uber Ihren Link ein.\n'
                             '- Kanalbonus: Treten Sie dem Kanal bei und erhalten Sie +1 Datei und +1 Themenquiz.',
        'topic_prompt': 'Zu welchem Thema mochten Sie einen Test erstellen?\n'
                        'Bitte geben Sie das Thema ein.\n'
                        'Beispiel: `Hauptstadte der Welt`',
        'file_topic_prompt': 'Auf welches Thema innerhalb der hochgeladenen Datei mochten Sie sich konzentrieren? '
                             '(optional)\n'
                             'Bitte geben Sie das Thema ein.\n'
                             'Beispiel: `Hauptstadte der Welt`',
        'need_translation': 'Ubersetzung notig?',
        'shuffle_strategy_prompt': 'Wie soll die Zufallsreihenfolge funktionieren?',
        'choose_translation_lang': 'In welche Sprache soll ubersetzt werden?',
        'pages_prompt': 'Welcher Seitenbereich soll verwendet werden? (1..{total})\nBeispiel: `20-30` oder `20`',
        'manual_has_image': 'Gibt es ein Bild zur Frage? Senden Sie es oder schreiben Sie /skip.',
        'manual_draft_found': 'Sie haben einen gespeicherten Testentwurf. Fortfahren?',
        'premium_status_free': 'Kostenloser Tarif (einmalig):\n'
                               'Dateilimit: {f_left}/{f_total}\n'
                               'Themenlimit: {t_left}/{t_total}\n'
                               'Gultigkeit: {days} Tag(e) (bis: {until})',
        'premium_status_premium': 'Premium-Tarif ist aktiv.\n'
                                  'Gultig bis: {until}\n'
                                  'Dateilimit: {f_left}/{f_total}\n'
                                  'Themenlimit: {t_left}/{t_total}',
        'premium_plan_1d': '1 Tag',
        'premium_plan_7d': '7 Tage',
        'premium_plan_30d': '1 Monat',
        'premium_plan_details': 'Gewahlter Tarif: {plan}\n'
                                'Laufzeit: {days} Tag(e)\n'
                                'Preis: {price} UZS\n'
                                'Rabatt: -{disc}%\n'
                                '\n'
                                'Limits:\n'
                                'Dateien: {files}\n'
                                'Themen: {topics}\n'
                                '\n'
                                'Drucken Sie `Bezahlen`, um fortzufahren.'},
 'tr': {'btn_referral': 'Referans bonusu',
        'btn_channel_bonus': 'Kanal bonusu',
        'btn_bonuses': 'Bonuslar',
        'btn_admin_users': 'Kullanici istatistikleri',
        'btn_back': 'Geri',
        'menu_help': 'Menu bolumleri:\n'
                     '- Dosya yukle: test olusturmak icin dosya veya e-kitap gonderebilirsiniz.\n'
                     '- Konuya gore test: konu yazip zorluk, adet ve sure secerek test olusturabilirsiniz.\n'
                     '- Yeni test: sorulari elle girerek test olusturabilirsiniz.\n'
                     '- Arayuz dili: botun arayuz dilini degistirebilirsiniz.\n'
                     '- Premium: limitleri artirip premium alabilirsiniz.\n'
                     '- Bonuslar: bonuslar bolumunu acabilirsiniz.',
        'bonuses_menu_text': 'Bonuslar bolumu.\n'
                             '- Referans bonusu: arkadaslarinizi baglantinizla davet edin.\n'
                             '- Kanal bonusu: kanala katilin ve +1 dosya ile +1 konu testi kazanin.',
        'topic_prompt': 'Testi hangi konuya gore olusturmak istersiniz?\n'
                        'Lutfen konuyu yazin.\n'
                        'Ornek: `Dunya baskentleri`',
        'file_topic_prompt': 'Yuklenen dosyada hangi konuya odaklanayim? (istege bagli)\n'
                             'Lutfen konuyu yazin.\n'
                             'Ornek: `Dunya baskentleri`',
        'need_translation': 'Ceviri gerekli mi?',
        'shuffle_strategy_prompt': 'Rastgele sira nasil calissin?',
        'choose_translation_lang': 'Hangi dile cevirilsin?',
        'pages_prompt': 'Hangi sayfa araligi kullanilsin? (1..{total})\nOrnek: `20-30` veya `20`',
        'manual_has_image': 'Soru icin resim var mi? Gonderin veya /skip yazin.',
        'manual_draft_found': 'Kaydedilmis bir test taslaginiz var. Devam edilsin mi?',
        'premium_status_free': 'Ucretsiz plan (tek seferlik):\n'
                               'Dosya limiti: {f_left}/{f_total}\n'
                               'Konu limiti: {t_left}/{t_total}\n'
                               'Gecerlilik suresi: {days} gun (bitis: {until})',
        'premium_status_premium': 'Premium plan aktif.\n'
                                  'Gecerlilik suresi: {until}\n'
                                  'Dosya limiti: {f_left}/{f_total}\n'
                                  'Konu limiti: {t_left}/{t_total}',
        'premium_plan_1d': '1 gun',
        'premium_plan_7d': '7 gun',
        'premium_plan_30d': '1 ay',
        'premium_plan_details': 'Secilen plan: {plan}\n'
                                'Sure: {days} gun\n'
                                'Fiyat: {price} UZS\n'
                                'Indirim: -{disc}%\n'
                                '\n'
                                'Limitler:\n'
                                'Dosya: {files}\n'
                                'Konu: {topics}\n'
                                '\n'
                                'Devam etmek icin `Odeme yap` dugmesine basin.'},
 'kk': {'btn_referral': 'Referral bonusy',
        'btn_channel_bonus': 'Kanal bonusy',
        'btn_bonuses': 'Bonustar',
        'btn_admin_users': 'Paidalanushy statistikasy',
        'btn_back': 'Artqa',
        'menu_help': 'Menu bolimderi:\n'
                     '- Fail zhykteu: test quru ushin fail nemese elektron kitaptı zhyktei alasiz.\n'
                     '- Taqyryp boiynsha test: taqyrypty jazyp, kurdelilik, san zhane uaqytty tandap test qurasiz.\n'
                     '- Zhana test: suraqtardy qoldan kirgizip test qurasiz.\n'
                     '- Interfeis tili: bot interfeisinin tilin ozgerte alasiz.\n'
                     '- Premium: limitterdi artirip, premium ala alasiz.\n'
                     '- Bonustar: bonus bolimine ote alasiz.',
        'bonuses_menu_text': 'Bonus bolimi.\n'
                             '- Referral bonusy: siltemeniz arqyly dostarynyzdy shaqyrynyz.\n'
                             '- Kanal bonusy: kanalga zhazylyp, +1 fail zhane +1 taqyryp testi alanyz.',
        'topic_prompt': 'Qai taqyryp boiynsha test quraiyn?\nTaqyrypty zhazynyz.\nMysal: `Alemdik astanalar`',
        'file_topic_prompt': 'Zhyktelgen fail ishinen qai taqyrypqa nazar audaraiyn? (mindetti emes)\n'
                             'Taqyrypty zhazynyz.\n'
                             'Mysal: `Alemdik astanalar`',
        'need_translation': 'Audarma kerek pe?',
        'shuffle_strategy_prompt': 'Kezdeisoq rettі qalai qoldanaiyq?',
        'choose_translation_lang': 'Qai tilge audaraiyn?',
        'pages_prompt': 'Qai better aralygyn paidalanaiyn? (1..{total})\nMysal: `20-30` nemese `20`',
        'manual_has_image': 'Suraqqa suret bar ma? Zhiberiniz nemese /skip zhazynyz.',
        'manual_draft_found': 'Saqlangan qoldan zhasalgan test nobaiy bar. Zhalgastıraiıq pa?',
        'premium_status_free': 'Tegin zhospar (bir rettik):\n'
                               'Fail limiti: {f_left}/{f_total}\n'
                               'Taqyryp limiti: {t_left}/{t_total}\n'
                               'Merzimi: {days} kun (ayaqtalady: {until})',
        'premium_status_premium': 'Premium zhospar belsendi.\n'
                                  'Merzimi: {until}\n'
                                  'Fail limiti: {f_left}/{f_total}\n'
                                  'Taqyryp limiti: {t_left}/{t_total}',
        'premium_plan_1d': '1 kun',
        'premium_plan_7d': '7 kun',
        'premium_plan_30d': '1 ai',
        'premium_plan_details': 'Tandalgan tarif: {plan}\n'
                                'Merzimi: {days} kun\n'
                                'Bagasy: {price} UZS\n'
                                'Zhenildik: -{disc}%\n'
                                '\n'
                                'Limitter:\n'
                                'Fail: {files}\n'
                                'Taqyryp: {topics}\n'
                                '\n'
                                'Zhalgastıru ushin `Tolem zhasau` tumesin basynyz.'},
 'ar': {'btn_referral': 'مكافأة الإحالة',
        'btn_channel_bonus': 'مكافأة القناة',
        'btn_bonuses': 'المكافآت',
        'btn_admin_users': 'إحصاءات المستخدمين',
        'btn_back': 'رجوع'},
 'zh': {'btn_referral': '邀请奖励',
        'btn_channel_bonus': '频道奖励',
        'btn_bonuses': '奖励',
        'btn_admin_users': '用户统计',
        'btn_back': '返回'},
 'ko': {'btn_referral': '추천 보너스',
        'btn_channel_bonus': '채널 보너스',
        'btn_bonuses': '보너스',
        'btn_admin_users': '사용자 통계',
        'btn_back': '뒤로'}}


def _apply_final_clean_overrides() -> None:
    for code, data in _FINAL_CLEAN_OVERRIDES.items():
        _STRINGS.setdefault(code, {}).update(data)


_apply_final_clean_overrides()




_FINAL_CLEAN_EXTRA: Dict[str, Dict[str, str]] = {'de': {'premium_choose_plan': 'Bitte wahlen Sie einen Premium-Tarif:'},
 'tr': {'premium_choose_plan': 'Lutfen bir Premium plan secin:'},
 'kk': {'premium_choose_plan': 'Premium zhosparlardyn birin tandanyz:'},
 'ar': {'menu_help': 'أقسام القائمة:\n'
                     '- رفع ملف: أرسل ملفًا أو كتابًا إلكترونيًا لإنشاء اختبار.\n'
                     '- اختبار حسب الموضوع: اكتب الموضوع واختر الصعوبة والعدد والوقت.\n'
                     '- اختبار جديد: أنشئ اختبارًا يدويًا.\n'
                     '- لغة الواجهة: غيّر لغة أزرار ورسائل البوت.\n'
                     '- بريميوم: زد الحدود واحصل على بريميوم.\n'
                     '- المكافآت: افتح قسم المكافآت.',
        'bonuses_menu_text': 'قسم المكافآت.\n'
                             '- مكافأة الإحالة: ادعُ أصدقاءك عبر رابط الإحالة الخاص بك.\n'
                             '- مكافأة القناة: اشترك في القناة واحصل على +1 ملف و +1 اختبار حسب الموضوع.',
        'premium_choose_plan': 'يرجى اختيار إحدى باقات بريميوم:',
        'premium_status_free': 'الخطة المجانية (مرة واحدة):\n'
                               'حد الملفات: {f_left}/{f_total}\n'
                               'حد المواضيع: {t_left}/{t_total}\n'
                               'المدة: {days} يوم (ينتهي في: {until})',
        'premium_status_premium': 'خطة بريميوم مفعلة.\n'
                                  'صالحة حتى: {until}\n'
                                  'حد الملفات: {f_left}/{f_total}\n'
                                  'حد المواضيع: {t_left}/{t_total}',
        'premium_plan_1d': 'يوم واحد',
        'premium_plan_7d': '7 أيام',
        'premium_plan_30d': 'شهر واحد',
        'premium_plan_details': 'الخطة المختارة: {plan}\n'
                                'المدة: {days} يوم\n'
                                'السعر: {price} UZS\n'
                                'الخصم: -{disc}%\n'
                                '\n'
                                'الحدود:\n'
                                'الملفات: {files}\n'
                                'المواضيع: {topics}\n'
                                '\n'
                                'اضغط `الدفع` للمتابعة.'},
 'zh': {'menu_help': '菜单栏目：\n'
                     '- 上传文件：发送文件或电子书来生成测试。\n'
                     '- 按主题测试：输入主题并选择难度、数量和时间。\n'
                     '- 新建测试：手动创建测试。\n'
                     '- 界面语言：更改机器人按钮和消息语言。\n'
                     '- 高级版：提高额度并开通高级版。\n'
                     '- 奖励：打开奖励栏目。',
        'bonuses_menu_text': '奖励栏目。\n- 邀请奖励：通过你的邀请链接邀请朋友。\n- 频道奖励：加入频道后可获得 +1 文件和 +1 主题测试。',
        'premium_choose_plan': '请选择一个高级版套餐：',
        'premium_status_free': '免费套餐（一次性）：\n文件额度：{f_left}/{f_total}\n主题额度：{t_left}/{t_total}\n有效期：{days} 天（到期：{until}）',
        'premium_status_premium': '高级版套餐已开通。\n有效期至：{until}\n文件额度：{f_left}/{f_total}\n主题额度：{t_left}/{t_total}',
        'premium_plan_1d': '1 天',
        'premium_plan_7d': '7 天',
        'premium_plan_30d': '1 个月',
        'premium_plan_details': '所选套餐：{plan}\n'
                                '时长：{days} 天\n'
                                '价格：{price} UZS\n'
                                '折扣：-{disc}%\n'
                                '\n'
                                '额度：\n'
                                '文件：{files}\n'
                                '主题：{topics}\n'
                                '\n'
                                '点击 `支付` 继续。'},
 'ko': {'menu_help': '메뉴 섹션:\n'
                     '- 파일 업로드: 파일이나 전자책을 보내 테스트를 만들 수 있습니다.\n'
                     '- 주제별 테스트: 주제를 입력하고 난이도, 문제 수, 시간을 선택하세요.\n'
                     '- 새 테스트: 테스트를 수동으로 만들 수 있습니다.\n'
                     '- 인터페이스 언어: 봇의 버튼과 메시지 언어를 변경할 수 있습니다.\n'
                     '- 프리미엄: 한도를 늘리고 프리미엄을 이용하세요.\n'
                     '- 보너스: 보너스 섹션을 여세요.',
        'bonuses_menu_text': '보너스 섹션입니다.\n- 추천 보너스: 추천 링크로 친구를 초대하세요.\n- 채널 보너스: 채널에 가입하면 +1 파일과 +1 주제 테스트를 받을 수 있습니다.',
        'premium_choose_plan': '프리미엄 요금제 중 하나를 선택하세요:',
        'premium_status_free': '무료 요금제(1회):\n'
                               '파일 한도: {f_left}/{f_total}\n'
                               '주제 한도: {t_left}/{t_total}\n'
                               '유효 기간: {days}일 (만료: {until})',
        'premium_status_premium': '프리미엄 요금제가 활성화되었습니다.\n'
                                  '유효 기간: {until}\n'
                                  '파일 한도: {f_left}/{f_total}\n'
                                  '주제 한도: {t_left}/{t_total}',
        'premium_plan_1d': '1일',
        'premium_plan_7d': '7일',
        'premium_plan_30d': '1개월',
        'premium_plan_details': '선택한 요금제: {plan}\n'
                                '기간: {days}일\n'
                                '가격: {price} UZS\n'
                                '할인: -{disc}%\n'
                                '\n'
                                '한도:\n'
                                '파일: {files}\n'
                                '주제: {topics}\n'
                                '\n'
                                '계속하려면 `결제하기`를 누르세요.'}}

for _code, _data in _FINAL_CLEAN_EXTRA.items():
    _STRINGS.setdefault(_code, {}).update(_data)


_FINAL_RUNTIME_FIXES: Dict[str, Dict[str, str]] = {
    "uz": {
        "btn_back": "Orqaga",
        "btn_stats": "📊 Statistika",
        "btn_prev_page": "⬅️ Oldingi",
        "btn_next_page": "Keyingi ➡️",
        "btn_open_private": "Botga o'tish",
        "private_only_group": "Bu buyruqni bot bilan shaxsiy chatda bajaring.",
        "group_stop_owner_only": "Kechirasiz, testni faqatgina testni boshlagan kishi to'xtatishi mumkin.",
    },
    "ru": {
        "btn_back": "Назад",
        "btn_stats": "📊 Статистика",
        "btn_prev_page": "⬅️ Назад",
        "btn_next_page": "Далее ➡️",
        "btn_open_private": "Открыть бота",
        "private_only_group": "Пожалуйста, используйте эту команду в личном чате с ботом.",
        "group_stop_owner_only": "Извините, остановить тест может только тот, кто его запустил.",
        "lobby_creator_only": "Только создатель теста может выполнить это действие.",
        "group_started_status": "\u0422\u0435\u0441\u0442 \u043d\u0430\u0447\u0430\u043b\u0441\u044f: {count} \u0432\u043e\u043f\u0440\u043e\u0441\u043e\u0432, \u043f\u043e {sec}s \u043a\u0430\u0436\u0434\u044b\u0439 (\u043f\u0440\u0438\u043c\u0435\u0440\u043d\u043e {est}).\n\u0423\u0447\u0430\u0441\u0442\u043d\u0438\u043a\u043e\u0432: {n}.\n\u0423\u0441\u043a\u043e\u0440\u0438\u0442\u044c: \u0414\u0430\u043b\u0435\u0435 (\u0442\u043e\u043b\u044c\u043a\u043e \u0430\u0432\u0442\u043e\u0440). \u041e\u0441\u0442\u0430\u043d\u043e\u0432\u0438\u0442\u044c: /cancel",
        "quiz_finished": "Тест завершён.",
        "quiz_stopped_no_participants": "Все участники пропустили 3 вопроса. Тест остановлен.",
        "stats_title": "Статистика: {title}",
        "stats_creator_only": "Статистику может смотреть только владелец теста.",
        "scoreboard_title": "Результаты:",
        "participants_joined": "Участники: {n}",
        "scoreboard_more": "... ещё {n} пользователей",
        "scoreboard_row": "{i}) {name}: {correct}/{answered} верно, время {t}s, среднее {avg}s{extra}",
        "stopped": "Тест остановлен.",
        "stopped_n": "Остановлено активных тестов: {n}.",
    },
    "en": {
        "btn_back": "Back",
        "btn_stats": "📊 Statistics",
        "btn_prev_page": "⬅️ Prev",
        "btn_next_page": "Next ➡️",
        "btn_open_private": "Open bot",
        "private_only_group": "Please use this command in the bot's private chat.",
        "group_stop_owner_only": "Sorry, only the person who started the quiz can stop it.",
    },
    "de": {
        "btn_back": "Zuruck",
        "btn_stats": "📊 Statistik",
        "btn_prev_page": "⬅️ Zuruck",
        "btn_next_page": "Weiter ➡️",
        "btn_open_private": "Bot privat offnen",
        "private_only_group": "Bitte nutzen Sie diesen Befehl im privaten Chat mit dem Bot.",
        "group_stop_owner_only": "Entschuldigung, nur die Person, die den Test gestartet hat, kann ihn stoppen.",
    },
    "tr": {
        "btn_back": "Geri",
        "btn_stats": "📊 Istatistik",
        "btn_prev_page": "⬅️ Onceki",
        "btn_next_page": "Sonraki ➡️",
        "btn_open_private": "Botu ac",
        "private_only_group": "Lutfen bu komutu botun ozel sohbetinde kullanin.",
        "group_stop_owner_only": "Uzgunuz, testi yalnizca testi baslatan kisi durdurabilir.",
    },
    "kk": {
        "btn_back": "Artqa",
        "btn_stats": "📊 Statistika",
        "btn_prev_page": "⬅️ Aldyngy",
        "btn_next_page": "Kelesi ➡️",
        "btn_open_private": "Botty ashu",
        "private_only_group": "Bul buiryqty botpen zheke chatta oryndanyz.",
        "group_stop_owner_only": "Keshiriniz, testti tek testti bastagan adam gana toqtata alady.",
    },
    "ar": {
        "btn_back": "رجوع",
        "btn_stats": "📊 الإحصاءات",
        "btn_prev_page": "⬅️ السابق",
        "btn_next_page": "التالي ➡️",
        "btn_open_private": "فتح البوت",
        "private_only_group": "يرجى استخدام هذا الأمر في المحادثة الخاصة مع البوت.",
        "group_stop_owner_only": "عذرًا، لا يمكن إيقاف الاختبار إلا من قبل الشخص الذي بدأه.",
    },
    "zh": {
        "btn_back": "返回",
        "btn_stats": "📊 统计",
        "btn_prev_page": "⬅️ 上一页",
        "btn_next_page": "下一页 ➡️",
        "btn_open_private": "打开机器人",
        "private_only_group": "请在与机器人的私聊中使用此命令。",
        "group_stop_owner_only": "抱歉，只有开始测试的人才能停止它。",
    },
    "ko": {
        "btn_back": "뒤로",
        "btn_stats": "📊 통계",
        "btn_prev_page": "⬅️ 이전",
        "btn_next_page": "다음 ➡️",
        "btn_open_private": "봇 열기",
        "private_only_group": "이 명령은 봇과의 개인 채팅에서 사용해 주세요.",
        "group_stop_owner_only": "죄송하지만 테스트를 시작한 사람만 중지할 수 있습니다.",
    },
}

for _code, _data in _FINAL_RUNTIME_FIXES.items():
    _STRINGS.setdefault(_code, {}).update(_data)


def norm_ui_lang(lang: str) -> str:
    x = (lang or "").strip().lower()
    if x in SUPPORTED_UI_LANGS:
        return x
    return "uz"


def lang_name(code: str) -> str:
    c = (code or "").strip().lower()
    if c == "uz":
        return "O'zbek"
    if c == "ru":
        return "\u0420\u0443\u0441\u0441\u043a\u0438\u0439"
    if c == "en":
        return "English"
    if c == "de":
        return "Deutsch"
    if c == "tr":
        return "T\u00fcrk\u00e7e"
    if c == "kk":
        return "\u049a\u0430\u0437\u0430\u049b\u0448\u0430"
    if c == "ar":
        return "\u0627\u0644\u0639\u0631\u0628\u064a\u0629"
    if c == "zh":
        return "\u4e2d\u6587"
    if c == "ko":
        return "\ud55c\uad6d\uc5b4"
    return c or "uz"


def t(lang: str, key: str, **kwargs: Any) -> str:
    lang = norm_ui_lang(lang)
    base = _STRINGS.get("en", {})
    msg = _STRINGS.get(lang, {}).get(key) or base.get(key) or key
    try:
        return msg.format(**kwargs)
    except Exception:
        return msg







