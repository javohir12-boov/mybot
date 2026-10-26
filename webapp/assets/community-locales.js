"use strict";
const communityTranslations = {
  uz: {
    myPublished: "E’lon qilgan testlarim", myPublishedHint: "Ochiq va maxfiy testlaringizni shu yerdan boshqaring.", noPublished: "Hali test e’lon qilmagansiz.", liveReturnRoom: "Ochilgan xonaga qaytish",
    visibility: "Kimlar ko‘rsin?", visibilityPublic: "Barchaga ochiq", visibilityPrivate: "Maxfiy", visibilityHint: "Ochiq test umumiy ro‘yxatda chiqadi. Maxfiy test va uning xonasi faqat xona havolasi berilgan odamlarga ochiladi.",
    removePublished: "Olib tashlash", removePublishedConfirm: "Test olib tashlansinmi? Shu testning barcha faol xonalari ham to‘xtaydi. Saqlangan asl testingiz profilingizda qoladi.", publishedRemoved: "Test olib tashlandi.", publishedRemovedReason: "Muallif testni olib tashlagani uchun xona to‘xtadi.",
    supportPanel: "Admin panel · Xabarlar", supportPanelHint: "Foydalanuvchilarning yordam xabarlari va javoblaringiz.", supportChatIntro: "Adminga yozing. Javoblar shu yozishmada ko‘rinadi.", supportAdmin: "Administrator", supportYou: "Siz", supportHistory: "Yordam yozishmasi", supportNoMessages: "Savolingizni pastdagi maydonga yozing.", supportInboxEmpty: "Hozircha yordam xabarlari yo‘q.", supportOlder: "Oldingi xabarlar", supportAdminReplyHint: "Javob foydalanuvchining Profil → Yordam bo‘limiga keladi.", supportReply: "Javob yuborish", supportSend: "Xabar yuborish", supportSent: "Xabaringiz yuborildi.",
    errors: { admin_only: "Bu bo‘lim faqat administratorlar uchun." }
  },
  en: {
    myPublished: "My published quizzes", myPublishedHint: "Manage your public and private quizzes here.", noPublished: "You haven’t published any quizzes yet.", liveReturnRoom: "Return to your open room",
    visibility: "Who can see it?", visibilityPublic: "Public", visibilityPrivate: "Private", visibilityHint: "Public quizzes appear in the catalogue. A private quiz and its room are accessible through a shared room link.",
    removePublished: "Remove", removePublishedConfirm: "Remove this quiz? All its active rooms will stop. Your original saved quiz will remain in your profile.", publishedRemoved: "Quiz removed.", publishedRemovedReason: "The creator removed this quiz, so the room has stopped.",
    supportPanel: "Admin panel · Messages", supportPanelHint: "User support conversations and your replies.", supportChatIntro: "Message the administrator. Replies will appear in this conversation.", supportAdmin: "Administrator", supportYou: "You", supportHistory: "Support conversation", supportNoMessages: "Write your question in the field below.", supportInboxEmpty: "No support messages yet.", supportOlder: "Earlier messages", supportAdminReplyHint: "Your reply appears in the user’s Profile → Help conversation.", supportReply: "Send reply", supportSend: "Send message", supportSent: "Your message was sent.",
    errors: { admin_only: "This section is for administrators only." }
  },
  ru: {
    myPublished: "Мои опубликованные тесты", myPublishedHint: "Управляйте открытыми и закрытыми тестами здесь.", noPublished: "Вы ещё не опубликовали тесты.", liveReturnRoom: "Вернуться в открытую комнату",
    visibility: "Кто может видеть?", visibilityPublic: "Для всех", visibilityPrivate: "Закрытый", visibilityHint: "Открытый тест появится в каталоге. Закрытый тест и его комната доступны по ссылке на комнату.",
    removePublished: "Убрать", removePublishedConfirm: "Убрать тест? Все его активные комнаты остановятся. Исходный сохранённый тест останется в профиле.", publishedRemoved: "Тест убран.", publishedRemovedReason: "Автор убрал тест, поэтому комната остановлена.",
    supportPanel: "Админ-панель · Сообщения", supportPanelHint: "Обращения пользователей и ваши ответы.", supportChatIntro: "Напишите администратору. Ответ придёт в эту переписку.", supportAdmin: "Администратор", supportYou: "Вы", supportHistory: "Переписка с поддержкой", supportNoMessages: "Напишите вопрос в поле ниже.", supportInboxEmpty: "Обращений пока нет.", supportOlder: "Ранние сообщения", supportAdminReplyHint: "Ответ появится у пользователя в разделе Профиль → Помощь.", supportReply: "Отправить ответ", supportSend: "Отправить сообщение", supportSent: "Сообщение отправлено.",
    errors: { admin_only: "Этот раздел доступен только администраторам." }
  }
};
for (const [lang, updates] of Object.entries(communityTranslations)) {
  const { errors, ...labels } = updates;
  Object.assign(window.QUIZ_LOCALES[lang], labels);
  Object.assign(window.QUIZ_LOCALES[lang].errors, errors);
}
