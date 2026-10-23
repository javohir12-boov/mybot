"use strict";
(() => {
  const tg = window.Telegram?.WebApp;
  const main = document.querySelector("#main");
  const auth = tg?.initData || "";
  const icons = {
    file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z"/><path d="M14 2v6h6M8 13h8M8 17h5"/>',
    spark: '<path d="m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5Z"/><path d="m20 2 .6 1.4L22 4l-1.4.6L20 6l-.6-1.4L18 4l1.4-.6Z"/>',
    edit: '<path d="m16 3 5 5-12 12-6 1 1-6Z"/><path d="m14 5 5 5M3 7V4a1 1 0 0 1 1-1h4M21 15v5a1 1 0 0 1-1 1h-5"/>',
    user: '<circle cx="12" cy="8" r="4"/><path d="M4 21v-2a8 8 0 0 1 16 0v2"/>',
    team: '<circle cx="9" cy="8" r="3"/><path d="M3 21v-2a6 6 0 0 1 12 0v2M16 5a3 3 0 0 1 0 6m2 4a5 5 0 0 1 3 5v1"/>',
    heart: '<path d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.7l-1.1-1.1a5.5 5.5 0 0 0-7.8 7.8L12 21l8.8-8.6a5.5 5.5 0 0 0 0-7.8Z"/>',
    book: '<path d="M12 5C9 3 5 3 2 4v15c3-1 7-1 10 1m0-15c3-2 7-2 10-1v15c-3-1-7-1-10 1Zm0 0v15"/>',
    medal: '<circle cx="12" cy="9" r="6"/><path d="m8 14-2 8 6-3 6 3-2-8m-4-8 .9 1.8 2 .3-1.4 1.4.3 2-1.8-1-1.8 1 .3-2-1.4-1.4 2-.3Z"/>',
    upload: '<path d="M12 16V3m-5 5 5-5 5 5M4 15v5a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-5"/>',
    sliders: '<path d="M4 7h8m4 0h4M4 17h3m4 0h9"/><circle cx="14" cy="7" r="2"/><circle cx="9" cy="17" r="2"/>',
    arrow: '<path d="M5 12h14m-5-5 5 5-5 5"/>',
    chevron: '<path d="m9 5 7 7-7 7"/>',
    back: '<path d="M19 12H5m5-5-5 5 5 5"/>',
    check: '<path d="m5 12 4 4L19 6"/>',
    shield: '<path d="m12 2 8 4v6c0 5-8 10-8 10S4 17 4 12V6Z"/><path d="m8 12 3 3 5-6"/>',
    bulb: '<path d="M9 18h6m-5 3h4M8 14a6 6 0 1 1 8 0l-1 2H9Z"/>',
    crown: '<path d="m3 6 4 5 5-7 5 7 4-5-2 13H5ZM5 22h14"/>',
    gift: '<path d="M3 8h18v4H3zM5 12v9h14v-9M12 8v13"/><path d="M12 8S4 8 5 4s7 0 7 4Zm0 0s8 0 7-4-7 0-7 4Z"/>',
    globe: '<circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="4" ry="9"/><path d="M3 12h18"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    trash: '<path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7"/>',
    copy: '<rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V3H3v13h5"/>',
    share: '<path d="m21 3-7 18-4-8-8-3ZM10 13 21 3"/>'
  };
  const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.file}</svg>`;
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const storage = {
    get(key) { try { return JSON.parse(localStorage.getItem(key)); } catch { return null; } },
    set(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* Storage may be unavailable in a webview. */ } },
    remove(key) { try { localStorage.removeItem(key); } catch { /* Optional persistence. */ } }
  };
  const blankQuestion = () => ({ question: "", options: ["", "", "", ""], correct_index: 0, explanation: "" });
  const defaultOptions = () => ({ question_count: 10, open_period: 30, difficulty: "mixed", output_language: "source", shuffle_mode: "none", import_only: false });
  const state = { lang: "uz", tab: "file", route: "file", profile: null, file: null, fileOptions: defaultOptions(), topicOptions: defaultOptions(), topic: "", manual: { title: "", questions: [blankQuestion()], ...defaultOptions() }, job: null, readyId: null, error: null, quiz: null, quizzes: [], next: null, busy: false, routeVersion: 0 };
  let userKey = "guest";
  let profileDraft = null;
  let supportDraft = "";
  let supportBusy = false;
  let supportSent = false;
  let avatarUrl = "";
  let pendingAvatar = null;
  let avatarPreview = "";
  let removeAvatar = false;
  const avatarCropper = new window.AvatarCropper();
  let avatarLoading = false;
  let avatarLoadTicket = 0;
  let pollTimer;
  let toastTimer;
  let imageUrls = [];
  const tr = key => window.QUIZ_LOCALES[state.lang]?.[key] ?? window.QUIZ_LOCALES.en[key] ?? key;
  const errorText = code => window.QUIZ_LOCALES[state.lang]?.errors[code] ?? window.QUIZ_LOCALES.en.errors[code] ?? tr("errors").server_error;
  const langs = { source: () => tr("sourceLang"), uz: () => "O‘zbekcha", ru: () => "Русский", en: () => "English", de: () => "Deutsch", tr: () => "Türkçe", kk: () => "Қазақша", ar: () => "العربية", zh: () => "中文", ko: () => "한국어" };
  const selected = (a, b) => String(a) === String(b) ? " selected" : "";

  function toast(message) {
    const node = document.querySelector("#toast");
    clearTimeout(toastTimer);
    node.textContent = message;
    node.hidden = false;
    toastTimer = setTimeout(() => { node.hidden = true; }, 4500);
  }

  async function api(path, { method = "GET", body } = {}) {
    if (!auth) throw Object.assign(new Error(tr("openTelegram")), { code: "unauthorized" });
    const headers = { Authorization: `tma ${auth}` };
    if (body && !(body instanceof FormData)) { headers["Content-Type"] = "application/json"; body = JSON.stringify(body); }
    let response;
    try { response = await fetch(`/api/${path}`, { method, headers, body }); }
    catch { throw Object.assign(new Error(errorText("network")), { code: "network" }); }
    let data;
    try { data = await response.json(); } catch { throw Object.assign(new Error(errorText("server_error")), { code: "server_error" }); }
    if (!response.ok) throw Object.assign(new Error(errorText(data.error)), { code: data.error });
    return data;
  }

  function openTelegram(url) {
    if (!/^https:\/\/t\.me\/(?:share\/url|[A-Za-z0-9_]+)(?:\?|$)/.test(url)) return;
    if (tg && auth) tg.openTelegramLink(url);
    else window.open(url, "_blank", "noopener,noreferrer");
  }
  function botLink(payload) {
    const name = state.profile?.bot_username;
    return name ? `https://t.me/${encodeURIComponent(name)}?start=${encodeURIComponent(payload)}` : "";
  }

  function theme() {
    const dark = tg?.colorScheme === "dark";
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    if (auth) {
      tg.setHeaderColor(dark ? "#151720" : "#f6f7fb");
      tg.setBackgroundColor(dark ? "#151720" : "#f6f7fb");
      if (tg.isVersionAtLeast("7.10")) tg.setBottomBarColor(dark ? "#20232e" : "#ffffff");
    }
  }

  function heading(type) {
    return `<div class="eyebrow">${icon("spark")}${tr("eyebrow")}</div><h1>${tr(`${type}Title`)}<br><span class="accent">${tr(`${type}Accent`)}</span></h1><p class="intro">${tr(`${type}Intro`)}</p>`;
  }
  const cardHead = (name, title, extra = "") => `<div class="card-head"><span class="small-icon">${icon(name)}</span><h2>${title}</h2>${extra}</div>`;
  const primary = (label, name = "arrow") => { const busy = state.busy || (state.route === "profile-edit" && avatarLoading); return `<button type="submit" class="primary" ${busy ? "disabled" : ""}>${icon(name)}${busy ? tr("loading") : label}</button>`; };
  const tip = message => `<aside class="tip">${icon("bulb")}<div><strong>${tr("tipTitle")}</strong><p>${message}</p></div></aside>`;
  const back = () => `<button class="back" data-go="profile">${icon("back")}${tr("back")}</button>`;
  const profileName = () => state.profile?.profile?.display_name || state.profile?.user?.first_name || tr("guest");
  const avatarContent = (url = avatarUrl) => url ? `<img src="${esc(url)}" alt="">` : esc(profileName()[0].toUpperCase());

  function errorHtml() {
    if (!state.error) return "";
    const channel = state.profile?.required_channel;
    const channelLink = channel?.startsWith("@") && /^@[A-Za-z0-9_]+$/.test(channel) ? `https://t.me/${channel.slice(1)}` : "";
    return `<div class="error" role="alert">${esc(errorText(state.error))}${state.error === "subscription_required" && channelLink ? `<a href="${esc(channelLink)}" target="_blank" rel="noopener">${tr("joinChannel")}</a>` : ""}</div>`;
  }

  function optionsHtml(opts, manual = false) {
    return `<section class="card">${cardHead("sliders", tr("settings"))}
      <div class="grid-two">${!manual ? `<label class="field"><span>${tr("count")}</span><select name="question_count">${[5, 10, 20, 30, 50].map(n => `<option value="${n}"${selected(opts.question_count, n)}>${n}</option>`).join("")}</select></label>` : ""}
      <label class="field"><span>${tr("time")}</span><select name="open_period">${[15, 30, 45, 60, 120].map(n => `<option value="${n}"${selected(opts.open_period, n)}>${n} ${tr("seconds")}</option>`).join("")}</select></label>
      ${manual ? "" : `<label class="field"><span>${tr("language")}</span><select name="output_language">${Object.entries(langs).map(([key, label]) => `<option value="${key}"${selected(opts.output_language, key)}>${label()}</option>`).join("")}</select></label>`}</div>
      ${!manual ? `<span class="field-label">${tr("difficulty")}</span><div class="chips">${["easy", "medium", "hard", "mixed"].map(d => `<button type="button" class="chip ${opts.difficulty === d ? "selected" : ""}" data-difficulty="${d}" aria-pressed="${opts.difficulty === d}">${tr(d)}</button>`).join("")}</div><input type="hidden" name="difficulty" value="${opts.difficulty}">` : ""}
      <details class="advanced"><summary>${tr("advanced")}</summary><label class="check-row"><input type="checkbox" name="shuffle"${opts.shuffle_mode === "both" ? " checked" : ""}>${tr("shuffle")}</label></details></section>`;
  }

  function filePage() {
    return `${heading("file")}<div class="steps"><span class="current"><b>1</b>${tr("stepFile")}</span><i></i><span><b>2</b>${tr("stepSettings")}</span><i></i><span><b>3</b>${tr("stepReady")}</span></div>
      <form id="file-form">${errorHtml()}<section class="card">${cardHead("file", tr("source"), `<span class="tag">${tr("required")}</span>`)}
      <input id="file-input" type="file" accept=".pdf,.docx,.pptx,.xlsx,.txt,.md,.json,.png,.jpg,.jpeg,.webp" hidden>
      <button class="upload" id="upload-zone" type="button" style="width:100%"><span class="upload-symbol">${icon(state.file ? "check" : "upload")}</span><strong id="upload-name">${esc(state.file?.name || tr("upload"))}</strong><p id="upload-size">${state.file ? `${(state.file.size / 1024 / 1024).toFixed(2)} MB` : tr("uploadHint")}</p><span class="choose">${tr("choose")}</span></button>
      <p class="formats">PDF · DOCX · PPTX · XLSX · TXT · JSON · JPG · PNG</p><p class="formats">${tr("limits")}</p>
      <label class="check-row" style="margin-top:15px"><input type="checkbox" name="import_only" ${state.fileOptions.import_only ? "checked" : ""}>${tr("importOnly")}</label></section>
      ${optionsHtml(state.fileOptions)}${primary(tr("generate"), "spark")}<p class="under-button">${icon("shield")}${tr("secure")}</p></form>${tip(tr("fileTip"))}`;
  }

  function topicPage() {
    return `${heading("topic")}<form id="topic-form">${errorHtml()}<section class="card">${cardHead("spark", tr("topicLabel"))}<label class="field"><span>${tr("topicLabel")}</span><textarea name="topic" required minlength="3" maxlength="200" placeholder="${tr("topicPlaceholder")}">${esc(state.topic)}</textarea></label></section>${optionsHtml(state.topicOptions)}${primary(tr("generate"), "spark")}<p class="under-button">${icon("shield")}${tr("secure")}</p></form>${tip(tr("topicTip"))}`;
  }

  function manualPage() {
    const draft = state.manual;
    return `${heading("manual")}<form id="manual-form">${errorHtml()}<section class="card"><label class="field"><span>${tr("testTitle")}</span><input name="title" type="text" required maxlength="200" value="${esc(draft.title)}" placeholder="${tr("titlePlaceholder")}"></label></section>
      ${draft.questions.map((q, n) => `<section class="card manual-question" data-question="${n}"><div class="question-head"><span>${tr("question")} ${n + 1}</span>${draft.questions.length > 1 ? `<button class="icon-button" type="button" data-remove="${n}" aria-label="${tr("remove")}">${icon("trash")}</button>` : ""}</div>
      <label class="field"><span>${tr("question")}</span><input name="question" type="text" value="${esc(q.question)}" maxlength="300" required placeholder="${tr("questionPlaceholder")}"></label>
      ${q.options.map((answer, i) => `<div class="answer-row"><input type="radio" name="correct-${n}" value="${i}" ${q.correct_index === i ? "checked" : ""} aria-label="${tr("correctAnswer")} ${"ABCD"[i]}"><span class="answer-letter">${"ABCD"[i]}</span><input type="text" name="answer" value="${esc(answer)}" maxlength="100" required aria-label="${tr("answer")} ${"ABCD"[i]}" placeholder="${tr("answer")} ${"ABCD"[i]}"></div>`).join("")}
      <label class="field" style="margin-top:14px"><span>${tr("explanation")}</span><input type="text" name="explanation" maxlength="200" value="${esc(q.explanation)}"></label></section>`).join("")}
      <button type="button" class="secondary" data-add ${draft.questions.length >= 100 ? "disabled" : ""}>${icon("plus")}${tr("addQuestion")}</button><p class="under-button">${tr("draft")}</p>
      ${optionsHtml(draft, true)}${primary(tr("save"), "check")}</form>`;
  }

  function profilePage() {
    const p = state.profile, q = p?.quota, user = p?.user;
    const name = profileName();
    const files = q ? (q.premium_active ? q.files_left : q.trial_files_left) : "—";
    const topics = q ? (q.premium_active ? q.topics_left : q.trial_topics_left) : "—";
    const until = q?.premium_active ? q.premium_until : q?.trial_until;
    const row = (name, label, route) => `<button class="row-button" data-go="${route}"><span class="small-icon">${icon(name)}</span><span>${label}</span>${icon("chevron")}</button>`;
    return `<h1>${tr("profileTitle")}</h1><p class="intro">${tr("profileIntro")}</p>${errorHtml()}<section class="card"><div class="profile-hero"><span class="avatar">${avatarContent()}</span><div><h2>${esc(name)}</h2><span class="muted">${q?.premium_active ? tr("premium") : tr("free")}${p?.profile?.username ? ` · @${esc(p.profile.username)}` : ""}</span></div></div><button class="secondary" data-go="profile-edit">${icon("edit")}${tr("editProfile")}</button>
      <div class="quota"><div><strong>${files}</strong><small>${tr("filesLeft")}</small></div><div><strong>${topics}</strong><small>${tr("topicsLeft")}</small></div></div>${until ? `<p class="muted" style="margin:12px 0 0">${tr("expires")}: ${esc(new Date(until).toLocaleDateString(state.lang))}</p>` : ""}</section>
      <section class="card" style="padding-block:4px">${row("bulb", tr("support"), "support")}${row("crown", tr("premium"), "premium")}${row("gift", tr("bonuses"), "bonuses")}${row("file", tr("myTests"), "quizzes")}</section>
      <section class="card">${cardHead("globe", tr("uiLanguage"))}<label class="field"><span>${tr("uiLanguage")}</span><select id="ui-language">${["uz", "ru", "en"].map(l => `<option value="${l}"${selected(state.lang, l)}>${langs[l]()}</option>`).join("")}</select></label></section>
      ${auth && !p ? `<button class="secondary" data-refresh>${icon("arrow")}${tr("retry")}</button>` : ""}`;
  }

  function avatarCropHtml() {
    const disabled = state.busy || avatarLoading ? "disabled" : "";
    return `<div class="avatar-cropper"><h2>${tr("avatarCropTitle")}</h2><p class="muted">${tr("avatarCropHint")}</p>
      <div class="avatar-crop-stage" tabindex="0" role="group" aria-label="${esc(tr("avatarCropHint"))}"><canvas width="768" height="768"></canvas></div>
      <label class="avatar-zoom"><span>${tr("avatarZoom")}</span><input type="range" min="0.1" max="4" step="any" value="${avatarCropper.zoom}" aria-label="${esc(tr("avatarZoom"))}" ${disabled}><output></output></label>
      <div class="avatar-crop-actions"><button type="button" class="secondary" data-avatar-fit ${disabled}>${tr("avatarFit")}</button><button type="button" class="secondary" data-avatar-reset ${disabled}>${tr("avatarReset")}</button></div>
      <p class="muted">${tr("avatarCropSaveHint")}</p>${primary(tr("saveAvatar"), "check")}</div>`;
  }

  function profileEditPage() {
    if (!profileDraft) profileDraft = { display_name: profileName(), username: state.profile?.profile?.username || "" };
    return `${back()}<h1>${tr("editProfile")}</h1><p class="intro">${tr("profileNote")}</p><form id="profile-form">${errorHtml()}
      <section class="card avatar-editor">${avatarCropper.active ? avatarCropHtml() : `<span class="avatar">${avatarContent(removeAvatar ? "" : (avatarPreview || avatarUrl))}</span>`}
      <input type="file" id="avatar-input" accept="image/png,image/jpeg,image/webp" hidden>
      <button type="button" class="secondary" data-avatar-choose ${state.busy ? "disabled" : ""}>${icon("upload")}${tr("chooseAvatar")}</button>
      ${pendingAvatar || (state.profile?.profile?.has_avatar && !removeAvatar) ? `<button type="button" class="back" data-avatar-remove ${state.busy ? "disabled" : ""}>${icon("trash")}${tr("removeAvatar")}</button>` : ""}<p class="muted">${tr("avatarHint")}</p></section>
      <section class="card"><label class="field"><span>${tr("displayName")}</span><input name="display_name" type="text" autocomplete="name" required maxlength="80" value="${esc(profileDraft.display_name)}"></label>
      <label class="field"><span>${tr("username")}</span><input name="profile_username" type="text" autocomplete="username" autocapitalize="none" spellcheck="false" maxlength="33" placeholder="username" value="${esc(profileDraft.username)}"><small>${tr("usernameHint")}</small></label></section>
      ${primary(tr("saveProfile"), "check")}<button type="button" class="secondary" data-go="profile">${tr("cancel")}</button></form>`;
  }

  function premiumPage() {
    const plans = state.profile?.plans;
    return `${back()}<h1>${tr("premium")}</h1><p class="intro">${tr("premiumIntro")}</p><section class="card premium-card">${cardHead("crown", tr("premium"))}
      ${plans ? `<div class="plans">${Object.values(plans).map(p => `<div class="plan"><b>${p.days} ${tr("days")}</b><strong>${Number(p.price).toLocaleString(state.lang)}</strong><span>UZS</span><small>${p.files} ${tr("files")}<br>${p.topics} ${tr("topics")}</small></div>`).join("")}</div>` : `<p class="muted">${tr("openTelegram")}</p>`}
      <button class="primary" style="margin-top:20px" data-premium>${icon("crown")}${tr("payment")}</button></section><p class="muted">${tr("paymentHint")}</p>`;
  }

  function bonusPage() {
    const r = state.profile?.referral;
    return `${back()}<h1>${tr("bonuses")}</h1><p class="intro">${tr("bonusIntro")}</p><section class="card">${cardHead("gift", tr("bonuses"))}
      <div class="quota"><div><strong>${r?.total ?? "—"}</strong><small>${tr("invited")}</small></div><div><strong>${r?.qualified ?? "—"}</strong><small>${tr("qualified")}</small></div></div><p class="muted" style="margin-top:13px">${tr("toNext")}: ${r?.to_next_reward ?? "—"}</p>
      <div class="ref-link">${esc(botLink(`ref_${state.profile?.user.id}`) || tr("openTelegram"))}</div><button class="primary" data-copy>${icon("copy")}${tr("copy")}</button><button class="secondary" data-share>${icon("share")}${tr("share")}</button></section>`;
  }

  function supportPage() {
    return `${back()}<h1>${tr("support")}</h1><p class="intro">${tr("supportIntro")}</p><form id="support-form">${errorHtml()}
      ${supportSent ? `<p class="result-count" role="status">${tr("supportSent")}</p>` : ""}
      <section class="card"><label class="field"><span>${tr("supportMessage")}</span><textarea name="support_message" maxlength="3000" rows="6" required placeholder="${esc(tr("supportPlaceholder"))}" ${supportBusy ? "disabled" : ""}>${esc(supportDraft)}</textarea><small>${tr("supportHint")}</small></label>
      <button type="submit" class="primary" ${supportBusy || !auth ? "disabled" : ""}>${icon("share")}${tr(supportBusy ? "loading" : "supportSend")}</button></section></form>`;
  }

  async function sendSupport() {
    if (supportBusy) return;
    if (!supportDraft.trim()) return toast(errorText("invalid_support_message"));
    const version = state.routeVersion;
    supportBusy = true; supportSent = false; state.error = null;
    render();
    try {
      await api("support", { method: "POST", body: { message: supportDraft } });
      supportDraft = ""; supportSent = true;
      toast(tr("supportSent"));
    } catch (error) {
      if (state.routeVersion === version) state.error = error.code || "server_error";
      toast(error.message);
    } finally {
      supportBusy = false;
      if (state.route === "support") render();
    }
  }

  function quizzesPage() {
    return `${back()}<h1>${tr("myTests")}</h1><p class="intro">${tr("secure")}</p>${errorHtml()}<section class="card">${state.quizzes.length ? state.quizzes.map(q => `<button class="quiz-row" data-quiz="${Number(q.id)}"><span class="small-icon">${icon(q.is_ai_generated ? "spark" : "file")}</span><span><strong>${esc(q.title)}</strong><small>${q.question_count} ${tr("questions")} · ${q.open_period} ${tr("seconds")}</small></span>${icon("chevron")}</button>`).join("") : `<div class="empty">${tr(auth ? "noTests" : "openTelegram")}</div>`}</section>${state.next ? `<button class="secondary" data-more>${tr("more")}</button>` : ""}`;
  }

  function resultPage() {
    const quiz = state.quiz;
    if (!quiz) return `${back()}${errorHtml()}`;
    return `${back()}<div class="success-symbol">${icon("check")}</div><div class="eyebrow">${tr("ready")}</div><h1>${esc(quiz.title)}</h1><span class="result-count">${quiz.questions.length} ${tr("questions")} · ${quiz.open_period} ${tr("seconds")}</span>
      <button class="primary" data-start="${quiz.id}">${icon("share")}${tr("openBot")}</button><button class="secondary" data-publish-existing="${quiz.id}">${icon("team")}${tr("livePublish")}</button><button class="secondary" data-go="${state.tab === "profile" ? "file" : state.tab}">${icon("plus")}${tr("newTest")}</button><p class="under-button">${icon("shield")}${tr("secure")}</p>
      ${quiz.questions.map((q, n) => `<section class="question-preview"><h3>${n + 1}. ${esc(q.question)}</h3>${q.has_image ? `<img data-question-image="${Number(q.question_id)}" alt="${tr("question")} ${n + 1}" style="display:block;max-width:100%;max-height:320px;object-fit:contain;margin:12px auto;border-radius:8px" hidden>` : ""}${q.options.map((o, i) => `<div class="answer-preview ${i === q.correct_index ? "correct" : ""}">${"ABCD"[i]}. ${esc(o)}${i === q.correct_index ? " ✓" : ""}</div>`).join("")}${q.explanation ? `<p class="explanation">${esc(q.explanation)}</p>` : ""}</section>`).join("")}`;
  }

  function workingPage() {
    return `<section class="card loading-card" role="status"><div class="spinner"></div><h2>${tr("working")}</h2><p>${tr("workingHint")}</p></section>`;
  }

  function render() {
    imageUrls.forEach(url => URL.revokeObjectURL(url));
    imageUrls = [];
    document.documentElement.lang = state.lang;
    document.querySelector("#profile-shortcut").ariaLabel = tr("profile");
    document.querySelector("#profile-shortcut").innerHTML = avatarContent();
    document.querySelector("#navigation").ariaLabel = tr("settings");
    document.querySelector("#navigation").innerHTML = [["file", "file"], ["topic", "spark"], ["manual", "edit"], ["interactive", "team"], ["profile", "user"]].map(([name, symbol]) => `<button class="nav-item ${state.tab === name ? "active" : ""}" data-go="${name}" ${state.tab === name ? 'aria-current="page"' : ""}>${icon(symbol)}<span>${tr(name)}</span></button>`).join("");
    const banner = document.querySelector("#connection");
    banner.hidden = !!auth && !state.readyId && !state.job;
    if (!auth) banner.textContent = tr("preview");
    else if (state.job) banner.innerHTML = `<button class="back" style="margin:0" data-go="working">${tr("working")} ${icon("chevron")}</button>`;
    else if (state.readyId) banner.innerHTML = `<button class="back" style="margin:0" data-quiz="${state.readyId}">${tr("ready")} ${icon("chevron")}</button>`;
    if (["file", "topic", "manual"].includes(state.route) && state.job && state.jobTab === state.route) main.innerHTML = workingPage();
    else main.innerHTML = ({ file: filePage, topic: topicPage, manual: manualPage, profile: profilePage, "profile-edit": profileEditPage, premium: premiumPage, bonuses: bonusPage, support: supportPage, quizzes: quizzesPage, result: resultPage, working: workingPage, interactive: interactive.hubPage, "interactive-create": interactive.createPage, "interactive-room": interactive.roomPage })[state.route]?.() || filePage();
    if (auth && tg?.BackButton) {
      if (["profile-edit", "premium", "bonuses", "support", "quizzes", "result", "working", "interactive-create", "interactive-room"].includes(state.route)) tg.BackButton.show(); else tg.BackButton.hide();
    }
    bindFileDrop();
    if (state.route === "profile-edit" && avatarCropper.active) avatarCropper.bind(main.querySelector(".avatar-cropper"), state.busy || avatarLoading);
    if (state.route === "result" && state.quiz) loadImages(state.quiz.id);
    interactive.afterRender();
  }

  async function loadImages(quizId) {
    for (const img of main.querySelectorAll("[data-question-image]")) {
      try {
        const response = await fetch(`/api/quizzes/${quizId}/images/${img.dataset.questionImage}`, { headers: { Authorization: `tma ${auth}` } });
        if (!response.ok) continue;
        const blob = await response.blob();
        if (!img.isConnected) continue;
        const url = URL.createObjectURL(blob);
        imageUrls.push(url); img.src = url; img.hidden = false;
      } catch { /* A missing image must not hide the saved quiz. */ }
    }
  }

  function capture() {
    const form = main.querySelector("form");
    if (!form) return;
    if (interactive.capture(form)) return;
    const data = new FormData(form);
    if (form.id === "support-form") {
      if (!supportBusy) supportDraft = String(data.get("support_message") || "");
      return;
    }
    if (form.id === "profile-form") {
      profileDraft = { display_name: String(data.get("display_name") || ""), username: String(data.get("profile_username") || "") };
      return;
    }
    const opts = { question_count: Number(data.get("question_count") || 10), open_period: Number(data.get("open_period") || 30), difficulty: data.get("difficulty") || "mixed", output_language: data.get("output_language") || "source", shuffle_mode: data.has("shuffle") ? "both" : "none", import_only: data.has("import_only") };
    if (form.id === "file-form") state.fileOptions = opts;
    if (form.id === "topic-form") { state.topicOptions = opts; state.topic = String(data.get("topic") || ""); }
    if (form.id === "manual-form") {
      const questions = [...form.querySelectorAll(".manual-question")].map(section => ({ question: section.querySelector('[name="question"]').value, options: [...section.querySelectorAll('[name="answer"]')].map(i => i.value), correct_index: Number(section.querySelector('input[type="radio"]:checked')?.value ?? 0), explanation: section.querySelector('[name="explanation"]').value }));
      state.manual = { ...opts, title: String(data.get("title") || ""), questions };
      storage.set(`quiz-draft:${userKey}`, state.manual);
    }
  }

  async function navigate(route) {
    capture();
    interactive.onNavigate(state.route, route);
    if (state.route === "profile-edit" && route !== "profile-edit") clearProfileDraft();
    state.error = null;
    state.route = route;
    if (["file", "topic", "manual", "profile"].includes(route)) state.tab = route;
    else if (["profile-edit", "premium", "bonuses", "support", "quizzes"].includes(route)) state.tab = "profile";
    else if (interactive.routes.includes(route)) state.tab = "interactive";
    const version = ++state.routeVersion;
    render();
    window.scrollTo({ top: 0, behavior: "instant" });
    if (interactive.routes.includes(route)) { await interactive.load(route); return; }
    if (route === "quizzes" && auth) {
      main.innerHTML = `<div class="card loading-card"><div class="spinner"></div><p>${tr("loading")}</p></div>`;
      try {
        const result = await api("quizzes");
        state.quizzes = result.quizzes; state.next = result.next;
      } catch (e) { state.error = e.code; }
      if (state.routeVersion === version) render();
    }
  }

  async function showQuiz(id, captureDraft = true) {
    if (captureDraft) capture();
    interactive.onNavigate(state.route, "result");
    const version = ++state.routeVersion;
    state.route = "result"; state.error = null; state.quiz = null;
    main.innerHTML = `<div class="card loading-card"><div class="spinner"></div><p>${tr("loading")}</p></div>`;
    try { state.quiz = await api(`quizzes/${Number(id)}`); state.readyId = null; }
    catch (e) { state.error = e.code; }
    if (state.routeVersion === version) { render(); window.scrollTo({ top: 0, behavior: "instant" }); }
  }

  function pickFile(file) {
    if (!file) return;
    const suffix = file.name.toLowerCase().split(".").pop();
    if (!["pdf", "docx", "pptx", "xlsx", "txt", "md", "json", "png", "jpg", "jpeg", "webp"].includes(suffix)) return toast(errorText("unsupported_file"));
    if (file.size > (suffix === "pptx" ? 25 : 5) * 1024 * 1024) return toast(errorText("too_large"));
    if (!file.size) return toast(errorText("invalid_file"));
    state.file = file;
    document.querySelector("#upload-name").textContent = file.name;
    document.querySelector("#upload-size").textContent = `${(file.size / 1024 / 1024).toFixed(2)} MB`;
  }

  function bindFileDrop() {
    const zone = document.querySelector("#upload-zone");
    if (!zone) return;
    zone.addEventListener("click", () => document.querySelector("#file-input").click());
    for (const event of ["dragenter", "dragover"]) zone.addEventListener(event, e => { e.preventDefault(); zone.classList.add("drag"); });
    for (const event of ["dragleave", "drop"]) zone.addEventListener(event, e => { e.preventDefault(); zone.classList.remove("drag"); });
    zone.addEventListener("drop", e => pickFile(e.dataTransfer.files[0]));
    document.querySelector("#file-input").addEventListener("change", e => pickFile(e.target.files[0]));
  }

  async function refreshProfile() {
    if (!auth) return;
    state.profile = await api("profile");
    await refreshAvatar();
    document.querySelector("#profile-shortcut").innerHTML = avatarContent();
  }

  async function refreshAvatar() {
    if (avatarUrl) URL.revokeObjectURL(avatarUrl);
    avatarUrl = "";
    if (!state.profile?.profile?.has_avatar) return;
    try {
      const response = await fetch("/api/profile/avatar", { headers: { Authorization: `tma ${auth}` } });
      if (response.ok) avatarUrl = URL.createObjectURL(await response.blob());
    } catch { /* The initials remain visible when the photo cannot load. */ }
  }

  function clearProfileDraft() {
    ++avatarLoadTicket; avatarLoading = false;
    avatarCropper.clear();
    if (avatarPreview) URL.revokeObjectURL(avatarPreview);
    avatarPreview = ""; pendingAvatar = null; removeAvatar = false; profileDraft = null;
  }

  async function prepareAvatar(file) {
    if (avatarCropper.active) return avatarCropper.toFile();
    const url = URL.createObjectURL(file);
    try {
      const image = new Image();
      await new Promise((resolve, reject) => { image.onload = resolve; image.onerror = reject; image.src = url; });
      if (!image.naturalWidth || image.naturalWidth * image.naturalHeight > 16000000) throw new Error();
      const scale = Math.min(1, 768 / Math.max(image.naturalWidth, image.naturalHeight));
      const canvas = document.createElement("canvas");
      canvas.width = Math.max(1, Math.round(image.naturalWidth * scale));
      canvas.height = Math.max(1, Math.round(image.naturalHeight * scale));
      canvas.getContext("2d").drawImage(image, 0, 0, canvas.width, canvas.height);
      const blob = await new Promise(resolve => canvas.toBlob(resolve, "image/png"));
      if (!blob) throw new Error();
      return new File([blob], "avatar.png", { type: "image/png" });
    } catch { throw Object.assign(new Error(errorText("invalid_avatar")), { code: "invalid_avatar" }); }
    finally { URL.revokeObjectURL(url); }
  }

  async function saveProfile() {
    const draft = { ...profileDraft }, photo = pendingAvatar, shouldRemove = removeAvatar;
    const version = state.routeVersion;
    state.busy = true; state.error = null;
    render();
    try {
      const preparedPhoto = photo ? await prepareAvatar(photo) : null;
      const saved = await api("profile", { method: "PATCH", body: draft });
      state.profile.profile = saved.profile;
      if (preparedPhoto) {
        const body = new FormData(); body.append("avatar", preparedPhoto);
        state.profile.profile = (await api("profile/avatar", { method: "POST", body })).profile;
      } else if (shouldRemove) state.profile.profile = (await api("profile/avatar", { method: "DELETE" })).profile;
      await refreshAvatar();
      if (state.routeVersion === version) {
        clearProfileDraft();
        state.route = "profile";
      }
      toast(tr("profileSaved"));
    } catch (error) { state.error = error.code || "server_error"; toast(error.message); }
    finally { state.busy = false; render(); }
  }

  async function poll() {
    clearTimeout(pollTimer);
    if (!state.job) return;
    try {
      const result = await api(`jobs/${state.job}`);
      if (result.status === "working") { pollTimer = setTimeout(poll, 2500); return; }
      storage.remove(`quiz-job:${userKey}`);
      state.job = null;
      if (result.status === "done") {
        state.readyId = result.quiz_id;
        if (state.route === "working" || state.route === state.jobTab) await showQuiz(result.quiz_id);
        else { render(); toast(tr("ready")); }
        try { await refreshProfile(); if (state.route === "profile") render(); } catch { /* Next profile visit can retry. */ }
        if (state.jobTab === "file") state.file = null;
      } else {
        state.error = result.error || "generation_failed";
        if (state.route === "working") state.route = state.jobTab || "file";
        render(); toast(errorText(state.error));
      }
    } catch (e) {
      if (["unauthorized", "not_found"].includes(e.code)) {
        storage.remove(`quiz-job:${userKey}`); state.job = null; state.error = e.code;
        if (state.route === "working") state.route = state.jobTab || "file";
        render();
      } else { pollTimer = setTimeout(poll, 6000); }
    }
  }

  document.addEventListener("input", e => { if (e.target.closest("form")) capture(); });
  document.addEventListener("change", async e => {
    if (interactive.change(e.target)) return;
    if (e.target.id === "avatar-input") {
      if (state.busy) return;
      const file = e.target.files[0];
      if (!file) return;
      if (!["image/png", "image/jpeg", "image/webp"].includes(file.type)) return toast(errorText("invalid_avatar"));
      if (file.size > 5 * 1024 * 1024) return toast(errorText("too_large"));
      capture();
      const version = state.routeVersion;
      const ticket = ++avatarLoadTicket;
      avatarLoading = true;
      render();
      try {
        if (!await avatarCropper.load(file)) return;
        if (state.routeVersion !== version || state.route !== "profile-edit") { avatarCropper.clear(); return; }
        if (avatarPreview) URL.revokeObjectURL(avatarPreview);
        pendingAvatar = file; avatarPreview = ""; removeAvatar = false;
      } catch { if (ticket === avatarLoadTicket) toast(errorText("invalid_avatar")); }
      finally {
        if (ticket === avatarLoadTicket) {
          avatarLoading = false;
          if (state.route === "profile-edit") render();
        }
      }
      return;
    }
    if (e.target.id !== "ui-language") { if (e.target.closest("form")) capture(); return; }
    const previous = state.lang;
    state.lang = e.target.value;
    render();
    try {
      if (auth) await api("settings", { method: "PATCH", body: { ui_lang: state.lang } });
      toast(tr("saved"));
    } catch (error) { state.lang = previous; state.error = error.code; render(); }
  });

  document.addEventListener("click", async e => {
    if (e.target.closest(".brand")) { e.preventDefault(); await navigate("file"); return; }
    const button = e.target.closest("button");
    if (!button || button.disabled) return;
    if (interactive.handles(button)) { await interactive.handleClick(button); return; }
    if (button.dataset.go) { await navigate(button.dataset.go); return; }
    if (button.id === "profile-shortcut") { await navigate("profile"); return; }
    if (button.hasAttribute("data-avatar-choose")) { document.querySelector("#avatar-input").click(); return; }
    if (button.hasAttribute("data-avatar-fit")) { avatarCropper.fit(); return; }
    if (button.hasAttribute("data-avatar-reset")) { avatarCropper.reset(); return; }
    if (button.hasAttribute("data-avatar-remove")) {
      capture();
      avatarCropper.clear();
      if (avatarPreview) URL.revokeObjectURL(avatarPreview);
      avatarPreview = ""; pendingAvatar = null; removeAvatar = true;
      render(); return;
    }
    if (button.dataset.difficulty) {
      main.querySelector('[name="difficulty"]').value = button.dataset.difficulty;
      for (const chip of main.querySelectorAll("[data-difficulty]")) { const active = chip === button; chip.classList.toggle("selected", active); chip.setAttribute("aria-pressed", String(active)); }
      capture(); return;
    }
    if (button.hasAttribute("data-add")) { capture(); state.manual.questions.push(blankQuestion()); render(); main.querySelectorAll(".manual-question").item(state.manual.questions.length - 1)?.scrollIntoView({ block: "center" }); return; }
    if (button.hasAttribute("data-remove")) { capture(); state.manual.questions.splice(Number(button.dataset.remove), 1); storage.set(`quiz-draft:${userKey}`, state.manual); render(); return; }
    if (button.dataset.quiz) { await showQuiz(button.dataset.quiz); return; }
    if (button.hasAttribute("data-more")) {
      button.disabled = true;
      try { const result = await api(`quizzes?before=${state.next}`); state.quizzes.push(...result.quizzes); state.next = result.next; if (state.route === "quizzes") render(); }
      catch (error) { toast(error.message); button.disabled = false; }
      return;
    }
    if (button.dataset.start) { const link = botLink(`quiz_${Number(button.dataset.start)}`); link ? openTelegram(link) : toast(tr("openTelegram")); return; }
    if (button.hasAttribute("data-premium")) { const link = botLink("webpremium"); link ? openTelegram(link) : toast(tr("openTelegram")); return; }
    if (button.hasAttribute("data-copy") || button.hasAttribute("data-share")) {
      const link = botLink(`ref_${state.profile?.user.id}`);
      if (!link) return toast(tr("openTelegram"));
      if (button.hasAttribute("data-share")) { openTelegram(`https://t.me/share/url?url=${encodeURIComponent(link)}`); return; }
      try { await navigator.clipboard.writeText(link); toast(tr("copied")); }
      catch { toast(link); }
    }
    if (button.hasAttribute("data-refresh")) {
      try { await refreshProfile(); state.error = null; } catch (error) { state.error = error.code; }
      render();
    }
  });

  document.addEventListener("submit", async e => {
    e.preventDefault();
    const form = e.target;
    if (!(form instanceof HTMLFormElement) || state.busy) return;
    capture();
    if (!auth) return toast(tr("openTelegram"));
    if (form.id === "support-form") { await sendSupport(); return; }
    if (form.id === "interactive-create-form") { await interactive.submit(form); return; }
    if (form.id === "profile-form") { if (!avatarLoading) await saveProfile(); return; }
    if (form.id === "file-form" && !state.file) return toast(tr("selectFile"));
    state.busy = true; state.error = null;
    const submit = form.querySelector('[type="submit"]');
    submit.disabled = true; submit.textContent = tr("loading");
    const origin = state.route;
    try {
      if (form.id === "manual-form") {
        const result = await api("quizzes/manual", { method: "POST", body: state.manual });
        storage.remove(`quiz-draft:${userKey}`);
        state.manual = { title: "", questions: [blankQuestion()], ...defaultOptions() };
        if (state.route === origin) await showQuiz(result.quiz_id, false);
        else { state.readyId = result.quiz_id; toast(tr("ready")); }
      } else {
        let result;
        if (form.id === "file-form") {
          const body = new FormData(); body.append("options", JSON.stringify(state.fileOptions)); body.append("file", state.file);
          result = await api("quizzes/file", { method: "POST", body });
        } else result = await api("quizzes/topic", { method: "POST", body: { ...state.topicOptions, topic: state.topic } });
        state.job = result.job_id; state.jobTab = origin;
        storage.set(`quiz-job:${userKey}`, { id: state.job, tab: origin });
        if (state.route === origin) state.route = "working";
        pollTimer = setTimeout(poll, 1200);
      }
    } catch (error) { state.error = error.code || "server_error"; toast(error.message); }
    finally { state.busy = false; render(); }
  });

  const interactive = window.createInteractive({ api, tr, icon, esc, toast, storage, errorText, render, navigate, openTelegram,
    route: () => state.route, profile: () => state.profile, authorized: () => !!auth, userKey: () => userKey, currentQuiz: () => state.quiz,
    fetchImage: async path => { const response = await fetch(`/api/${path}`, { headers: { Authorization: `tma ${auth}` } }); return response.ok ? response.blob() : null; }
  });
  window.addEventListener("hashchange", () => { if (["file", "topic", "manual", "profile", "interactive"].includes(location.hash.slice(1))) navigate(location.hash.slice(1)); });
  async function init() {
    theme();
    if (tg && auth) { tg.ready(); tg.expand(); tg.onEvent("themeChanged", theme); tg.BackButton.onClick(() => navigate(interactive.routes.includes(state.route) ? "interactive" : "profile")); }
    const initial = location.hash.slice(1);
    if (["file", "topic", "manual", "profile", "interactive"].includes(initial)) { state.route = initial; state.tab = initial; }
    render();
    if (auth) {
      try {
        await refreshProfile();
        const lang = state.profile.settings.ui_lang;
        state.lang = window.QUIZ_LOCALES[lang] ? lang : "en";
        userKey = String(state.profile.user.id);
      } catch (error) { state.error = error.code; }
    }
    const draft = storage.get(`quiz-draft:${userKey}`);
    if (draft && Array.isArray(draft.questions) && draft.questions.length > 0 && draft.questions.length <= 100 && draft.questions.every(q => typeof q.question === "string" && Array.isArray(q.options) && q.options.length === 4)) state.manual = { ...state.manual, ...draft };
    const job = storage.get(`quiz-job:${userKey}`);
    if (auth && /^[a-f0-9]{32}$/.test(job?.id)) { state.job = job.id; state.jobTab = job.tab; pollTimer = setTimeout(poll, 1000); }
    render();
    const sharedRoom = new URLSearchParams(location.search).get("room");
    if (auth && /^[a-f0-9]{32}$/.test(sharedRoom || "")) await interactive.openRoom(sharedRoom);
    else if (state.route === "interactive") await interactive.load("interactive");
  }
  init().catch(() => { state.error = "server_error"; render(); });
})();
