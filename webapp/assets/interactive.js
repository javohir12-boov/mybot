"use strict";
window.createInteractive = function (ctx) {
  const { api, tr, icon, esc, toast } = ctx;
  const categories = [["education", "book"], ["love", "heart"], ["friendship", "team"], ["interests", "spark"], ["other", "globe"]];
  const subjects = ["general", "math", "history", "science", "languages", "technology"];
  const routes = ["interactive", "interactive-create", "interactive-room", "published"];
  const blank = () => ({ question: "", options: ["", "", "", ""], correct_index: 0, weights: [0, 30, 70, 100] });
  const ui = { category: "education", subject: "all", tab: "quizzes", lobby: null, draft: null, saved: [], source: "new", sourceId: "", roomId: "", room: null, error: "", busy: false, answering: false, epoch: 0 };
  let refreshTimer, clockTimer, imageUrl = "";
  ui.own = null; ui.ownNext = null; ui.confirmDelete = ""; ui.removing = false;
  const selected = (a, b) => String(a) === String(b) ? " selected" : "";
  const errorHtml = () => ctx.errorHtml(ui.error);
  const person = p => `<span class="live-person-avatar">${esc((p.name || "Q")[0].toUpperCase())}</span>`;
  const kindLabel = kind => tr(kind === "affinity" ? "liveKindAffinity" : "liveKindKnowledge");
  const back = () => `<button class="back" data-go="interactive">${icon("back")}${tr("liveBack")}</button>`;

  function hubPage() {
    const data = ui.lobby || { rooms: [], quizzes: [], online: [], online_count: 0, leaderboard: [] };
    const cards = ui.tab === "rooms" ? data.rooms : data.quizzes;
    return `<div class="eyebrow live-eyebrow"><span class="live-dot"></span>${tr("liveEyebrow")}</div><h1>${tr("liveTitle")}<br><span class="accent">${tr("liveAccent")}</span></h1><p class="intro">${tr("liveIntro")}</p>
      <div class="live-categories" aria-label="${tr("liveCategory")}">${categories.map(([key, symbol]) => `<button type="button" class="live-category ${ui.category === key ? "selected" : ""}" data-live-category="${key}" aria-pressed="${ui.category === key}">${icon(symbol)}<span>${tr(key)}</span></button>`).join("")}</div>
      ${ui.category === "education" ? `<div class="live-subjects"><button class="live-subject ${ui.subject === "all" ? "selected" : ""}" data-live-subject="all">${tr("allSubjects")}</button>${subjects.map(s => `<button class="live-subject ${ui.subject === s ? "selected" : ""}" data-live-subject="${s}">${tr(s)}</button>`).join("")}</div>` : ""}
      ${errorHtml()}<div class="live-section-head"><div class="live-switch"><button class="${ui.tab === "rooms" ? "selected" : ""}" data-live-tab="rooms">${tr("liveRooms")} <b>${data.rooms.length}</b></button><button class="${ui.tab === "quizzes" ? "selected" : ""}" data-live-tab="quizzes">${tr("publicTests")}</button></div><button class="live-add icon-button" data-go="interactive-create" aria-label="${tr("addPublicTest")}">${icon("plus")}</button></div>
      <div class="live-cards">${!ui.lobby && ctx.authorized() && !ui.error ? `<div class="empty">${tr("loading")}</div>` : cards.length ? (ui.tab === "rooms" ? cards.map(roomCard) : cards.map(quizCard)).join("") : `<div class="card empty"><p>${tr(ui.tab === "rooms" ? "liveEmptyRooms" : "liveEmptyTests")}</p><button class="secondary" ${ui.tab === "rooms" ? 'data-live-tab="quizzes"' : 'data-go="interactive-create"'}>${icon("plus")}${tr(ui.tab === "rooms" ? "publicTests" : "addPublicTest")}</button></div>`}</div>
      <button class="secondary" data-go="interactive-create">${icon("edit")}${tr("addPublicTest")}</button>
      <section class="card live-online-card"><div class="live-section-head"><h2>${tr("liveOnlinePeople")}</h2><span class="live-count"><i class="live-dot"></i>${data.online_count} ${tr("liveOnline")}</span></div><div class="live-online-people">${data.online.length ? data.online.map(p => `<div>${person(p)}<span>${esc(p.name)}</span></div>`).join("") : `<p class="muted">${tr("liveNoPeople")}</p>`}</div></section>
      <section class="card"><div class="card-head"><span class="small-icon">${icon("medal")}</span><h2>${tr("liveAuthors")}</h2></div><p class="muted">${tr("liveRankingHint")}</p>${data.leaderboard.length ? data.leaderboard.map(p => `<div class="live-rank"><span class="live-rank-number ${p.rank <= 3 ? "top" : ""}">${p.rank}</span>${person(p)}<div><strong>${esc(p.name)}</strong>${p.username ? `<small>@${esc(p.username)}</small>` : ""}</div><span class="live-rank-plays"><b>${p.plays}</b><small>${tr("livePlays")}</small></span></div>`).join("") : `<p class="empty">${tr("liveNoRanking")}</p>`}<p class="muted live-ranking-rule">${tr("liveRankingRule")}</p></section>
      <aside class="tip">${icon("bulb")}<p>${tr("liveRules")}</p></aside>`;
  }

  function quizCard(q) {
    return `<article class="card live-quiz-card"><div class="live-quiz-top"><span class="small-icon">${icon(q.kind === "affinity" ? "heart" : "book")}</span><span class="tag">${kindLabel(q.kind)}</span>${q.is_mine ? `<span class="tag">${tr("liveMine")}</span>` : ""}</div><h3>${esc(q.title)}</h3><p class="muted">${q.question_count} ${tr("questions")} · ${q.open_period} ${tr("seconds")}</p><div class="live-card-bottom"><div class="live-card-author"><span>${esc(q.builtin ? tr("liveBuiltin") : q.author.name)}</span><small>${q.plays} ${tr("livePlays")}</small></div><button class="live-start" data-live-start="${esc(q.id)}">${tr("startTogether")}${icon("arrow")}</button></div></article>`;
  }

  function roomCard(r) {
    return `<article class="card live-quiz-card"><div class="live-quiz-top"><span class="live-count"><i class="live-dot"></i>${r.online} ${tr("liveOnline")}</span><span class="tag">${kindLabel(r.kind)}</span></div><h3>${esc(r.title)}</h3><p class="muted">${tr("liveQuestion")} ${r.question_number} / ${r.question_count}</p><button class="primary" data-live-room="${esc(r.id)}">${icon("team")}${tr("joinRoom")}</button></article>`;
  }

  function getDraft() {
    if (!ui.draft) {
      const saved = ctx.storage.get(`interactive-draft:${ctx.userKey()}`);
      const valid = saved && Array.isArray(saved.questions) && saved.questions.length > 0 && saved.questions.length <= 50 && saved.questions.every(q => typeof q.question === "string" && Array.isArray(q.options) && q.options.length === 4 && Array.isArray(q.weights) && q.weights.length === 4);
      ui.draft = valid ? saved : { title: "", category: ui.category, subject: ui.subject === "all" ? "general" : ui.subject, kind: ["love", "friendship", "interests"].includes(ui.category) ? "affinity" : "knowledge", open_period: 30, visibility: "public", questions: [blank()] };
    }
    return ui.draft;
  }

  function ownPage() {
    return `<button class="back" data-go="profile">${icon("back")}${tr("back")}</button><h1>${tr("myPublished")}</h1><p class="intro">${tr("myPublishedHint")}</p>${errorHtml()}
      ${ui.own === null ? `<p class="empty">${tr(ctx.authorized() ? "loading" : "openTelegram")}</p>` : ui.own.length ? ui.own.map(q => `<article class="card"><span class="tag">${tr(q.visibility === "private" ? "visibilityPrivate" : "visibilityPublic")}</span><h2 class="published-title">${esc(q.title)}</h2><p class="muted">${q.question_count} ${tr("questions")}</p>${(q.rooms || []).map(room => `<button class="secondary" data-live-room="${esc(room.id)}">${icon("team")}${tr("liveReturnRoom")}</button>`).join("")}<div class="split-actions"><button class="secondary" data-live-start="${esc(q.id)}">${icon("team")}${tr("startTogether")}</button><button class="secondary danger" data-live-delete="${esc(q.id)}" ${ui.removing ? "disabled" : ""}>${icon("trash")}${tr("removePublished")}</button></div>${ui.confirmDelete === q.id ? `<div class="delete-confirm"><p>${tr("removePublishedConfirm")}</p><div class="split-actions"><button class="primary danger" data-live-delete-confirm="${esc(q.id)}" ${ui.removing ? "disabled" : ""}>${tr("removePublished")}</button><button class="secondary" data-live-delete-cancel ${ui.removing ? "disabled" : ""}>${tr("cancel")}</button></div></div>` : ""}</article>`).join("") : `<section class="card empty">${tr("noPublished")}</section>`}
      ${ui.ownNext ? `<button class="secondary" data-live-own-more>${tr("more")}</button>` : ""}<button class="secondary" data-go="interactive-create">${icon("plus")}${tr("addPublicTest")}</button>`;
  }

  function createPage() {
    const draft = getDraft(), affinity = draft.kind === "affinity";
    return `${back()}<h1>${tr("liveNewTitle")}</h1><p class="intro">${tr("liveNewIntro")}</p><form id="interactive-create-form">${errorHtml()}
      <div class="live-source-switch"><button type="button" class="${ui.source === "new" ? "selected" : ""}" data-live-source="new">${tr("liveNewSource")}</button><button type="button" class="${ui.source === "saved" ? "selected" : ""}" data-live-source="saved">${tr("liveSavedSource")}</button></div>
      ${ui.source === "saved" ? `<section class="card"><label class="field"><span>${tr("liveChooseSaved")}</span><select id="live-saved-quiz" required><option value="">${tr("liveChooseSaved")}</option>${ui.saved.filter(q => q.question_count > 0 && q.question_count <= 50).map(q => `<option value="${q.id}"${selected(ui.sourceId, q.id)}>${esc(q.title)} · ${q.question_count}</option>`).join("")}</select><small>${tr("liveSavedHint")}</small></label></section>` : ""}
      <section class="card"><label class="field"><span>${tr("testTitle")}</span><input name="live_title" type="text" required minlength="3" maxlength="120" value="${esc(draft.title)}" placeholder="${tr("titlePlaceholder")}"></label><label class="field"><span>${tr("visibility")}</span><select name="live_visibility"><option value="public"${selected(draft.visibility || "public", "public")}>${tr("visibilityPublic")}</option><option value="private"${selected(draft.visibility, "private")}>${tr("visibilityPrivate")}</option></select><small>${tr("visibilityHint")}</small></label><div class="grid-two"><label class="field"><span>${tr("liveCategory")}</span><select name="live_category">${categories.map(([c]) => `<option value="${c}"${selected(draft.category, c)}>${tr(c)}</option>`).join("")}</select></label><label class="field"><span>${tr("liveKind")}</span><select name="live_kind" ${ui.source === "saved" ? "disabled" : ""}><option value="knowledge"${selected(draft.kind, "knowledge")}>${tr("liveKindKnowledge")}</option><option value="affinity"${selected(draft.kind, "affinity")}>${tr("liveKindAffinity")}</option></select></label>${draft.category === "education" ? `<label class="field"><span>${tr("liveSubject")}</span><select name="live_subject">${subjects.map(s => `<option value="${s}"${selected(draft.subject, s)}>${tr(s)}</option>`).join("")}</select></label>` : ""}<label class="field"><span>${tr("time")}</span><select name="live_period">${[10, 15, 20, 30, 45, 60, 90, 120].map(n => `<option value="${n}"${selected(draft.open_period, n)}>${n} ${tr("seconds")}</option>`).join("")}</select></label></div></section>
      ${ui.source === "new" ? `${affinity ? `<aside class="tip live-weight-tip">${icon("heart")}<p>${tr("liveWeightsHint")}</p></aside>` : ""}${draft.questions.map((q, n) => questionEditor(q, n, affinity)).join("")}<button class="secondary" type="button" data-live-add-question ${draft.questions.length >= 50 ? "disabled" : ""}>${icon("plus")}${tr("addQuestion")}</button><p class="under-button">${tr("liveDraftHint")}</p>` : ""}
      <button class="primary" type="submit" ${ui.busy ? "disabled" : ""}>${icon("share")}${tr(ui.busy ? "loading" : "livePublish")}</button><p class="under-button">${tr("visibilityHint")}</p></form>`;
  }

  function questionEditor(q, n, affinity) {
    return `<section class="card live-question-editor"><div class="question-head"><span>${tr("question")} ${n + 1}</span>${getDraft().questions.length > 1 ? `<button type="button" class="icon-button" data-live-remove-question="${n}" aria-label="${tr("remove")}">${icon("trash")}</button>` : ""}</div><label class="field"><span>${tr("question")}</span><input name="live_question" type="text" required maxlength="300" value="${esc(q.question)}" placeholder="${tr("questionPlaceholder")}"></label>${q.options.map((o, i) => `<div class="answer-row">${!affinity ? `<input type="radio" name="live_correct_${n}" value="${i}"${q.correct_index === i ? " checked" : ""} aria-label="${tr("correctAnswer")} ${"ABCD"[i]}">` : ""}<span class="answer-letter">${"ABCD"[i]}</span><input type="text" name="live_option" required maxlength="100" value="${esc(o)}" placeholder="${tr("answer")} ${"ABCD"[i]}" aria-label="${tr("answer")} ${"ABCD"[i]}">${affinity ? `<label class="live-weight"><span>${tr("liveWeight")}</span><input name="live_weight" type="number" required min="0" max="100" step="1" value="${Number(q.weights[i])}"></label>` : ""}</div>`).join("")}</section>`;
  }

  function capture(form) {
    if (form.id !== "interactive-create-form") return false;
    const draft = getDraft(), data = new FormData(form);
    draft.visibility = String(data.get("live_visibility") || "public");
    draft.title = String(data.get("live_title") || "");
    draft.category = data.get("live_category") || "education";
    draft.subject = data.get("live_subject") || "general";
    draft.kind = ui.source === "saved" ? "knowledge" : data.get("live_kind") || "knowledge";
    draft.open_period = Number(data.get("live_period") || 30);
    if (ui.source === "new") draft.questions = [...form.querySelectorAll(".live-question-editor")].map((section, n) => ({
      question: section.querySelector('[name="live_question"]').value,
      options: [...section.querySelectorAll('[name="live_option"]')].map(e => e.value),
      correct_index: Number(section.querySelector('input[type="radio"]:checked')?.value || 0),
      weights: section.querySelectorAll('[name="live_weight"]').length === 4 ? [...section.querySelectorAll('[name="live_weight"]')].map(e => Number(e.value)) : draft.questions[n]?.weights || [0, 30, 70, 100]
    }));
    ctx.storage.set(`interactive-draft:${ctx.userKey()}`, draft);
    return true;
  }

  function roomPage() {
    const r = ui.room;
    if (!r) return `${back()}${errorHtml()}${ui.error ? "" : `<div class="card loading-card"><div class="spinner"></div><p>${tr("loading")}</p></div>`}`;
    const result = r.my_result;
    return `<div class="live-room-toolbar"><button class="back" data-live-leave>${icon("back")}${tr("liveLeave")}</button><button class="icon-button" data-live-share aria-label="${tr("liveShare")}">${icon("share")}</button></div>${errorHtml()}<div class="live-room-meta"><span class="live-count"><i class="live-dot"></i>${r.online_count} ${tr("liveOnline")}</span><span class="tag">${kindLabel(r.kind)}</span></div><h1 class="live-room-title">${esc(r.title)}</h1>
      ${r.status === "active" ? `<section class="card live-playing"><div class="live-question-meta"><span>${tr("liveQuestion")} ${r.question_index + 1} / ${r.question_count}</span><span class="live-clock" data-live-countdown>—</span></div><div class="live-progress"><span style="width:${100 * (r.question_index + 1) / r.question_count}%"></span></div><h2>${esc(r.question.text)}</h2>${r.question.has_image ? `<img class="live-question-image" data-live-image alt="${tr("question")}" hidden>` : ""}<div class="live-options">${r.question.options.map((o, i) => `<button class="live-option ${r.selected === i ? "chosen" : ""}" data-live-answer="${i}" ${r.selected !== null || ui.answering ? "disabled" : ""}><span>${"ABCD"[i]}</span><b>${esc(o)}</b>${r.selected === i ? icon("check") : ""}</button>`).join("")}</div><p class="live-answer-hint" role="status">${tr(r.selected !== null ? "liveAnswerAccepted" : "liveChooseAnswer")}</p></section>` : `<section class="card live-result"><div class="success-symbol">${icon(r.kind === "affinity" ? "heart" : "check")}</div><h2>${tr(r.status === "stopped" ? "liveStopped" : "liveFinished")}</h2>${["idle", "deleted"].includes(r.reason) ? `<p class="muted">${tr(r.reason === "deleted" ? "publishedRemovedReason" : "liveIdleReason")}</p>` : ""}<span>${tr(r.kind === "affinity" ? "liveAffinityResult" : "liveResult")}</span><strong class="live-percent">${result.percent}%</strong><p>${tr("liveAnswered")}: ${result.answered} / ${r.question_count}</p>${r.kind === "affinity" ? `<p class="muted">${tr("liveAffinityHint")}</p>` : ""}${!result.complete ? `<span class="tag">${tr("livePartial")}</span>` : ""}${r.can_restart ? `<button class="primary" data-live-start="${r.quiz_id}">${icon("plus")}${tr("liveRestart")}</button>` : ""}</section>`}
      <div class="split-actions"><button class="secondary" data-live-share>${icon("share")}${tr("liveShare")}</button><button class="secondary" data-live-copy>${icon("copy")}${tr("liveCopy")}</button></div>
      <section class="card"><div class="card-head"><span class="small-icon">${icon("team")}</span><h2>${tr("liveParticipants")}</h2><span class="tag">${r.member_count}</span></div>${r.players.map((p, n) => `<div class="live-rank"><span class="live-rank-number">${n + 1}</span>${person(p)}<div><strong>${esc(p.name)}${p.is_me ? " · ✓" : ""}</strong><small>${p.answered} ${tr("liveAnswered").toLowerCase()}${p.online ? ` · ${tr("liveOnline")}` : ""}</small></div>${r.kind === "knowledge" ? `<span class="live-rank-plays"><b>${p.score}</b><small>${tr("liveScore")}</small></span>` : ""}</div>`).join("")}</section><aside class="tip">${icon("bulb")}<p>${tr("liveRules")}</p></aside>`;
  }

  function paint() {
    if (routes.includes(ctx.route())) ctx.render();
  }

  function stopTimers() {
    clearTimeout(refreshTimer); clearInterval(clockTimer);
    if (imageUrl) URL.revokeObjectURL(imageUrl);
    imageUrl = "";
  }

  function onNavigate(oldRoute, route) {
    stopTimers(); ui.epoch += 1; ui.error = "";
    if (oldRoute === "interactive-room" && route !== "interactive-room" && ui.roomId && ctx.authorized()) {
      api(`interactive/rooms/${ui.roomId}/leave`, { method: "POST" }).catch(() => {});
      const url = new URL(location.href); url.searchParams.delete("room"); history.replaceState(null, "", url);
    }
  }

  async function load(route) {
    if (!ctx.authorized()) return;
    const epoch = ui.epoch;
    if (route === "published") {
      try { const data = await api("interactive/quizzes/mine"); if (epoch === ui.epoch) { ui.own = data.quizzes; ui.ownNext = data.next; } }
      catch (e) { if (epoch === ui.epoch) ui.error = e.code; }
      if (epoch === ui.epoch) paint();
      return;
    }
    if (route === "interactive") await refreshLobby(epoch);
    if (route === "interactive-create") {
      try { ui.saved = (await api("quizzes")).quizzes; }
      catch (e) { ui.error = e.code; }
      if (epoch === ui.epoch && ctx.route() === route) paint();
    }
    if (route === "interactive-room" && ui.roomId) {
      try { await api(`interactive/rooms/${ui.roomId}/join`, { method: "POST" }); await refreshRoom(epoch); }
      catch (e) { if (epoch === ui.epoch) { ui.error = e.code; paint(); } }
    }
  }

  async function refreshLobby(epoch) {
    clearTimeout(refreshTimer);
    try {
      const result = await api(`interactive?category=${ui.category}&subject=${ui.subject}`);
      if (epoch !== ui.epoch || ctx.route() !== "interactive") return;
      ui.lobby = result; ui.error = ""; paint();
    } catch (e) { if (epoch !== ui.epoch) return; ui.error = e.code; paint(); if (ctx.isAuthError(e.code)) return; }
    if (epoch === ui.epoch && ctx.route() === "interactive" && !document.hidden) refreshTimer = setTimeout(() => refreshLobby(epoch), 8000);
  }

  async function refreshRoom(epoch) {
    clearTimeout(refreshTimer);
    try {
      const result = await api(`interactive/rooms/${ui.roomId}`);
      if (epoch !== ui.epoch || ctx.route() !== "interactive-room") return;
      ui.room = result; ui.clockOffset = Date.now() / 1000 - result.server_time; ui.error = ""; paint();
      if (result.status !== "active") return;
    } catch (e) { if (epoch !== ui.epoch) return; ui.error = e.code; paint(); if (ctx.isAuthError(e.code) || ["not_found", "join_room_first"].includes(e.code)) return; }
    if (epoch === ui.epoch && ctx.route() === "interactive-room" && !document.hidden) refreshTimer = setTimeout(() => refreshRoom(epoch), 2000);
  }

  function afterRender() {
    clearInterval(clockTimer);
    if (imageUrl) URL.revokeObjectURL(imageUrl);
    imageUrl = "";
    if (ctx.route() !== "interactive-room" || !ui.room || document.hidden) return;
    const snapshot = ui.room, offset = ui.clockOffset;
    const tick = () => {
      const node = document.querySelector("[data-live-countdown]");
      if (!node) return;
      const left = Math.max(0, Math.ceil(snapshot.deadline + offset - Date.now() / 1000));
      node.textContent = `${left}s`;
      node.classList.toggle("urgent", left <= 5);
      if (!left) document.querySelectorAll("[data-live-answer]").forEach(b => { b.disabled = true; });
    };
    tick(); clockTimer = setInterval(tick, 250);
    const img = document.querySelector("[data-live-image]");
    if (img) ctx.fetchImage(`interactive/rooms/${snapshot.id}/images/${snapshot.question_index}`).then(blob => {
      if (!blob || !img.isConnected) return;
      imageUrl = URL.createObjectURL(blob); img.src = imageUrl; img.hidden = false;
    }).catch(() => {});
  }

  async function openRoom(id) {
    if (!ctx.authorized()) return toast(tr("openTelegram"));
    ui.roomId = id; ui.room = null;
    const url = new URL(location.href); url.searchParams.set("room", id); history.replaceState(null, "", url);
    await ctx.navigate("interactive-room");
  }

  async function start(quizId, button) {
    if (!ctx.authorized()) return toast(tr("openTelegram"));
    button.disabled = true;
    try { const result = await api("interactive/rooms", { method: "POST", body: { quiz_id: quizId } }); await openRoom(result.room_id); }
    catch (e) { ui.error = e.code; toast(e.message); paint(); }
    finally { button.disabled = false; }
  }

  async function handleClick(button) {
    if (button.dataset.liveDelete) { ui.confirmDelete = button.dataset.liveDelete; paint(); return true; }
    if (button.hasAttribute("data-live-delete-cancel")) { ui.confirmDelete = ""; paint(); return true; }
    if (button.dataset.liveDeleteConfirm) {
      if (ui.removing) return true;
      const id = button.dataset.liveDeleteConfirm; ui.removing = true; paint();
      try { await api(`interactive/quizzes/${id}`, { method: "DELETE" }); ui.confirmDelete = ""; await load("published"); toast(tr("publishedRemoved")); }
      catch (e) { ui.error = e.code; toast(e.message); }
      finally { ui.removing = false; paint(); }
      return true;
    }
    if (button.hasAttribute("data-live-own-more")) {
      button.disabled = true;
      try { const data = await api(`interactive/quizzes/mine?before=${encodeURIComponent(ui.ownNext)}`); ui.own.push(...data.quizzes); ui.ownNext = data.next; paint(); }
      catch (e) { toast(e.message); button.disabled = false; }
      return true;
    }
    if (button.hasAttribute("data-live-category") || button.hasAttribute("data-live-subject")) {
      if (button.dataset.liveCategory) { ui.category = button.dataset.liveCategory; ui.subject = "all"; }
      else ui.subject = button.dataset.liveSubject;
      ui.lobby = null; ui.epoch += 1; paint();
      if (ctx.authorized()) await refreshLobby(ui.epoch);
      return true;
    }
    if (button.dataset.liveTab) { ui.tab = button.dataset.liveTab; paint(); return true; }
    if (button.dataset.liveStart) { await start(button.dataset.liveStart, button); return true; }
    if (button.dataset.liveRoom) { await openRoom(button.dataset.liveRoom); return true; }
    if (button.hasAttribute("data-live-leave")) { await ctx.navigate("interactive"); return true; }
    if (button.hasAttribute("data-live-answer")) {
      if (!ui.room || ui.answering || ui.room.selected !== null) return true;
      const roomId = ui.roomId, index = ui.room.question_index, option = Number(button.dataset.liveAnswer), epoch = ui.epoch;
      ui.answering = true; paint();
      try { await api(`interactive/rooms/${roomId}/answers`, { method: "POST", body: { question_index: index, option_index: option } }); }
      catch (e) { if (epoch === ui.epoch) { ui.error = e.code; toast(e.message); } }
      finally { ui.answering = false; if (epoch === ui.epoch) await refreshRoom(epoch); }
      return true;
    }
    if (button.hasAttribute("data-live-share") || button.hasAttribute("data-live-copy")) {
      const name = ctx.profile()?.bot_username;
      if (!name || !ui.roomId) { toast(tr("openTelegram")); return true; }
      const link = `https://t.me/${encodeURIComponent(name)}?start=room_${ui.roomId}`;
      if (button.hasAttribute("data-live-share")) ctx.openTelegram(`https://t.me/share/url?url=${encodeURIComponent(link)}`);
      else { try { await navigator.clipboard.writeText(link); toast(tr("copied")); } catch { toast(link); } }
      return true;
    }
    if (button.dataset.liveSource) {
      capture(document.querySelector("#interactive-create-form")); ui.source = button.dataset.liveSource;
      if (ui.source === "saved") getDraft().kind = "knowledge";
      paint(); return true;
    }
    if (button.hasAttribute("data-live-add-question")) {
      capture(document.querySelector("#interactive-create-form")); getDraft().questions.push(blank()); paint();
      document.querySelectorAll(".live-question-editor").item(getDraft().questions.length - 1)?.scrollIntoView({ block: "center" });
      return true;
    }
    if (button.hasAttribute("data-live-remove-question")) {
      capture(document.querySelector("#interactive-create-form")); getDraft().questions.splice(Number(button.dataset.liveRemoveQuestion), 1); paint(); return true;
    }
    if (button.dataset.publishExisting) {
      await ctx.navigate("interactive-create"); ui.source = "saved"; ui.sourceId = button.dataset.publishExisting;
      const quiz = ctx.currentQuiz(); if (quiz) { getDraft().title = quiz.title.slice(0, 120); getDraft().open_period = Math.max(10, Math.min(120, quiz.open_period)); getDraft().kind = "knowledge"; }
      if (!ui.saved.some(q => String(q.id) === ui.sourceId) && quiz) ui.saved.push({ ...quiz, question_count: quiz.questions.length });
      paint(); return true;
    }
    return false;
  }

  function change(target) {
    const form = target.closest("#interactive-create-form");
    if (!form) return false;
    capture(form);
    if (target.id === "live-saved-quiz") {
      ui.sourceId = target.value;
      const q = ui.saved.find(q => String(q.id) === target.value);
      if (q) { getDraft().title = q.title.slice(0, 120); getDraft().open_period = Math.max(10, Math.min(120, q.open_period)); }
      paint();
    } else if (["live_category", "live_kind"].includes(target.name)) {
      if (target.name === "live_category" && ui.source === "new") getDraft().kind = ["love", "friendship", "interests"].includes(getDraft().category) ? "affinity" : "knowledge";
      paint();
    }
    return true;
  }

  async function submit(form) {
    capture(form);
    if (ui.busy) return;
    if (!ctx.authorized()) return toast(tr("openTelegram"));
    const data = { ...getDraft() };
    if (ui.source === "saved") { if (!ui.sourceId) return; data.source_quiz_id = Number(ui.sourceId); delete data.questions; }
    ui.busy = true; ui.error = ""; paint();
    try {
      const published = await api("interactive/quizzes", { method: "POST", body: data });
      ui.category = data.category; ui.subject = "all"; ui.tab = "quizzes"; ui.lobby = null;
      ctx.storage.remove(`interactive-draft:${ctx.userKey()}`); ui.draft = null; ui.source = "new"; ui.sourceId = "";
      if (data.visibility === "private") {
        try {
          const room = await api("interactive/rooms", { method: "POST", body: { quiz_id: published.quiz_id } });
          await openRoom(room.room_id);
        } catch (error) { await ctx.navigate("published"); throw error; }
      } else await ctx.navigate("interactive");
      toast(tr("livePublished"));
    } catch (e) { ui.error = e.code; toast(e.message); }
    finally { ui.busy = false; paint(); }
  }

  document.addEventListener("visibilitychange", () => {
    if (!routes.includes(ctx.route()) || !ctx.authorized()) return;
    if (document.hidden) stopTimers(); else load(ctx.route());
  });
  const handles = button => button.hasAttribute("data-publish-existing") || [...button.attributes].some(a => a.name.startsWith("data-live-"));
  return { routes, hubPage, createPage, roomPage, ownPage, capture, onNavigate, load, afterRender, handles, handleClick, change, submit, openRoom };
};
