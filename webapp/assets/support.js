"use strict";
window.createSupport = function (ctx) {
  const { api, tr, esc, icon } = ctx;
  const routes = ["support", "admin-support", "admin-support-thread"];
  const ui = { epoch: 0, error: "", messages: [], threads: [], next: null, older: null, busy: false, loading: false, userId: null, name: "", sent: false };
  const drafts = new Map();
  let timer;
  const key = () => ctx.route() === "support" ? "self" : `admin:${ui.userId}`;
  const path = () => ctx.route() === "support" ? "support/messages" : `admin/support/${ui.userId}/messages`;
  const timestamp = value => new Date(value * 1000).toLocaleString(document.documentElement.lang, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  const back = route => `<button class="back" data-go="${route}">${icon("back")}${tr("back")}</button>`;

  function messageHtml(message) {
    const mine = (ctx.route() === "support") !== (message.sender === "admin");
    return `<article class="support-bubble ${mine ? "mine" : ""}"><small>${(message.sender === "admin" ? tr("supportAdmin") : ctx.route() === "support" ? tr("supportYou") : esc(ui.name))}</small><p>${esc(message.body)}</p><time>${esc(timestamp(message.created_at))}</time></article>`;
  }

  function page() {
    if (ctx.route() === "admin-support") return `${back("profile")}<h1>${tr("supportPanel")}</h1><p class="intro">${tr("supportPanelHint")}</p>${ctx.errorHtml(ui.error)}<div id="support-inbox">${inboxHtml()}</div>${ui.next ? `<button class="secondary" data-support-more>${tr("more")}</button>` : ""}`;
    const admin = ctx.route() === "admin-support-thread";
    return `${back(admin ? "admin-support" : "profile")}<h1>${esc(admin ? ui.name : tr("support"))}</h1><p class="intro">${tr(admin ? "supportAdminReplyHint" : "supportChatIntro")}</p>${ctx.errorHtml(ui.error)}
      ${ui.sent ? `<p class="result-count" role="status">${tr("supportSent")}</p>` : ""}
      ${ui.older ? `<button class="back" data-support-older>${tr("supportOlder")}</button>` : ""}<section class="card support-chat" id="support-messages" role="log" aria-live="polite" aria-label="${tr("supportHistory")}">${ui.messages.length ? ui.messages.map(messageHtml).join("") : `<p class="empty">${tr(ui.loading ? "loading" : "supportNoMessages")}</p>`}</section>
      <form id="support-form"><section class="card"><label class="field"><span>${tr("supportMessage")}</span><textarea name="support_message" maxlength="3000" rows="4" required placeholder="${tr("supportPlaceholder")}" ${ui.busy ? "disabled" : ""}>${esc(drafts.get(key()) || "")}</textarea><small>${tr(admin ? "supportAdminReplyHint" : "supportHint")}</small></label><button type="submit" class="primary" ${ui.busy || !ctx.authorized() || (admin && !ui.userId) ? "disabled" : ""}>${icon("share")}${tr(ui.busy ? "loading" : admin ? "supportReply" : "supportSend")}</button></section></form>`;
  }

  function inboxHtml() {
    return ui.threads.length ? `<section class="card">${ui.threads.map(thread => `<button class="support-thread" data-support-thread="${thread.user_id}"><span class="small-icon">${icon("user")}</span><span><strong>${esc(thread.name)}</strong><small>${esc(thread.preview)}</small><time>${esc(timestamp(thread.updated_at))}</time></span>${thread.unread ? `<b class="unread-badge">${thread.unread}</b>` : icon("chevron")}</button>`).join("")}</section>` : `<section class="card empty">${tr(ui.loading ? "loading" : "supportInboxEmpty")}</section>`;
  }

  function capture(form) {
    if (form?.id !== "support-form") return false;
    if (!ui.busy) drafts.set(key(), String(new FormData(form).get("support_message") || ""));
    return true;
  }

  function onNavigate(old, route) {
    clearTimeout(timer); ui.epoch++;
    ui.error = "";
    if (old !== route) { ui.messages = []; ui.threads = []; ui.older = null; ui.next = null; ui.sent = false; ui.paged = false; }
  }

  async function refresh(epoch, initial = false) {
    clearTimeout(timer);
    const route = ctx.route();
    if (!routes.includes(route) || !ctx.authorized()) return;
    try {
      if (route === "admin-support") {
        const data = await api("admin/support");
        if (epoch !== ui.epoch) return;
        const changed = JSON.stringify(ui.threads) !== JSON.stringify(data.threads);
        const freshIds = new Set(data.threads.map(thread => thread.user_id));
        ui.threads = ui.paged ? [...data.threads, ...ui.threads.filter(thread => !freshIds.has(thread.user_id))] : data.threads;
        if (!ui.paged) ui.next = data.next;
        if (initial || changed || ui.error) { ui.error = ""; ui.loading = false; ctx.render(); }
      } else {
        const after = !initial && ui.messages.length ? ui.messages[ui.messages.length - 1].id : 0;
        const data = await api(`${path()}${after ? `?after=${after}` : ""}`);
        if (epoch !== ui.epoch) return;
        if (initial) { ui.messages = data.messages; ui.older = data.next_before; ui.scrollToEnd = true; }
        else { const ids = new Set(ui.messages.map(message => message.id)); ui.messages.push(...data.messages.filter(message => !ids.has(message.id))); }
        if (initial || ui.error) { ui.error = ""; ui.loading = false; ctx.render(); }
        else if (data.messages.length) {
          const log = document.querySelector("#support-messages");
          if (log) {
            const nearEnd = log.scrollHeight - log.scrollTop - log.clientHeight < 48;
            log.innerHTML = ui.messages.map(messageHtml).join("");
            if (nearEnd) log.scrollTop = log.scrollHeight;
          }
        }
      }
    } catch (error) {
      if (epoch !== ui.epoch) return;
      ui.loading = false; ui.error = error.code; capture(document.querySelector("#support-form")); ctx.render();
      if (ctx.isAuthError(error.code) || error.code === "admin_only") return;
    }
    if (epoch === ui.epoch && !document.hidden) timer = setTimeout(() => refresh(epoch), 5000);
  }

  async function load() {
    if (!ctx.authorized()) { ui.loading = false; return; }
    ui.loading = true;
    await refresh(ui.epoch, true);
  }

  async function submit(form) {
    capture(form);
    if (ui.busy) return;
    const body = drafts.get(key()) || "", draftKey = key(), epoch = ui.epoch, destination = ctx.route() === "support" ? "support" : path();
    if (!body.trim()) return ctx.toast(ctx.errorText("invalid_support_message"));
    ui.busy = true; ui.error = ""; ui.sent = false; ctx.render();
    try {
      await api(destination, { method: "POST", body: { message: body } });
      drafts.delete(draftKey);
      if (epoch === ui.epoch) { ui.sent = true; await refresh(epoch); }
      ctx.toast(tr("supportSent"));
    } catch (error) { if (epoch === ui.epoch) ui.error = error.code; ctx.toast(error.message); }
    finally { ui.busy = false; ui.scrollToEnd = true; if (routes.includes(ctx.route())) ctx.render(); }
  }

  function handles(button) { return [...button.attributes].some(attribute => attribute.name.startsWith("data-support-")); }
  async function handleClick(button) {
    if (button.dataset.supportThread) {
      const thread = ui.threads.find(item => String(item.user_id) === button.dataset.supportThread);
      if (!thread) return;
      ui.userId = thread.user_id; ui.name = thread.name;
      await ctx.navigate("admin-support-thread");
    } else if (button.hasAttribute("data-support-older")) {
      const epoch = ui.epoch; button.disabled = true;
      try { const data = await api(`${path()}?before=${ui.older}`); if (epoch !== ui.epoch) return; capture(document.querySelector("#support-form")); ui.messages.unshift(...data.messages); ui.older = data.next_before; ui.scrollToEnd = false; ctx.render(); }
      catch (error) { ctx.toast(error.message); button.disabled = false; }
    } else if (button.hasAttribute("data-support-more")) {
      const epoch = ui.epoch; button.disabled = true;
      try { const data = await api(`admin/support?before=${ui.next}`); if (epoch !== ui.epoch) return; const ids = new Set(ui.threads.map(thread => thread.user_id)); ui.threads.push(...data.threads.filter(thread => !ids.has(thread.user_id))); ui.next = data.next; ui.paged = true; ctx.render(); }
      catch (error) { ctx.toast(error.message); button.disabled = false; }
    }
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) clearTimeout(timer);
    else if (routes.includes(ctx.route())) refresh(ui.epoch);
  });
  function afterRender() {
    const log = document.querySelector("#support-messages");
    if (log && ui.scrollToEnd) { log.scrollTop = log.scrollHeight; ui.scrollToEnd = false; }
  }
  return { routes, page, capture, onNavigate, load, submit, handles, handleClick, afterRender };
};
