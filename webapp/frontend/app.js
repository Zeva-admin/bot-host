const tg = window.Telegram?.WebApp;
const state = {
  jwt: sessionStorage.getItem("cartplay_jwt") || "",
  profile: null,
  points: null,
  referrals: null,
  matches: [],
  matchOffset: 0,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function fmtDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 16);
  return date.toLocaleString("ru-RU", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" });
}

function showNotice(text, danger = false) {
  const notice = $("#notice");
  notice.textContent = text;
  notice.hidden = false;
  notice.style.borderColor = danger ? "var(--red)" : "var(--line)";
}

function bind(name, value) {
  $$(`[data-bind="${name}"]`).forEach((node) => {
    node.textContent = value;
  });
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(state.jwt ? { Authorization: `Bearer ${state.jwt}` } : {}),
      ...(options.headers || {}),
    },
  });
  if (!res.ok) {
    let message = "Ошибка запроса";
    try {
      const body = await res.json();
      message = body.detail || message;
    } catch (_) {}
    throw new Error(message);
  }
  return res.json();
}

async function auth() {
  tg?.ready();
  tg?.expand();
  const initData = tg?.initData || "";
  if (!initData) {
    showNotice("Открой кабинет через Telegram Mini App.", true);
    throw new Error("Missing Telegram initData");
  }
  const data = await api("/api/auth/telegram", {
    method: "POST",
    body: JSON.stringify({ initData }),
  });
  state.jwt = data.token;
  sessionStorage.setItem("cartplay_jwt", state.jwt);
}

function renderProfile(profile) {
  bind("user_id", profile.user_id);
  bind("first_seen", profile.first_seen_at ? `в игре с ${fmtDate(profile.first_seen_at)}` : "в игре");
  bind("balance", Number(profile.balance || 0).toFixed(2));
  bind("total_won", money(profile.total_won));
  bind("total_lost", money(profile.total_lost));
  bind("total_deposited", money(profile.total_deposited));
  bind("total_withdrawn", money(profile.total_withdrawn));
  bind("matches_total", profile.matches_total || 0);
  bind("winrate", `${profile.winrate || 0}%`);
}

function renderPoints(points) {
  bind("points_balance", Number(points.balance || 0).toLocaleString("ru-RU"));
  bind("points_total", `получено ${Number(points.total_earned || 0).toLocaleString("ru-RU")} всего`);
  bind("points_rate", `Курс: ${points.rate} очков = 1 USDT · доступно к обмену: ${money(points.usd_equivalent)}`);
}

function renderReferrals(referrals) {
  bind("ref_total", referrals.total || 0);
  bind("ref_active", referrals.active || 0);
  bind("ref_points", `+${referrals.points_earned || 0}`);
  bind("ref_link", referrals.referral_link || "");
}

function matchRow(match) {
  const cls = match.result === "win" ? "win" : match.result === "lose" ? "lose" : "draw";
  const amount = match.amount_delta > 0 ? `+${money(match.amount_delta)}` : match.amount_delta < 0 ? `−${money(Math.abs(match.amount_delta))}` : "$0.00";
  const label = match.opponent_id ? `vs UID ${match.opponent_id}` : "матч";
  return `<div class="match-row" data-result="${match.result}">
    <div class="match-dot ${cls}"></div>
    <div>${label}</div>
    <div class="match-date">${fmtDate(match.finished_at || match.created_at)}</div>
    <div class="match-amt ${cls}">${amount}</div>
  </div>`;
}

function renderMatches(matches) {
  $("#matchPreview").innerHTML = matches.slice(0, 5).map(matchRow).join("");
  applyHistoryFilter();
}

function renderRecent(results) {
  const row = $("#recentSuits");
  row.innerHTML = results.map((item) => {
    const cls = item.result === "win" ? "win" : item.result === "lose" ? "lose" : "draw";
    const mark = item.result === "win" ? "♦" : item.result === "lose" ? "♠" : "·";
    return `<div class="suit-mark ${cls}">${mark}</div>`;
  }).join("");
}

function applyHistoryFilter() {
  const filter = $("#historyFilter").value;
  const rows = state.matches.filter((match) => filter === "all" || match.result === filter);
  $("#historyList").innerHTML = rows.map(matchRow).join("");
}

function renderLedger(items) {
  $("#pointsLedger").innerHTML = items.map((item) => {
    const positive = item.delta > 0;
    return `<div class="plain-row">
      <div>
        <div>${item.reason}</div>
        <div class="muted">${fmtDate(item.created_at)}${item.match_id ? ` · ${item.match_id}` : ""}</div>
      </div>
      <div class="match-amt ${positive ? "win" : "lose"}">${positive ? "+" : ""}${item.delta}</div>
    </div>`;
  }).join("");
}

function renderReferralList(items) {
  $("#referralList").innerHTML = items.map((item) => `<div class="plain-row">
    <div>
      <div>${item.display_name}</div>
      <div class="muted">${item.status} · матчей: ${item.matches_count || 0}</div>
    </div>
    <div class="muted">${fmtDate(item.activated_at || item.created_at)}</div>
  </div>`).join("");
}

function renderTickets(items) {
  $("#ticketList").innerHTML = items.map((ticket) => `<div class="plain-row ticket" data-ticket="${ticket.id}">
    <div>
      <div>#${ticket.id} · ${ticket.category || "Другое"}</div>
      <div class="muted">${ticket.status} · ${fmtDate(ticket.created_at)}</div>
    </div>
    <button class="btn" data-open-ticket="${ticket.id}">Открыть</button>
  </div>`).join("");
}

async function loadAll() {
  const [profile, points, referrals, recent, ledger, refs, tickets] = await Promise.all([
    api("/api/me"),
    api("/api/me/points"),
    api("/api/me/referrals"),
    api("/api/me/matches/recent-results?limit=10"),
    api("/api/me/points/ledger?limit=30"),
    api("/api/me/referrals/list"),
    api("/api/me/support/tickets"),
  ]);
  state.profile = profile;
  state.points = points;
  state.referrals = referrals;
  renderProfile(profile);
  renderPoints(points);
  renderReferrals(referrals);
  renderRecent(recent);
  renderLedger(ledger);
  renderReferralList(refs);
  renderTickets(tickets);
  await loadMoreMatches(true);
}

async function loadMoreMatches(reset = false) {
  if (reset) {
    state.matches = [];
    state.matchOffset = 0;
  }
  const batch = await api(`/api/me/matches?limit=20&offset=${state.matchOffset}`);
  state.matches.push(...batch);
  state.matchOffset += batch.length;
  renderMatches(state.matches);
  $("#loadMoreMatches").hidden = batch.length < 20;
}

function nav(page) {
  $$(".tab-page").forEach((node) => node.classList.toggle("active", node.dataset.page === page));
  $$(".nav-item").forEach((node) => node.classList.toggle("active", node.dataset.nav === page));
  window.scrollTo({ top: 0, behavior: "smooth" });
}

async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const input = document.createElement("textarea");
  input.value = text;
  document.body.append(input);
  input.select();
  document.execCommand("copy");
  input.remove();
}

async function openTicket(ticketId) {
  const messages = await api(`/api/me/support/tickets/${ticketId}/messages`);
  const thread = $("#ticketThread");
  thread.hidden = false;
  thread.innerHTML = `<h2>Тикет #${ticketId}</h2>
    ${messages.map((m) => `<div class="message ${m.is_admin ? "admin" : ""}"><div>${m.text}</div><div class="muted">${fmtDate(m.created_at)}</div></div>`).join("")}
    <form id="messageForm" class="ticket-form">
      <textarea name="text" rows="3" placeholder="Ответить"></textarea>
      <button class="btn primary" type="submit">Отправить</button>
    </form>`;
  $("#messageForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const text = new FormData(event.currentTarget).get("text").trim();
    if (!text) return;
    await api(`/api/me/support/tickets/${ticketId}/messages`, { method: "POST", body: JSON.stringify({ text }) });
    await openTicket(ticketId);
  });
}

function wireUi() {
  $$("[data-nav]").forEach((node) => node.addEventListener("click", () => nav(node.dataset.nav)));
  $("#historyFilter").addEventListener("change", applyHistoryFilter);
  $("#loadMoreMatches").addEventListener("click", () => loadMoreMatches(false));
  $("#copyRef").addEventListener("click", async () => {
    await copyText(state.referrals?.referral_link || "");
    showNotice("Реферальная ссылка скопирована.");
  });
  $("#openRedeem").addEventListener("click", () => $("#redeemDialog").showModal());
  $("#submitRedeem").addEventListener("click", async () => {
    const points = Number($("#redeemAmount").value || 0);
    const updated = await api("/api/me/points/redeem", { method: "POST", body: JSON.stringify({ points }) });
    state.points = updated;
    renderPoints(updated);
    renderLedger(await api("/api/me/points/ledger?limit=30"));
    $("#redeemDialog").close();
  });
  $$("[data-action='open-bot']").forEach((node) => {
    node.addEventListener("click", () => {
      const start = node.dataset.start;
      const username = state.referrals?.referral_link?.match(/t\.me\/([^?]+)/)?.[1] || "cartplaybot";
      const url = `https://t.me/${username}?start=${start}`;
      if (tg?.openTelegramLink) tg.openTelegramLink(url);
      else window.open(url, "_blank");
    });
  });
  $("#newTicket").addEventListener("click", () => $("#ticketForm").hidden = false);
  $("#cancelTicket").addEventListener("click", () => $("#ticketForm").hidden = true);
  $("#ticketForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    await api("/api/me/support/tickets", {
      method: "POST",
      body: JSON.stringify({ category: form.get("category"), text: form.get("text") }),
    });
    event.currentTarget.reset();
    event.currentTarget.hidden = true;
    renderTickets(await api("/api/me/support/tickets"));
  });
  $("#ticketList").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-open-ticket]");
    if (button) await openTicket(button.dataset.openTicket);
  });
}

async function boot() {
  wireUi();
  try {
    await auth();
    await loadAll();
  } catch (error) {
    showNotice(error.message || "Не удалось открыть кабинет.", true);
  }
}

boot();
