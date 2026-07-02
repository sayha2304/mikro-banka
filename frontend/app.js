/* =========================================================================
   Mikro-Banka Dashboard — istemci
   Gateway'e WebSocket ile bağlanır; adım olaylarını canlı işler.
   Yeni: kademeli onay kuyruğu (Onayla/Reddet + SLA geri sayımı), risk skoru,
   ve iki ayrı metrik — SİSTEM işlem süresi (otomatik) vs YÖNETİCİ onay süresi.
   ========================================================================= */
(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const LEGACY_MS = 24 * 60 * 60 * 1000;

  const el = {
    ws: $("ws"), wsLabel: $("wsLabel"), runBtn: $("runBtn"),
    ms: $("msValue"), factor: $("factor"), oursNote: $("oursNote"),
    kProcessed: $("kProcessed"), kStpRate: $("kStpRate"), kBalance: $("kBalance"),
    nIntake: $("n-intake"), mIntake: $("m-intake"),
    laneAi: $("lane-ai"), dAi: $("d-ai"), fAi: $("f-ai"), sAi: $("s-ai"),
    laneCmp: $("lane-cmp"), dCmp: $("d-cmp"), fCmp: $("f-cmp"), sCmp: $("s-cmp"),
    nRisk: $("n-risk"), mRisk: $("m-risk"),
    nDecision: $("n-decision"), mDecision: $("m-decision"),
    queue: $("queue"), queueEmpty: $("queueEmpty"), queueCount: $("queueCount"),
    log: $("log"), empty: $("empty"), counts: $("counts"),
  };

  const stats = { STP: 0, MANAGER: 0, EXECUTIVE: 0, REJECT: 0 };
  let processed = 0;
  const meta = new Map();     // checkId -> { drawer, amount, findeksScore, riskScore, riskBand }
  const cards = new Map();    // checkId -> { node, timer }
  let currentId = null, clientStart = 0, rafId = null;

  const nf = new Intl.NumberFormat("tr-TR");
  const tl = (n) => new Intl.NumberFormat("tr-TR", { style: "currency", currency: "TRY", maximumFractionDigits: 0 }).format(Number(n) || 0);

  /* ---------- WebSocket ---------- */
  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    ws.onopen = () => { el.ws.className = "wsdot on"; el.wsLabel.textContent = "Canlı bağlantı"; };
    ws.onclose = () => {
      el.ws.className = "wsdot off"; el.wsLabel.textContent = "Bağlantı koptu — yeniden deneniyor";
      setTimeout(connect, 1500);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (e) => { let m; try { m = JSON.parse(e.data); } catch { return; } if (m.type === "step") handleStep(m); };
  }

  /* ---------- Canlı ms sayacı (yalnızca güncel çek) ---------- */
  function startTimer() {
    clientStart = performance.now();
    cancelAnimationFrame(rafId);
    const tick = () => { el.ms.textContent = nf.format(Math.round(performance.now() - clientStart)); rafId = requestAnimationFrame(tick); };
    rafId = requestAnimationFrame(tick);
  }
  function freezeTimer(systemMs) {
    cancelAnimationFrame(rafId);
    el.ms.textContent = nf.format(systemMs);
    const factor = Math.max(1, Math.round(LEGACY_MS / Math.max(1, systemMs)));
    el.factor.textContent = "≈ " + nf.format(factor) + "× daha hızlı";
  }

  /* ---------- Pipeline sıfırlama ---------- */
  function resetPipeline() {
    el.nIntake.className = "stage";
    el.nRisk.className = "stage"; el.mRisk.textContent = "Beklemede";
    el.nDecision.className = "stage decision"; el.mDecision.textContent = "Beklemede";
    resetLane(el.laneAi, el.dAi, el.fAi, el.sAi, "ai");
    resetLane(el.laneCmp, el.dCmp, el.fCmp, el.sCmp, "cmp");
    el.factor.textContent = "—";
  }
  function resetLane(lane, detail, fill, state, kind) {
    lane.className = "lane " + kind; detail.textContent = "Beklemede"; state.textContent = "Beklemede"; fill.style.width = "0%";
  }

  /* ---------- Adım işleyici ---------- */
  function handleStep(m) {
    const isCurrent = m.checkId === currentId;
    switch (m.step) {
      case "intake":
        currentId = m.checkId;
        if (m.check) meta.set(m.checkId, { drawer: m.check.drawer, amount: m.check.amount, findeksScore: m.check.findeksScore });
        resetPipeline(); startTimer();
        el.nIntake.className = "stage done";
        el.mIntake.textContent = m.check ? `${m.check.drawer} · ${tl(m.check.amount)}` : (m.detail || m.checkId);
        el.oursNote.textContent = `${m.checkId} işleniyor…`;
        break;

      case "fanout":
        if (isCurrent) { el.sAi.textContent = "Kuyrukta"; el.sCmp.textContent = "Kuyrukta"; }
        break;

      case "ocr":
        if (isCurrent) updateLane(el.laneAi, el.dAi, el.fAi, el.sAi, "ai", m);
        break;

      case "compliance":
        if (isCurrent) updateLane(el.laneCmp, el.dCmp, el.fCmp, el.sCmp, "cmp", m);
        break;

      case "risk":
        if (isCurrent) { el.nRisk.className = "stage active"; el.mRisk.textContent = m.detail || "Değerlendiriliyor…"; }
        break;

      case "manager":
        onManagerPending(m, isCurrent);
        break;

      case "manager_progress":
        onManagerProgress(m);
        break;

      case "decision":
        onDecision(m, isCurrent);
        break;
    }
  }

  function updateLane(lane, detail, fill, state, kind, m) {
    if (m.status === "active") { lane.className = `lane ${kind} state-active`; fill.style.width = "90%"; state.textContent = "İşleniyor"; }
    else if (m.status === "done") { lane.className = `lane ${kind} state-done`; fill.style.width = "100%"; state.textContent = "Tamamlandı"; }
    else if (m.status === "error") { lane.className = `lane ${kind} state-error`; fill.style.width = "100%"; state.textContent = "Bulgu"; }
    detail.textContent = m.title + (m.detail ? " · " + m.detail : "");
  }

  /* ---------- Onay kuyruğu ---------- */
  function onManagerPending(m, isCurrent) {
    if (meta.has(m.checkId)) Object.assign(meta.get(m.checkId), { riskScore: m.riskScore, riskBand: m.riskBand });
    else meta.set(m.checkId, { drawer: m.drawer, amount: m.amount, findeksScore: m.findeksScore, riskScore: m.riskScore, riskBand: m.riskBand });

    if (isCurrent) {
      el.nRisk.className = "stage done"; el.mRisk.textContent = "Sonuçlar birleştirildi";
      el.nDecision.className = "stage decision warn";
      el.mDecision.textContent = "Onay bekleniyor (kuyrukta)";
      freezeTimer(m.systemMs);
      el.oursNote.textContent = `Sistem ${nf.format(m.systemMs)} ms'de kuyruğa aldı · onay bekleniyor`;
    }
    addQueueCard(m);
  }

  function onManagerProgress(m) {
    const c = cards.get(m.checkId);
    if (!c) return;
    const prog = c.node.querySelector(".qprog");
    if (prog) prog.textContent = `${m.approvals}/${m.approvalsNeeded} onay · son onay: ${m.approver}`;
    c.node.dataset.approvals = m.approvals;
    const btn = c.node.querySelector(".qbtn.approve");
    if (btn) { btn.disabled = false; btn.textContent = "2. Onayı Ver (Genel Müdür)"; }
  }

  function addQueueCard(m) {
    if (el.queueEmpty) { el.queueEmpty.style.display = "none"; }
    const exec = m.tier === "EXECUTIVE";
    const info = meta.get(m.checkId) || {};
    const drawer = m.drawer || info.drawer || "—";
    const amount = m.amount ?? info.amount ?? 0;
    const findeks = m.findeksScore ?? info.findeksScore ?? "—";

    const node = document.createElement("div");
    node.className = "qcard" + (exec ? " exec" : "");
    node.dataset.id = m.checkId;
    node.dataset.approvals = "0";
    node.innerHTML =
      `<div class="qcard-head">
         <span class="qtier">${exec ? "Genel Müdür · Çift Onay" : "Yönetici Onayı"}</span>
         <span class="risk-chip ${riskClass(m.riskBand)}">Risk ${m.riskScore} · ${m.riskBand}</span>
       </div>
       <div class="qmain">
         <span class="qwho">${drawer}</span>
         <span class="qsub"><span class="mono">${m.checkId}</span><span class="qamount mono">${tl(amount)}</span><span class="mono">Findeks ${findeks}</span></span>
       </div>
       <div class="qnote">AI ön-onay: ${m.aiNote || "kontrol tamam"}</div>
       ${exec ? `<div class="qprog">0/${m.approvalsNeeded} onay</div>` : ""}
       <div class="qsla"><div class="qsla-track"><i class="qsla-bar"></i></div><span class="qsla-txt"></span></div>
       <div class="qactions">
         <button class="qbtn approve" type="button">Onayla</button>
         <button class="qbtn reject" type="button">Reddet</button>
       </div>`;

    el.queue.prepend(node);

    const approveBtn = node.querySelector(".qbtn.approve");
    const rejectBtn = node.querySelector(".qbtn.reject");
    approveBtn.addEventListener("click", () => {
      const n = Number(node.dataset.approvals || "0");
      const approver = exec ? (n === 0 ? "Yönetici" : "Genel Müdür") : "Yönetici";
      approveBtn.disabled = true; approveBtn.textContent = "Gönderiliyor…";
      postDecision(m.checkId, "approve", approver);
    });
    rejectBtn.addEventListener("click", () => {
      approveBtn.disabled = true; rejectBtn.disabled = true; rejectBtn.textContent = "Gönderiliyor…";
      postDecision(m.checkId, "reject", exec ? "Genel Müdür" : "Yönetici");
    });

    startSla(node, m.slaMs || 20000);
    cards.set(m.checkId, { node });
    updateQueueCount();
  }

  function startSla(node, slaMs) {
    const bar = node.querySelector(".qsla-bar");
    const txt = node.querySelector(".qsla-txt");
    const started = Date.now();
    const iv = setInterval(() => {
      const left = Math.max(0, slaMs - (Date.now() - started));
      const pct = Math.max(0, (left / slaMs) * 100);
      bar.style.width = pct + "%";
      txt.textContent = "SLA " + Math.ceil(left / 1000) + "s";
      if (left <= 0) { clearInterval(iv); txt.textContent = "SLA — oto-onaylanıyor…"; }
    }, 250);
    node.dataset.slaIv = String(iv);
  }

  function removeQueueCard(checkId) {
    const c = cards.get(checkId);
    if (!c) return;
    const iv = Number(c.node.dataset.slaIv);
    if (iv) clearInterval(iv);
    c.node.remove();
    cards.delete(checkId);
    updateQueueCount();
    if (cards.size === 0 && el.queueEmpty) el.queueEmpty.style.display = "";
  }

  function updateQueueCount() {
    const n = cards.size;
    el.queueCount.textContent = n === 0 ? "0 bekleyen" : `${n} bekleyen`;
  }

  async function postDecision(checkId, action, approver) {
    try {
      await fetch("/api/decision", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ checkId, action, approver }) });
    } catch (_) { /* sunucu olayı yine de kartı kapatır */ }
  }

  /* ---------- Nihai karar ---------- */
  function onDecision(m, isCurrent) {
    if (!m.final) return;

    removeQueueCard(m.checkId);

    if (isCurrent) {
      freezeTimer(m.totalMs);
      el.nRisk.className = "stage done"; el.mRisk.textContent = "Sonuçlar birleştirildi";
      const approved = m.decision === "APPROVE";
      el.nDecision.className = "stage decision " + (approved ? "done" : "error");
      el.mDecision.textContent = m.title;
      const human = m.approvalMs != null ? ` · onay ${fmtDur(m.approvalMs)} (${m.approvedBy})` : "";
      el.oursNote.textContent = approved
        ? `Onaylandı · sistem ${nf.format(m.totalMs)} ms${human}`
        : `Reddedildi${human} · bakiye etkilenmedi`;
    }

    if (m.balanceRemaining != null) el.kBalance.textContent = tl(m.balanceRemaining);
    bumpCount(m.path, m.decision);
    addRow(m);
  }

  function bumpCount(path, decision) {
    if (decision === "REJECT") stats.REJECT++;
    else if (path === "STP") stats.STP++;
    else if (path === "EXECUTIVE") stats.EXECUTIVE++;
    else stats.MANAGER++;
    processed++;

    el.counts.textContent = `STP: ${stats.STP} · Yönetici: ${stats.MANAGER} · G.Müdür: ${stats.EXECUTIVE} · Red: ${stats.REJECT}`;
    el.kProcessed.textContent = nf.format(processed);
    const rate = processed ? Math.round((stats.STP / processed) * 100) : 0;
    el.kStpRate.textContent = `Oto-onay (STP) oranı %${rate}`;
  }

  const PATH_LABEL = {
    STP: "Oto-Onay", MANAGER: "Yönetici Onayı", EXECUTIVE: "Genel Müdür · Çift Onay",
    MANAGER_REJECT: "Red · Yönetici", EXECUTIVE_REJECT: "Red · G. Müdür",
    COMPLIANCE_BLOCK: "Red · Uyum", AI_BLOCK: "Red · İmza",
  };
  const PATH_CLASS = {
    STP: "ok", MANAGER: "info", EXECUTIVE: "exec",
    MANAGER_REJECT: "bad", EXECUTIVE_REJECT: "bad", COMPLIANCE_BLOCK: "bad", AI_BLOCK: "bad",
  };
  const riskClass = (band) => band === "YÜKSEK" ? "risk-high" : band === "ORTA" ? "risk-mid" : "risk-low";
  const fmtDur = (ms) => ms >= 1000 ? (ms / 1000).toFixed(1) + " sn" : ms + " ms";

  function addRow(m) {
    if (el.empty) { el.empty.remove(); el.empty = null; }
    const info = meta.get(m.checkId) || {};
    const drawer = m.drawer || info.drawer || "—";
    const amount = (m.amount != null ? m.amount : info.amount) || 0;
    const findeks = (m.findeksScore != null ? m.findeksScore : info.findeksScore) ?? "—";
    const rScore = (m.riskScore != null ? m.riskScore : info.riskScore);
    const rBand = m.riskBand || info.riskBand || "—";
    const cls = PATH_CLASS[m.path] || "info";
    const label = PATH_LABEL[m.path] || m.path;
    const riskCell = rScore != null ? `<span class="risk-chip ${riskClass(rBand)}">${rScore} · ${rBand}</span>` : "—";

    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td class="mono id">${m.checkId}</td>` +
      `<td class="who">${drawer}</td>` +
      `<td class="num mono">${tl(amount)}</td>` +
      `<td class="num mono">${findeks}</td>` +
      `<td>${riskCell}</td>` +
      `<td><span class="badge ${cls}">${label}</span></td>` +
      `<td class="num mono dur">${nf.format(m.totalMs)} ms</td>`;
    el.log.prepend(tr);

    meta.delete(m.checkId);
    while (el.log.children.length > 14) el.log.removeChild(el.log.lastChild);
  }

  /* ---------- Tetikleyici ---------- */
  async function runSimulation() {
    el.runBtn.disabled = true;
    try {
      const r = await fetch("/api/simulate", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      if (!r.ok) throw new Error("Broker hazır değil");
    } catch (err) {
      el.oursNote.textContent = "Tetikleme hatası: " + err.message;
    } finally {
      setTimeout(() => { el.runBtn.disabled = false; }, 900);
    }
  }

  el.runBtn.addEventListener("click", runSimulation);
  connect();
})();
