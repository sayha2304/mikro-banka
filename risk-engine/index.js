/**
 * ============================================================================
 *  RİSK TABANLI DİNAMİK ROTALAMA MOTORU  (Node.js Consumer)
 *  - "results" topic'inden AI/OCR ve Compliance sonuçlarını PARALEL toplar
 *  - Aynı checkId için iki sonuç birleşince rotalama kararını verir (decide)
 *  - Kademeli onay:
 *      • STP        -> anında oto-onay (insan yok)
 *      • MANAGER    -> tek yönetici onayı (kuyrukta bekler)
 *      • EXECUTIVE  -> genel müdür, çift onay
 *  - İnsan onayı gateway'in POST /api/decision ucundan "manager.decision"
 *    exchange'i ile gelir. Kimse onaylamazsa SLA süresi sonunda oto-onaylanır.
 *  - ÖNEMLİ: "sistem işlem süresi" (otomatik pipeline gecikmesi, ms) ile
 *    "yönetici onay süresi" (insan/SLA) AYRI ölçülür. Hız metriği sistem
 *    süresini kullanır; insan beklemesi bu metriği kirletmez.
 * ============================================================================
 */
import amqp from "amqplib";
import Redis from "ioredis";
import { decide } from "./rules.js";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const EX_RESULTS = "results";
const EX_UI = "ui.events";
const EX_DECISION = "manager.decision";   // gateway -> risk-engine (insan kararları)
const Q_RISK = "q.risk";
const Q_DECISION = "q.manager.decision";

const redis = new Redis(REDIS_URL);

const pending = new Map();  // checkId -> { ai, compliance, timer }  (paralel toplama)
const parked = new Map();   // checkId -> onay bekleyen bağlam
const AGG_TIMEOUT_MS = 5000;

const now = () => Date.now();

// --- Redis konfig tohumlaması (dinamik, canlı değiştirilebilir eşikler) ---
async function seedConfig() {
  const pipe = redis.pipeline();
  pipe.setnx("config:stp:maxAmount", "100000");    // STP üst tutar sınırı
  pipe.setnx("config:stp:minFindeks", "1500");     // STP min Findeks skoru
  pipe.setnx("config:exec:amount", "500000");      // Çift onay (genel müdür) eşiği
  pipe.setnx("config:manager:slaMs", "20000");     // Onay gelmezse oto-onay süresi
  pipe.setnx("balance:pool", "50000000");          // Hızlı bakiye havuzu (TL)
  await pipe.exec();
  console.log("[risk] Redis kuralları ve bakiye havuzu hazır.");
}

async function loadConfig() {
  const [stpMax, stpMinFindeks, execAmount, slaMs] = await redis.mget(
    "config:stp:maxAmount", "config:stp:minFindeks", "config:exec:amount", "config:manager:slaMs"
  );
  return {
    stpMax: Number(stpMax ?? 100000),
    stpMinFindeks: Number(stpMinFindeks ?? 1500),
    execAmount: Number(execAmount ?? 500000),
    slaMs: Number(slaMs ?? 20000),
  };
}

let publishUi = () => {};

async function main() {
  await seedConfig();

  const conn = await amqp.connect(RABBITMQ_URL);
  conn.on("close", () => {
    console.error("[risk] Broker koptu, çıkılıyor (restart devreye girecek).");
    process.exit(1);
  });
  const ch = await conn.createChannel();
  await ch.prefetch(32);

  await ch.assertExchange(EX_RESULTS, "topic", { durable: false });
  await ch.assertExchange(EX_UI, "fanout", { durable: false });
  await ch.assertExchange(EX_DECISION, "fanout", { durable: false });

  publishUi = (evt) => ch.publish(EX_UI, "", Buffer.from(JSON.stringify(evt)));

  // Paralel sonuç kuyruğu (AI + Compliance)
  await ch.assertQueue(Q_RISK, { durable: false });
  await ch.bindQueue(Q_RISK, EX_RESULTS, "ai.result");
  await ch.bindQueue(Q_RISK, EX_RESULTS, "compliance.result");

  // İnsan karar kuyruğu (gateway'den Onayla/Reddet)
  await ch.assertQueue(Q_DECISION, { durable: false, autoDelete: true });
  await ch.bindQueue(Q_DECISION, EX_DECISION, "");

  console.log("[risk] Hazır, paralel sonuçlar ve onay kararları dinleniyor.");

  ch.consume(Q_RISK, (msg) => {
    if (!msg) return;
    const res = JSON.parse(msg.content.toString());
    ch.ack(msg);
    onResult(res);
  });

  ch.consume(Q_DECISION, (msg) => {
    if (!msg) return;
    const dec = JSON.parse(msg.content.toString());
    ch.ack(msg);
    onHumanDecision(dec).catch((e) => console.error("[risk] karar hatası:", e.message));
  });
}

// ---- Paralel sonuçları topla ----
function onResult(res) {
  const id = res.checkId;
  let entry = pending.get(id);
  if (!entry) {
    entry = { ai: null, compliance: null };
    publishUi({
      checkId: id, step: "risk", lane: "core", status: "active",
      title: "Risk Motoru Değerlendiriyor",
      detail: "Paralel hatların sonuçları birleştiriliyor…",
      submittedAt: res.submittedAt, elapsedMs: now() - res.submittedAt,
    });
    entry.timer = setTimeout(() => route(id, true), AGG_TIMEOUT_MS);
    pending.set(id, entry);
  }
  if (res.service === "ai-ocr") entry.ai = res;
  if (res.service === "compliance") entry.compliance = res;

  if (entry.ai && entry.compliance) {
    clearTimeout(entry.timer);
    route(id, false);
  }
}

// ---- Rotalama kararı (otomatik pipeline'ın bittiği an) ----
async function route(id, timedOut) {
  const entry = pending.get(id);
  if (!entry) return;
  pending.delete(id);

  const ai = entry.ai;
  const compliance = entry.compliance;
  const submittedAt = ai?.submittedAt ?? compliance?.submittedAt ?? now();
  const cfg = await loadConfig();

  const amount = ai?.amount ?? compliance?.amount ?? 0;
  const findeksScore = ai?.findeksScore ?? compliance?.findeksScore ?? 0;
  const drawer = ai?.drawer ?? compliance?.drawer ?? "—";
  const signatureMatch = ai?.signatureMatch ?? null;
  const blacklisted = compliance?.blacklisted === true || compliance?.ok === false;
  const aiOk = ai ? ai.ok !== false : true;

  const v = decide(cfg, { amount, findeksScore, blacklisted, aiOk });
  const systemMs = now() - submittedAt;  // <-- OTOMATİK pipeline gecikmesi (hız metriği)

  const base = {
    checkId: id, amount, findeksScore, drawer, signatureMatch,
    riskScore: v.risk.score, riskBand: v.risk.band,
    path: v.path, tier: v.tier, systemMs, submittedAt,
  };

  // --- Otomatik sonuç (insan yok) ---
  if (v.outcome === "AUTO_APPROVE" || v.outcome === "AUTO_REJECT") {
    const approve = v.outcome === "AUTO_APPROVE";
    const balanceRemaining = await settleBalance(approve, amount);
    await redis.incr("stats:processed");
    publishUi({
      ...base, step: "decision", lane: "core",
      status: approve ? "done" : "error",
      title: v.label,
      detail: timedOut ? "(Not: bir hat zaman aşımına uğradı)" : "Paralel pipeline tamamlandı",
      final: true, decision: approve ? "APPROVE" : "REJECT",
      totalMs: systemMs, approvalMs: null, approvedBy: "Otomatik",
      balanceRemaining, elapsedMs: systemMs,
    });
    console.log(`[risk] ${id} -> ${v.outcome} (${v.path}) | sistem ${systemMs}ms`);
    return;
  }

  // --- İnsan onayı gerekiyor: kuyruğa park et ---
  const aiNote = signatureMatch != null ? `İmza %${signatureMatch} · tutar doğrulandı` : "AI ön-kontrol tamam";
  parked.set(id, {
    ...base, reason: v.reason, label: v.label,
    approvalsNeeded: v.approvalsNeeded, approvals: 0, approvers: [],
    parkedAt: now(), slaMs: cfg.slaMs, aiNote,
    slaTimer: setTimeout(() => resolveParked(id, "APPROVE", "SLA (oto-onay)"), cfg.slaMs),
  });

  publishUi({
    ...base, step: "manager", lane: "core", status: "pending",
    title: v.label,
    detail: `Risk skoru ${v.risk.score}/100 · ${v.risk.band}`,
    approvalsNeeded: v.approvalsNeeded, approvals: 0,
    aiNote, slaMs: cfg.slaMs, elapsedMs: systemMs,
  });
  console.log(`[risk] ${id} -> ONAY BEKLİYOR (${v.path}, ${v.approvalsNeeded} onay) | sistem ${systemMs}ms`);
}

// ---- Gateway'den gelen insan kararı ----
async function onHumanDecision({ checkId, action, approver }) {
  const item = parked.get(checkId);
  if (!item) return; // zaten çözülmüş / bilinmeyen
  if (action === "reject") {
    return resolveParked(checkId, "REJECT", approver || "Yönetici");
  }
  // approve
  item.approvals += 1;
  item.approvers.push(approver || `Onaycı ${item.approvals}`);
  if (item.approvals >= item.approvalsNeeded) {
    return resolveParked(checkId, "APPROVE", item.approvers.join(", "));
  }
  // Çift onayda ilk imza — ilerleme bildir
  publishUi({
    checkId, step: "manager_progress", lane: "core", status: "pending",
    approvals: item.approvals, approvalsNeeded: item.approvalsNeeded,
    approver: approver || `Onaycı ${item.approvals}`,
    detail: `${item.approvals}/${item.approvalsNeeded} onay alındı`,
  });
}

// ---- Park edilmiş işlemi sonuçlandır ----
async function resolveParked(id, kind, approvedBy) {
  const item = parked.get(id);
  if (!item) return;
  clearTimeout(item.slaTimer);
  parked.delete(id);

  const approve = kind === "APPROVE";
  const approvalMs = now() - item.parkedAt;
  const balanceRemaining = await settleBalance(approve, item.amount);
  await redis.incr("stats:processed");

  const path = approve ? item.path : (item.path === "EXECUTIVE" ? "EXECUTIVE_REJECT" : "MANAGER_REJECT");
  const title = approve
    ? (item.tier === "EXECUTIVE" ? "Onaylandı — Genel Müdür (Çift Onay)" : "Onaylandı — Yönetici Onayı")
    : "Reddedildi — Onay Merciince";

  publishUi({
    checkId: id, step: "decision", lane: "core",
    status: approve ? "done" : "error",
    title, detail: approve ? `Onay: ${approvedBy}` : `Ret: ${approvedBy}`,
    final: true, decision: approve ? "APPROVE" : "REJECT",
    path, tier: item.tier,
    totalMs: item.systemMs,          // sistem süresi (insan beklemesi hariç)
    approvalMs, approvedBy,          // insan/SLA süresi ayrı
    amount: item.amount, findeksScore: item.findeksScore,
    drawer: item.drawer, signatureMatch: item.signatureMatch,
    riskScore: item.riskScore, riskBand: item.riskBand,
    balanceRemaining, submittedAt: item.submittedAt, elapsedMs: item.systemMs,
  });
  console.log(`[risk] ${id} -> ${kind} (${path}) | onay ${approvalMs}ms · ${approvedBy}`);
}

async function settleBalance(approve, amount) {
  if (approve && amount > 0) return Number(await redis.incrbyfloat("balance:pool", -amount));
  return Number(await redis.get("balance:pool"));
}

main().catch((err) => {
  console.error("[risk] Fatal:", err);
  process.exit(1);
});
