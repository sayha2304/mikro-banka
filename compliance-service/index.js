/**
 * ============================================================================
 *  UYUM (COMPLIANCE) SERVİSİ  (Node.js)
 *  - FANOUT'tan gelen çeki AI/OCR ile AYNI ANDA alır (paralel hat)
 *  - Redis üzerinden kara liste / yaptırım taraması yapar (mikrosaniyeler)
 *  - Sonucu "results" topic exchange'ine 'compliance.result' ile basar
 * ============================================================================
 */
import amqp from "amqplib";
import Redis from "ioredis";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const EX_FANOUT = "check.fanout";
const EX_RESULTS = "results";
const EX_UI = "ui.events";
const Q_COMPLIANCE = "q.compliance";
const RK_COMPLIANCE = "compliance.result";

const BLACKLIST_KEY = "blacklist:drawers";

const redis = new Redis(REDIS_URL);

// Kara liste tohumlaması (idempotent)
async function seedBlacklist() {
  await redis.sadd(
    BLACKLIST_KEY,
    "Şüpheli Ticaret Ltd.",
    "Kara Liste İthalat A.Ş.",
    "Riskli Yatırım Holding"
  );
  console.log("[compliance] Kara liste Redis'e yüklendi.");
}

const now = () => Date.now();

async function main() {
  await seedBlacklist();

  const conn = await amqp.connect(RABBITMQ_URL);
  conn.on("close", () => {
    console.error("[compliance] Broker koptu, çıkılıyor (restart devreye girecek).");
    process.exit(1);
  });
  const ch = await conn.createChannel();
  await ch.prefetch(16);

  await ch.assertExchange(EX_FANOUT, "fanout", { durable: false });
  await ch.assertExchange(EX_RESULTS, "topic", { durable: false });
  await ch.assertExchange(EX_UI, "fanout", { durable: false });

  await ch.assertQueue(Q_COMPLIANCE, { durable: false });
  await ch.bindQueue(Q_COMPLIANCE, EX_FANOUT, "");

  const publishUi = (evt) =>
    ch.publish(EX_UI, "", Buffer.from(JSON.stringify(evt)));

  console.log("[compliance] Hazır, çek girişleri dinleniyor.");

  ch.consume(Q_COMPLIANCE, async (msg) => {
    if (!msg) return;
    const t0 = process.hrtime.bigint();
    const check = JSON.parse(msg.content.toString());
    const submittedAt = check.submittedAt ?? now();

    publishUi({
      checkId: check.checkId,
      step: "compliance",
      lane: "compliance",
      status: "active",
      title: "Uyum / Kara Liste Taraması",
      detail: "Redis üzerinde yaptırım listesi taranıyor...",
      submittedAt,
      elapsedMs: now() - submittedAt,
    });

    // Redis kara liste sorgusu — O(1)
    const blacklisted = (await redis.sismember(BLACKLIST_KEY, check.drawer)) === 1;
    // Uyum tarafı hafif bir ağ/işlem gecikmesi simüle eder
    await new Promise((r) => setTimeout(r, 30 + Math.random() * 30));

    const sanctionsHit = blacklisted;
    const ok = !blacklisted && !sanctionsHit;
    const latencyMs = Number(process.hrtime.bigint() - t0) / 1e6;

    const result = {
      checkId: check.checkId,
      service: "compliance",
      blacklisted,
      sanctionsHit,
      ok,
      // Karar için gereken orijinal çek alanlarını taşı
      amount: check.amount ?? 0,
      findeksScore: check.findeksScore ?? 0,
      drawer: check.drawer ?? "",
      latencyMs: Math.round(latencyMs * 10) / 10,
      submittedAt,
      at: now(),
    };

    ch.publish(EX_RESULTS, RK_COMPLIANCE, Buffer.from(JSON.stringify(result)));

    publishUi({
      checkId: check.checkId,
      step: "compliance",
      lane: "compliance",
      status: ok ? "done" : "error",
      title: ok ? "Uyum Temiz ✓" : "KARA LİSTE EŞLEŞMESİ ✗",
      detail: ok ? "Yaptırım kaydı bulunamadı" : `Keşideci engelli: ${check.drawer}`,
      submittedAt,
      elapsedMs: now() - submittedAt,
    });

    ch.ack(msg);
  });
}

main().catch((err) => {
  console.error("[compliance] Fatal:", err);
  process.exit(1);
});
