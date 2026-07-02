/**
 * ============================================================================
 *  GATEWAY  (Node.js)
 *  - Frontend'e WebSocket köprüsü
 *  - "Simülasyonu Başlat" tetikleyicisi (HTTP)
 *  - "Check_Submitted" olayını RabbitMQ FANOUT exchange'e basar
 *    (AI/OCR ve Compliance servislerine AYNI ANDA / paralel gider)
 *  - "ui.events" exchange'ini dinler ve tüm adımları WS ile ekrana yansıtır
 * ============================================================================
 */
import express from "express";
import { WebSocketServer } from "ws";
import http from "http";
import amqp from "amqplib";
import { randomUUID } from "crypto";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";
const PORT = parseInt(process.env.PORT || "3000", 10);

// --- RabbitMQ topolojisi (tüm servislerde ortak isimlendirme) ---
const EX_FANOUT = "check.fanout";   // Çek girişi -> paralel dağıtım
const EX_UI = "ui.events";          // Ekran/telemetri olayları (fanout)
const Q_UI = "q.ui.gateway";
const EX_DECISION = "manager.decision"; // Onayla/Reddet -> risk-engine (fanout)

// --- Keşideci havuzu: farklı senaryoları tetiklemek için ---
const SCENARIOS = [
  // STP (Straight-Through Processing) — küçük tutar + yüksek Findeks -> anında oto onay
  { drawer: "Anadolu Tekstil A.Ş.",   amount: 45_000,  findeksScore: 1720 },
  { drawer: "Ege Lojistik Ltd.",      amount: 88_500,  findeksScore: 1610 },
  { drawer: "Marmara Gıda San.",      amount: 32_000,  findeksScore: 1890 },
  // Yüksek tutar -> AI ön-onay + yönetici onayı simülasyonu
  { drawer: "Karadeniz İnşaat A.Ş.",  amount: 340_000, findeksScore: 1560 },
  { drawer: "Boğaziçi Holding",       amount: 1_250_000, findeksScore: 1740 },
  // Riskli — düşük Findeks veya kara liste ismi
  { drawer: "Şüpheli Ticaret Ltd.",   amount: 60_000,  findeksScore: 940  },
];

const app = express();
app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

let channel = null;
const clients = new Set();

// ---- WebSocket bağlantı yönetimi ----
wss.on("connection", (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: "hello", msg: "Gateway'e bağlanıldı. Sistem hazır." }));
  ws.on("close", () => clients.delete(ws));
  ws.on("error", () => clients.delete(ws));
});

function broadcast(obj) {
  const data = JSON.stringify(obj);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(data);
  }
}

// ---- RabbitMQ bağlantısı (yeniden bağlanma denemeli) ----
async function connectRabbit() {
  while (true) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      conn.on("close", () => {
        console.error("[gateway] RabbitMQ bağlantısı kapandı, yeniden denenecek...");
        channel = null;
        setTimeout(connectRabbit, 2000);
      });
      const ch = await conn.createChannel();

      await ch.assertExchange(EX_FANOUT, "fanout", { durable: false });
      await ch.assertExchange(EX_UI, "fanout", { durable: false });
      await ch.assertExchange(EX_DECISION, "fanout", { durable: false });

      // UI olaylarını dinle ve WS'e köprüle
      const { queue } = await ch.assertQueue(Q_UI, { durable: false, autoDelete: true });
      await ch.bindQueue(queue, EX_UI, "");
      await ch.consume(queue, (msg) => {
        if (!msg) return;
        try {
          const evt = JSON.parse(msg.content.toString());
          broadcast({ type: "step", ...evt });
        } catch (_) {}
        ch.ack(msg);
      });

      channel = ch;
      console.log("[gateway] RabbitMQ hazır. Fanout + UI köprüsü aktif.");
      return;
    } catch (err) {
      console.error("[gateway] RabbitMQ bağlanılamadı:", err.message, "- 2sn sonra tekrar");
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

// ---- Simülasyon tetikleyici ----
app.post("/api/simulate", (req, res) => {
  if (!channel) return res.status(503).json({ error: "Broker henüz hazır değil" });

  // İstek gövdesinden gelmezse rastgele senaryo seç
  const pick = SCENARIOS[Math.floor(Math.random() * SCENARIOS.length)];
  const check = {
    checkId: "CHK-" + randomUUID().slice(0, 8).toUpperCase(),
    drawer: req.body?.drawer ?? pick.drawer,
    amount: req.body?.amount ?? pick.amount,
    findeksScore: req.body?.findeksScore ?? pick.findeksScore,
    submittedAt: Date.now(),        // <-- zaman damgası (paralel süreç ölçümü buradan başlar)
    traceId: randomUUID(),
  };

  // 1) Ekrana "çek alındı" olayı
  publishUi({
    checkId: check.checkId,
    step: "intake",
    lane: "core",
    status: "done",
    title: "Çek Sisteme Düştü",
    detail: `${check.drawer} • ${fmtTL(check.amount)} • Findeks ${check.findeksScore}`,
    submittedAt: check.submittedAt,
    check,
  });

  // 2) FANOUT ile paralel dağıtım (AI/OCR + Compliance aynı anda)
  channel.publish(EX_FANOUT, "", Buffer.from(JSON.stringify(check)), {
    contentType: "application/json",
    timestamp: check.submittedAt,
  });

  publishUi({
    checkId: check.checkId,
    step: "fanout",
    lane: "core",
    status: "active",
    title: "Paralel Dağıtım (Fanout)",
    detail: "AI/OCR ve Uyum servisi AYNI ANDA tetiklendi",
    submittedAt: check.submittedAt,
  });

  res.json({ ok: true, check });
});

// ---- Yönetici / Genel Müdür kararı (Onayla / Reddet) ----
app.post("/api/decision", (req, res) => {
  if (!channel) return res.status(503).json({ error: "Broker henüz hazır değil" });
  const { checkId, action, approver } = req.body || {};
  if (!checkId || (action !== "approve" && action !== "reject")) {
    return res.status(400).json({ error: "checkId ve action (approve|reject) gerekli" });
  }
  channel.publish(EX_DECISION, "", Buffer.from(JSON.stringify({ checkId, action, approver })));
  res.json({ ok: true });
});

function publishUi(evt) {
  if (!channel) return;
  channel.publish(EX_UI, "", Buffer.from(JSON.stringify(evt)));
}

function fmtTL(n) {
  return new Intl.NumberFormat("tr-TR", { style: "currency", currency: "TRY", maximumFractionDigits: 0 }).format(n);
}

app.get("/health", (_req, res) => res.json({ ok: true, broker: !!channel, clients: clients.size }));

server.listen(PORT, () => console.log(`[gateway] HTTP+WS ${PORT} portunda dinliyor`));
connectRabbit();
