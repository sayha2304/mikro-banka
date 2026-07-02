"""
============================================================================
 AI & OCR SERVİSİ  (Python / FastAPI + aio-pika)
 - FANOUT'tan gelen "Check_Submitted" olayını tüketir
 - ~100ms içinde hafif simülasyonla İmza + Tutar doğrulaması üretir
 - Sonucu "results" (topic) exchange'ine 'ai.result' anahtarıyla basar
 - Adım telemetrisini "ui.events" exchange'ine yollar
============================================================================
"""
import asyncio
import json
import os
import random
import time

import aio_pika
from fastapi import FastAPI

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

EX_FANOUT = "check.fanout"     # giriş (fanout)
EX_RESULTS = "results"         # çıkış (topic)
EX_UI = "ui.events"            # ekran/telemetri (fanout)
Q_OCR = "q.ocr"
RK_AI = "ai.result"

app = FastAPI(title="AI & OCR Servisi", version="1.0.0")

STATE = {"processed": 0, "connected": False, "last_latency_ms": 0.0}
_channel = None
_ex_results = None
_ex_ui = None


async def publish_ui(evt: dict):
    if _ex_ui is None:
        return
    await _ex_ui.publish(
        aio_pika.Message(body=json.dumps(evt).encode()),
        routing_key="",
    )


async def handle_check(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        t0 = time.perf_counter()
        check = json.loads(message.body.decode())
        submitted_at = check.get("submittedAt", int(time.time() * 1000))

        # Ekran: AI hattı aktif
        await publish_ui({
            "checkId": check["checkId"],
            "step": "ocr",
            "lane": "ai",
            "status": "active",
            "title": "AI/OCR Analizi",
            "detail": "İmza vektörü ve tutar OCR taranıyor...",
            "submittedAt": submitted_at,
            "elapsedMs": now_ms() - submitted_at,
        })

        # --- Hafif simülasyon: gerçek OCR yerine ~100ms işlem ---
        await asyncio.sleep(random.uniform(0.08, 0.11))

        signature_match = round(random.uniform(97.5, 99.6), 1)
        amount_verified = True
        ok = signature_match >= 90.0

        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        STATE["processed"] += 1
        STATE["last_latency_ms"] = latency_ms

        result = {
            "checkId": check["checkId"],
            "service": "ai-ocr",
            "signatureMatch": signature_match,
            "amountVerified": amount_verified,
            "ok": ok,
            # Karar için gereken orijinal çek alanlarını taşı
            "amount": check.get("amount", 0),
            "findeksScore": check.get("findeksScore", 0),
            "drawer": check.get("drawer", ""),
            "latencyMs": latency_ms,
            "submittedAt": submitted_at,
            "at": now_ms(),
        }

        # Sonucu risk motoruna (topic) yolla
        await _ex_results.publish(
            aio_pika.Message(body=json.dumps(result).encode()),
            routing_key=RK_AI,
        )

        # Ekran: AI hattı tamam
        await publish_ui({
            "checkId": check["checkId"],
            "step": "ocr",
            "lane": "ai",
            "status": "done",
            "title": f"İmza Eşleşti: %{signature_match}",
            "detail": "Tutar Doğrulandı ✓",
            "submittedAt": submitted_at,
            "elapsedMs": now_ms() - submitted_at,
        })


def now_ms() -> int:
    return int(time.time() * 1000)


async def consumer_loop():
    global _channel, _ex_results, _ex_ui
    while True:
        try:
            conn = await aio_pika.connect_robust(RABBITMQ_URL)
            _channel = await conn.channel()
            await _channel.set_qos(prefetch_count=16)

            ex_fanout = await _channel.declare_exchange(EX_FANOUT, aio_pika.ExchangeType.FANOUT, durable=False)
            _ex_results = await _channel.declare_exchange(EX_RESULTS, aio_pika.ExchangeType.TOPIC, durable=False)
            _ex_ui = await _channel.declare_exchange(EX_UI, aio_pika.ExchangeType.FANOUT, durable=False)

            queue = await _channel.declare_queue(Q_OCR, durable=False)
            await queue.bind(ex_fanout, routing_key="")

            STATE["connected"] = True
            print("[ai-ocr] RabbitMQ hazır, çek girişleri dinleniyor.")
            await queue.consume(handle_check)
            await asyncio.Future()  # sonsuza kadar dinle
        except Exception as exc:  # noqa: BLE001
            STATE["connected"] = False
            print(f"[ai-ocr] Broker hatası: {exc} — 2sn sonra tekrar")
            await asyncio.sleep(2)


@app.on_event("startup")
async def _startup():
    asyncio.create_task(consumer_loop())


@app.get("/health")
async def health():
    return {"ok": True, "connected": STATE["connected"]}


@app.get("/metrics")
async def metrics():
    return {
        "processed": STATE["processed"],
        "last_latency_ms": STATE["last_latency_ms"],
        "connected": STATE["connected"],
    }
