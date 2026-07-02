# Mikro-Banka Çek Tahsilat Sistemi (PoC)

<!-- mb-cila -->
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-FF6600?logo=rabbitmq&logoColor=white) ![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white) ![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white) ![Node.js](https://img.shields.io/badge/Node.js-5FA04E?logo=nodedotjs&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white) ![License](https://img.shields.io/badge/license-All%20Rights%20Reserved-red)

> Bankalardaki **saatler/günler** süren manuel çek onayını, **event-driven + paralel pipeline + risk tabanlı dinamik rotalama** ile **1 saniyenin altına** indiren kanıt-of-konsept mimarisi. Ugreen NAS (Docker) üzerinde tek komutla ayağa kalkar.

### ✨ Öne Çıkanlar

- ⚡ **Gerçek paralellik** — RabbitMQ fanout ile AI/OCR imza doğrulama ve uyum/kara-liste taraması *aynı milisaniyede* çalışır.
- 🎯 **Risk tabanlı kademeli onay** — STP oto-onay · tek yönetici onayı · genel müdür çift onayı; her çeke 0–100 risk skoru.
- 🙋 **İnsan-döngüde onay** — canlı onay kuyruğu (Onayla / Reddet) + SLA geri sayımı; kimse dokunmazsa otomatik onay.
- 📊 **Dürüst telemetri** — otomatik *sistem işlem süresi* (saniye-altı) ile *insan onay süresi* ayrı raporlanır.
- 🔧 **Yeniden-derlemesiz kurallar** — risk eşikleri Redis'te; kod derlemeden canlı değiştirilebilir.
- 🐳 **Tek komut kurulum** — `docker compose up -d --build` ile 7 servis (ARM/x86 NAS uyumlu).

---

## Neden hızlı? (Mimarinin özü)

Geleneksel banka çeki **sıralı (sequential)** doğrular: şube tarar → genel müdürlüğe gider → uyum ekibi bakar → risk ekibi onaylar. Zincir kopunca günler kaybolur.

Bu PoC ise süreci **RabbitMQ Fanout Exchange** ile **paralel hatlara** böler. Çek girer girmez AI/OCR imza kontrolü yaparken, **aynı milisaniyede** uyum servisi Redis üzerinden kara liste taraması yapar. Risk skoru güvenliyse bürokrasi adımları **bypass** edilir (STP — Straight-Through Processing).

```
                                  ┌──────────── q.ocr ──────────►  AI/OCR (FastAPI)  ──┐
   Frontend ──POST /simulate─► Gateway ─► │ FANOUT (aynı anda / paralel)                ├─► results (topic)
      ▲   │                    (Node)     └──────── q.compliance ──────►  Uyum (Node+Redis) ──┘   │
      │   │                                                                                        ▼
      │   │  POST /decision (Onayla/Reddet)                                          Risk Motoru (Node+Redis)
      │   └──────────────► manager.decision ─────────────────────────────────────►  • AI + Uyum sonucunu birleştirir
      │                    (fanout)                                                  • Kademeli rotalama + risk skoru
      │                                                                              • STP anında; Yönetici/G.Müdür
      └──────────── WebSocket ◄──── ui.events (fanout) ◄──────────────────────────    onay kuyruğunda bekler (SLA'lı)
                    (canlı telemetri)                                                • sistem süresini (ms) fırlatır
```

---

## Bileşenler

| Servis | Teknoloji | Görevi |
|---|---|---|
| `rabbitmq` | RabbitMQ 3.13 | Fanout + Topic exchange (mesaj brokeri) |
| `redis` | Redis 7.2 | Hızlı bakiye havuzu + kara liste + canlı kural eşikleri |
| `gateway` | Node.js | WebSocket köprüsü, simülasyon tetikleyici, fanout publisher |
| `ai-ocr-service` | Python / FastAPI | İmza + tutar doğrulama (~100ms simülasyon) |
| `compliance-service` | Node.js | Redis üzerinden kara liste / yaptırım taraması |
| `risk-engine` | Node.js | Paralel sonuç birleştirme + dinamik rotalama (STP) |
| `frontend` | nginx + vanilla JS | Görsel operasyon izleme terminali |

---

## Kademeli dinamik rotalama + risk skoru

Eşikler **Redis'te** tutulur (`config:*`) — kodu yeniden derlemeden canlı değiştirilebilir. Her çek için 0–100 arası bir **risk skoru** (tutar + Findeks) hesaplanır ve karar buna göre kademelenir:

| Koşul | Yol | Onay | Sonuç |
|---|---|---|---|
| Tutar < 100.000 ve Findeks > 1500 | **STP** | insan yok | Anında **oto-onay** (bürokrasi bypass) |
| 100.000 ≤ Tutar < 500.000 **veya** düşük Findeks | **MANAGER** | 1 yönetici | Onay kuyruğunda bekler |
| Tutar ≥ 500.000 | **EXECUTIVE** | 2 onaycı (genel müdür) | Çift onay kuyruğu |
| Kara liste eşleşmesi | — | — | Anında **RED** (uyum) |
| İmza doğrulanamadı | — | — | Anında **RED** (AI) |

**İnsan onayı (human-in-the-loop):** MANAGER/EXECUTIVE yolundaki çekler dashboard'daki **Yönetici Onay Kuyruğu**'na düşer; operatör **Onayla / Reddet** eder. Kimse dokunmazsa **SLA süresi** (varsayılan 20 sn) sonunda otomatik onaylanır — akış hiç kilitlenmez.

**İki ayrı gecikme metriği (önemli):** Hız iddiası, **otomatik pipeline gecikmesini** ölçen *sistem işlem süresi* (saniye altı) ile gösterilir; insanın onaya harcadığı *yönetici onay süresi* ayrı raporlanır ve hız metriğini kirletmez. Bu, "makine gecikmesi" ile "insan gecikmesi"ni bilinçli olarak ayırır.

Kural mantığı `risk-engine/rules.js` içinde saf fonksiyon olarak izole edilmiştir ve testlidir:

```bash
cd risk-engine && node rules.test.js   # 15/15 test
```

### İlgili Redis anahtarları

| Anahtar | Varsayılan | Anlamı |
|---|---|---|
| `config:stp:maxAmount` | `100000` | STP üst tutar sınırı |
| `config:stp:minFindeks` | `1500` | STP min Findeks |
| `config:exec:amount` | `500000` | Çift onay (genel müdür) eşiği |
| `config:manager:slaMs` | `20000` | Onay gelmezse oto-onay süresi (ms) |
| `balance:pool` | `50000000` | Hızlı bakiye havuzu (TL) |

---

## Kurulum — Ugreen NAS

### Yöntem A · SSH (önerilen)

1. NAS'ta SSH'ı açın (UGOS → Terminal/SSH ayarları).
2. Proje klasörünü NAS'a kopyalayın (ör. `/volume1/docker/mikro-banka`):
   ```bash
   scp -r mikro-banka  <kullanıcı>@<NAS_IP>:/volume1/docker/
   ```
3. NAS'a bağlanıp klasöre girin ve ayağa kaldırın:
   ```bash
   ssh <kullanıcı>@<NAS_IP>
   cd /volume1/docker/mikro-banka
   docker compose up -d --build
   ```
4. Tarayıcıdan açın: **`http://<NAS_IP>:8080`**

### Yöntem B · UGOS Docker arayüzü (Container Station benzeri)

1. Klasörü NAS'ın Docker paylaşımına yükleyin.
2. Docker uygulamasında **"Compose / Proje Oluştur"** → `docker-compose.yml` dosyasını seçin.
3. **Build & Start** deyin, `http://<NAS_IP>:8080` adresine gidin.

### Portlar (`.env` üzerinden değiştirilebilir)

| Servis | Varsayılan | Açıklama |
|---|---|---|
| Dashboard | `8080` | Ana ekran |
| Gateway (WS/API) | `3000` | nginx proxy'ler, doğrudan gerekmez |
| AI health/metrics | `8000` | `http://NAS_IP:8000/metrics` |
| RabbitMQ paneli | `15672` | `mikrobank / mikrobank123` |

---

## Kullanım

1. `http://<NAS_IP>:8080` → sağ üstte **"Canlı bağlantı"** yeşil ışığı görün.
2. **"Yeni Çek Simüle Et"** butonuna basın.
3. İzleyin: iki paralel hat (AI/OCR + Uyum) **aynı anda** yeşile döner, risk motoru karar verir, üstteki sayaç **"24:00:00 vs X ms"** karşılaştırmasını gösterir.
4. **STP** çekleri anında tabloya düşer. **Yönetici / Genel Müdür** gerektiren çekler **Onay Kuyruğu**'na girer — oradan **Onayla / Reddet** edin ya da SLA geri sayımının otomatik onaylamasını izleyin.
5. Her tıklama rastgele bir senaryo üretir (STP, tek onay, çift onay veya kara liste reddi).

## API uçları (gateway)

| Yöntem | Uç | Gövde | İşlev |
|---|---|---|---|
| `POST` | `/api/simulate` | `{}` (opsiyonel `drawer/amount/findeksScore`) | Yeni çek üretir, fanout'a basar |
| `POST` | `/api/decision` | `{ checkId, action:"approve"\|"reject", approver }` | Onay kuyruğundaki çeki sonuçlandırır |
| `GET` | `/health` | — | Broker + istemci durumu |
| `GET` | `/metrics` (ai-ocr) | — | AI servisi sayaçları |

---

## Canlı kural değiştirme (demo için güçlü an)

STP tutar sınırını yeniden derlemeden düşürün:

```bash
docker exec -it mb-redis redis-cli SET config:stp:maxAmount 50000
```

Artık 50.000 TL üstü çekler otomatik olarak "yönetici onayı" hattına düşer. Rotalamanın gerçekten **veri-güdümlü** olduğunun kanıtı.

---

## Sorun giderme

- **Dashboard "Koptu" diyor:** Gateway RabbitMQ'ya bağlanana kadar 5–10 sn bekleyin (`depends_on: healthy`).
- **Build ARM/x86:** Kullanılan imajlar (rabbitmq, redis, node:20-alpine, python:3.11-slim, nginx) çoklu-mimaridir; hem Intel hem ARM Ugreen modellerinde çalışır.
- **Logları görmek:** `docker compose logs -f risk-engine`
- **Sıfırdan:** `docker compose down -v && docker compose up -d --build`

---

## GitHub'da yayınlama

`.env` (kimlik bilgileri) `.gitignore` ile hariç tutulur; paylaşılan `.env.example` çalıştırma için şablondur.

```bash
cd mikro-banka
git init
git add .
git commit -m "Mikro-Banka: event-driven paralel çek tahsilat PoC"
git branch -M main
git remote add origin git@github.com:<kullanıcı>/mikro-banka.git   # veya https://...
git push -u origin main
```

Klonlayan biri çalıştırmak için: `cp .env.example .env` → `docker compose up -d --build`.

---

## Klasör yapısı

```
mikro-banka/
├── docker-compose.yml
├── .env / .env.example / .gitignore
├── README.md
├── gateway/            # Node — WS + fanout publisher + /api/decision
├── ai-ocr-service/     # Python FastAPI — imza/tutar
├── compliance-service/ # Node — kara liste (Redis)
├── risk-engine/        # Node — kademeli rotalama + risk skoru + onay kuyruğu (+ test)
└── frontend/           # nginx + vanilla JS dashboard (onay kuyruğu + risk sütunu)
```

---

## 📄 Lisans / Kullanım

**© 2026 sayha2304 — Tüm hakları saklıdır.**
Bu depo yalnızca **görüntüleme ve değerlendirme** amacıyla herkese açıktır.
Kodun tamamının veya bir kısmının izinsiz **kopyalanması, değiştirilmesi,
dağıtılması veya herhangi bir projede kullanılması yasaktır.** Ayrıntı için `LICENSE`.
