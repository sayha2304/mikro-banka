/**
 * Dinamik rotalama kuralları — SAF fonksiyon (yan etkisiz, test edilebilir).
 * I/O (Redis, RabbitMQ) index.js tarafında; burada yalnızca karar mantığı.
 *
 * Kademeli onay modeli:
 *   • STP        -> anında oto-onay (insan yok)
 *   • MANAGER    -> tek yönetici onayı (insan / SLA)
 *   • EXECUTIVE  -> genel müdür, çift onay (2 farklı onaycı / SLA)
 *
 * @param {{stpMax:number, stpMinFindeks:number, execAmount:number}} cfg
 * @param {{amount:number, findeksScore:number, blacklisted:boolean, aiOk:boolean}} ctx
 * @returns {{outcome:'AUTO_APPROVE'|'AUTO_REJECT'|'NEEDS_APPROVAL',
 *            path:string, tier:string|null, approvalsNeeded:number,
 *            reason:string, label:string, risk:{score:number, band:string}}}
 */
export function computeRisk(amount, findeksScore, execAmount) {
  const cap = execAmount || 500000;
  const amtR = Math.min(amount / cap, 1) * 50;                    // 0..50  (tutar ağırlığı)
  const finR = (Math.max(0, 1600 - findeksScore) / 1600) * 45;   // 0..45  (düşük Findeks riski)
  const score = Math.round(Math.min(100, amtR + finR + 5));
  const band = score >= 60 ? "YÜKSEK" : score >= 30 ? "ORTA" : "DÜŞÜK";
  return { score, band };
}

export function decide(cfg, ctx) {
  const { amount, findeksScore, blacklisted, aiOk } = ctx;
  const risk = computeRisk(amount, findeksScore, cfg.execAmount);

  if (blacklisted) {
    return {
      outcome: "AUTO_REJECT", path: "COMPLIANCE_BLOCK", tier: null, approvalsNeeded: 0,
      reason: "blacklist", label: "Reddedildi — Uyum / Kara Liste Engeli", risk,
    };
  }
  if (aiOk === false) {
    return {
      outcome: "AUTO_REJECT", path: "AI_BLOCK", tier: null, approvalsNeeded: 0,
      reason: "signature", label: "Reddedildi — İmza Doğrulanamadı", risk,
    };
  }

  // KURAL 1 — STP: küçük tutar + yüksek Findeks -> manuel adımları bypass, anında onay
  if (amount < cfg.stpMax && findeksScore > cfg.stpMinFindeks) {
    return {
      outcome: "AUTO_APPROVE", path: "STP", tier: "STP", approvalsNeeded: 0,
      reason: "clean_stp", label: "Dinamik Oto-Onay (STP) — Manuel Adımlar Bypass", risk,
    };
  }

  // KURAL 2 — çok yüksek tutar -> genel müdür, çift onay
  if (amount >= cfg.execAmount) {
    return {
      outcome: "NEEDS_APPROVAL", path: "EXECUTIVE", tier: "EXECUTIVE", approvalsNeeded: 2,
      reason: "very_high_amount", label: "Genel Müdür Onayı — Çift Onay Gerekli", risk,
    };
  }

  // KURAL 3 — orta tutar VEYA düşük Findeks -> tek yönetici onayı
  const reason = amount >= cfg.stpMax ? "high_amount" : "low_findeks";
  const label = reason === "high_amount"
    ? "Yönetici Onayı — Yüksek Tutar"
    : "Yönetici Onayı — Düşük Findeks İncelemesi";
  return {
    outcome: "NEEDS_APPROVAL", path: "MANAGER", tier: "MANAGER", approvalsNeeded: 1,
    reason, label, risk,
  };
}
