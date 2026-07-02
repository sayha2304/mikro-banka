/**
 * Kural motoru birim testleri — saf mantık (broker/Redis olmadan).
 * Çalıştır:  node rules.test.js
 */
import assert from "node:assert/strict";
import { decide, computeRisk } from "./rules.js";

const cfg = { stpMax: 100000, stpMinFindeks: 1500, execAmount: 500000, slaMs: 20000 };
let pass = 0;
const t = (name, fn) => { fn(); console.log("✓ " + name); pass++; };

// ---------------- Rotalama ----------------
t("Küçük tutar + yüksek Findeks -> AUTO_APPROVE / STP", () => {
  const r = decide(cfg, { amount: 45000, findeksScore: 1720, blacklisted: false, aiOk: true });
  assert.equal(r.outcome, "AUTO_APPROVE"); assert.equal(r.path, "STP"); assert.equal(r.approvalsNeeded, 0);
});

t("Sınır altı (99.999) + yüksek -> STP", () => {
  const r = decide(cfg, { amount: 99999, findeksScore: 1600, blacklisted: false, aiOk: true });
  assert.equal(r.path, "STP");
});

t("Orta tutar (340k) -> NEEDS_APPROVAL / MANAGER (1 onay)", () => {
  const r = decide(cfg, { amount: 340000, findeksScore: 1560, blacklisted: false, aiOk: true });
  assert.equal(r.outcome, "NEEDS_APPROVAL"); assert.equal(r.path, "MANAGER");
  assert.equal(r.approvalsNeeded, 1); assert.equal(r.reason, "high_amount");
});

t("STP sınırı (tam 100.000) -> MANAGER", () => {
  const r = decide(cfg, { amount: 100000, findeksScore: 1600, blacklisted: false, aiOk: true });
  assert.equal(r.path, "MANAGER"); assert.equal(r.approvalsNeeded, 1);
});

t("Küçük tutar ama düşük Findeks -> MANAGER (low_findeks)", () => {
  const r = decide(cfg, { amount: 60000, findeksScore: 940, blacklisted: false, aiOk: true });
  assert.equal(r.path, "MANAGER"); assert.equal(r.reason, "low_findeks"); assert.equal(r.approvalsNeeded, 1);
});

t("Findeks tam eşikte (1500) STP değil -> MANAGER", () => {
  const r = decide(cfg, { amount: 30000, findeksScore: 1500, blacklisted: false, aiOk: true });
  assert.equal(r.path, "MANAGER");
});

t("Çok yüksek tutar (tam 500k) -> EXECUTIVE (çift onay)", () => {
  const r = decide(cfg, { amount: 500000, findeksScore: 1740, blacklisted: false, aiOk: true });
  assert.equal(r.path, "EXECUTIVE"); assert.equal(r.tier, "EXECUTIVE"); assert.equal(r.approvalsNeeded, 2);
});

t("1.25M -> EXECUTIVE / çift onay", () => {
  const r = decide(cfg, { amount: 1250000, findeksScore: 1740, blacklisted: false, aiOk: true });
  assert.equal(r.path, "EXECUTIVE"); assert.equal(r.approvalsNeeded, 2);
});

t("Kara liste -> AUTO_REJECT / COMPLIANCE_BLOCK", () => {
  const r = decide(cfg, { amount: 60000, findeksScore: 1800, blacklisted: true, aiOk: true });
  assert.equal(r.outcome, "AUTO_REJECT"); assert.equal(r.path, "COMPLIANCE_BLOCK");
});

t("İmza doğrulanamadı -> AUTO_REJECT / AI_BLOCK", () => {
  const r = decide(cfg, { amount: 40000, findeksScore: 1800, blacklisted: false, aiOk: false });
  assert.equal(r.outcome, "AUTO_REJECT"); assert.equal(r.path, "AI_BLOCK");
});

t("Kara liste, imzadan önce gelir", () => {
  const r = decide(cfg, { amount: 40000, findeksScore: 1800, blacklisted: true, aiOk: false });
  assert.equal(r.path, "COMPLIANCE_BLOCK");
});

// ---------------- Risk skoru ----------------
t("Risk skoru: temiz STP -> DÜŞÜK", () => {
  const r = computeRisk(45000, 1720, 500000);
  assert.equal(r.band, "DÜŞÜK"); assert.ok(r.score < 30);
});

t("Risk skoru: orta tutar -> ORTA", () => {
  const r = computeRisk(340000, 1560, 500000);
  assert.equal(r.band, "ORTA");
});

t("Risk skoru: yüksek tutar + düşük Findeks -> YÜKSEK", () => {
  const r = computeRisk(480000, 900, 500000);
  assert.equal(r.band, "YÜKSEK"); assert.ok(r.score >= 60);
});

t("Risk skoru 0-100 aralığında sınırlı", () => {
  const r = computeRisk(9_999_999, 1, 500000);
  assert.ok(r.score <= 100 && r.score >= 0);
});

console.log(`\n${pass}/${pass} test geçti ✅`);
