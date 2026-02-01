# Alumni Analytics ETL – Runbook

This document describes common failure modes, how to detect them,
and how to recover safely.

---

## 1) Source schema drift

**Symptom**
- ETL fails at transform step
- CI test `test_missing_columns` fails

**Detection**
- Log: "Missing required column"
- CI pipeline red

**Root cause**
- Airtable added/renamed fields

**Resolution**
1. Inspect raw extract
2. Update SAFE_COLUMNS
3. Add/adjust transform logic
4. Re-run ETL

**Prevention**
- Schema gate in pipeline
- Contract tests in CI

---

## 2) Upstream delay / partial data

**Symptom**
- Row counts drop sharply vs previous run

**Detection**
- STATUS.md shows row count anomaly
- Query: stats tables significantly smaller

**Resolution**
1. Re-run ETL after upstream is complete
2. Validate row counts

**Prevention**
- Row-count sanity checks
- Run window scheduled after source update

---

## 3) Duplicate or late-arriving records

**Symptom**
- Counts spike unexpectedly
- Same keys appear multiple times

**Detection**
- Aggregate row count increases abnormally

**Resolution**
1. Re-run ETL (WRITE_TRUNCATE ensures idempotency)
2. Verify unique keys

**Prevention**
- Deterministic rebuild
- Partition overwrite strategy

---

## 4) BigQuery permission / quota errors

**Symptom**
- Load job fails

**Detection**
- Error: 403, quota exceeded

**Resolution**
1. Check service account permissions
2. Verify dataset exists
3. Retry load

**Prevention**
- IAM review
- Quota monitoring

---

## 5) CI test failure

**Symptom**
- GitHub Actions red

**Detection**
- Failing test logs

**Resolution**
1. Fix transform logic
2. Re-run tests locally
3. Push fix

**Prevention**
- Required checks before merge