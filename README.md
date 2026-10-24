# Enterprise Data Platform on Databricks (serverless edition)

A small but production-pattern lakehouse: three domains, three environments, every mandatory guardrail.

* **Environments:** DEV / TEST / PROD serverless workspaces; catalogs `hr_*`, `finance_*`, `risk_*` per environment.
* **HR:** CDC from a simulated HCM -> SCD2 history -> Gold star schema; regional PII masking (ABAC).
* **Finance:** market ticks -> exactly-once, de-duplicated Silver -> daily FX close and OHLC.
* **ORM:** GRC + legacy + GL sources -> canonical model (crosswalk) -> regulatory marts; row-level security.
* **Data quality:** Lakeflow expectations + quarantine in Silver; YAML rules run by DQX or Great Expectations before Gold.
* **Compliance & operations:** GDPR erasure, table maintenance, audit, lineage, cost queries.
* **Delivery:** Declarative Automation Bundles + GitHub Actions (OIDC, no secrets), approval-gated PROD.

All data is synthetic. No credentials are stored in this repository.

```
databricks.yml            environments (targets) and variables
resources/                pipelines.yml, jobs.yml
setup/                    00_platform_setup (run once per workspace), 01_verify.sql
src/common/dq/            data-quality framework: rules/*.yml, dqx_engine, gx_engine, runner
src/hr | finance | orm/   generator (simulated sources), pipelines (Bronze->Silver), gold (Gold + gate)
src/ops/                  gdpr_erasure, table_maintenance
tests/                    22 unit tests (pytest, local Spark; both DQ engines)
.github/workflows/        ci.yml (pull requests), deploy.yml (main -> TEST -> approval -> PROD)
```
Run tests locally: `pip install -r requirements-dev.txt && python -m pytest -q tests`
