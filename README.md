# Food Delivery Lakehouse

An end-to-end data engineering and data science project on Databricks, built on an anonymized delivery dataset from a quick-service restaurant.

**Status:** Phase 1 (project setup and anonymization) is in progress.

## Data

The source export contains 8,926 rows but only 937 distinct delivery records, because records were duplicated with randomized `Created At` values. The Bronze layer keeps those rows as received, and the Silver layer will remove the duplicates.

Before ingestion, `scripts/anonymize.py`:

- replaces order, run, driver, fleet, and store identifiers
- shifts all timestamps by a private number of whole weeks, preserving durations, hours, and weekdays
- removes a vendor-specific column

Only a small anonymized sample is committed, in `data/sample/`. The source file, full anonymized file, and mapping secrets are excluded from Git.

## Roadmap

- [ ] Phase 1: Project setup and anonymization
- [ ] Phase 2: Bronze ingestion into Delta tables
- [ ] Phase 3: Silver cleaning, deduplication, and quality checks
- [ ] Phase 4: Gold star schema and KPI tables
- [ ] Phase 5: Operational analysis
- [ ] Phase 6: Delivery time model with MLflow
- [ ] Phase 7: Databricks Workflow, tests, and CI
- [ ] Phase 8: Dashboard and final documentation

## Run locally

```powershell
pip install -r requirements-dev.txt
pytest
python scripts/anonymize.py --input path/to/source.csv
```
