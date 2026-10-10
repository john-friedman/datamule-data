# datamule data generators

This `scripts` branch contains the code and configuration used to generate the data published on the `master` branch.

Clone only this branch to work on the generators without downloading the data history:

```sh
git clone --branch scripts --single-branch https://github.com/john-friedman/datamule-data.git
```

The scheduled workflows live on `master`. Each workflow checks out this branch in `.scripts/`, runs its generator from the `master` working directory, then commits the updated files to `master`. The daily generator reads `data.json` from this branch and reads or writes `data/` and `updates.json` in the working directory.

## Filing detection statistics

`code/datamule_statistics.py` uses three Datamule Hub queries to publish `fastest_sec_filings_websocket.parquet`, `websocket.parquet`, and `linked_filings.parquet` under `master:data/datamule-statistics/filing-detections-speed/`. The last file contains the selected `submissions_metadata` columns; the generator does not join it to the detection tables. The workflow reads `DATAMULE_API_KEY` from a repository Actions secret.

## Filer Agent

The same generator publishes `data/datamule-statistics/sec-filer-agents/` on `master`:

- `agents.parquet`: `calendar_year`, `agent_cik`, `filing_count`, `agent_name`.
- `entities-YYYY.parquet`: `agent_cik`, `cik`, `filing_count`, `name` for one calendar year.
- `manifest.json`: available years, generation timestamp, and the date through which filings are included.

`FILER_AGENT_SQL` joins `sec_accession_cik_table` to `sec_submission_details_table` on accession. Filing years come from `filingdate`, not the two-digit accession year. Accessions are padded to 18 digits before extracting the first ten digits as the submitting account's CIK. That account can belong to an agent or an entity filing for itself.

Annual agent totals count distinct accessions. Entity counts count distinct accessions per associated CIK, so their sum can exceed the agent total. Duplicate mapping and date rows do not inflate counts. Rows without a matching usable filing date are excluded.

Current names come from `data/filer_metadata/listed_filer_metadata.csv.gz` and `unlisted_filer_metadata.csv.gz`, produced by the daily metadata workflow. Both files must exist; missing individual CIK names remain blank for the page to display a CIK fallback. Names are included in the output so the page loads only the annual summary and selected year's entities. The optional `--filer-metadata-dir` argument overrides the input directory.

The website ranks submitting CIKs separately each year. Top 5 and Top 10 show their combined share versus the remaining CIKs; rank groups use 1–10, 11–100, and 101+. The year slider filters the selected agent's entities, sorted by filing count descending. The current year is labeled year to date.

Run offline checks with `python -m unittest discover -s tests`. Install `duckdb` to include the SQL fixture check; no API credentials or live queries are needed.
