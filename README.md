## VTC OAK Usage Report Generator

This tool generates a tabular usage report for VTC users by scanning DynamoDB tables and exporting to Excel or Word.

### Prerequisites
- Python 3.9+
- AWS IAM credentials for VTC DynamoDB with read permissions to:
  - `oak-account-vtc`
  - `oak-usage-log-vtc`
  - `oak-ask-ai-vtc`

### Setup
1. Create and populate a `.env` file in this directory:

```
VTC_DYNAMODB_ACCESS_KEY_ID=AKIA...
VTC_DYNAMODB_SECRET_ACCESS_KEY=...
VTC_DYNAMODB_REGION=ap-east-1
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. (Optional) Place `vtc_logo.png` in this directory for DOCX output branding.

### Usage
Run without arguments to generate the last 30 days report as Excel:

```bash
python generate_vtc_report.py
```

Specify date range (inclusive) in `YYYY-MM-DD`:

```bash
python generate_vtc_report.py --start-date 2025-09-01 --end-date 2025-09-30
```

Choose output format and filename:

```bash
python generate_vtc_report.py --format docx --output vtc_report.docx
python generate_vtc_report.py --format excel --output vtc_report.xlsx
```

### Output
- Excel: Saved as `.xlsx` without the index column.
- DOCX: Includes logo (if present), title, subtitle with date range and generation timestamp, and a table of results.

### Notes
- Metrics to `usage_type` mapping is configurable at the top of `generate_vtc_report.py` via `METRICS_USAGE_TYPE_MAP`.
- `createdAt` fields are parsed flexibly (ISO strings or epoch seconds/milliseconds). Entries outside the selected date range are excluded.
- All accounts from `oak-account-vtc` are included; users with no activity show zeros.
