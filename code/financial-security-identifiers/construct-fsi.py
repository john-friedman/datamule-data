from datamule import Portfolio
import gzip
import csv
import time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from utils import validate_identifiers, deduplicate_and_merge

# have to day by day. there is one week in 2024 with 15gb of data more than gh runners 14gb of storage.
# Create datasets directory if it doesn't exist
import os
os.makedirs('data/datasets', exist_ok=True)

def extract_fidi(tables):
    fidi = []
    for table in tables:
        if table.name == 'proxyvotingrecord':
            for row in table.data:
                identifiers = ['figi', 'isin', 'cusip']
                
                # Check which identifiers are present in the row
                present_identifiers = {key: row[key] for key in identifiers if key in row and row[key] is not None}
                
                # Only add if at least two identifiers are present
                if len(present_identifiers) >= 2:
                    fidi.append(present_identifiers)
    
    return fidi

def get_day_ranges(start_year=2024):
    """Generate daily date ranges from start_year to present"""
    current_date = date.today()
    start_date = date(start_year, 1, 1)
    
    days = []
    current = start_date
    
    while current <= current_date:
        days.append((current.strftime('%Y-%m-%d'), current.strftime('%Y-%m-%d')))
        current = current + relativedelta(days=1)
        
        if current > current_date:
            break
    
    return days

def download_portfolio_with_retry(start_date, end_date, max_retries=3, wait_seconds=15):
    """Download portfolio with retry logic"""
    for attempt in range(max_retries):
        try:
            portfolio = Portfolio('npx-fidi')
            portfolio.download_submissions(filing_date=(start_date, end_date),
                                           submission_type=['N-PX','N-PX/A'],
                                           document_type=['PROXY VOTING RECORD'],
                                           provider='datamule')
            return portfolio
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {start_date}: {e}")
            if attempt < max_retries - 1:  # Don't wait after the last attempt
                print(f"Waiting {wait_seconds} seconds before retry...")
                time.sleep(wait_seconds)
            else:
                print(f"All {max_retries} attempts failed for {start_date}")
                raise e

all_rows = []
total_fail_count = 0

day_ranges = get_day_ranges(2024)
print(f"Processing {len(day_ranges)} days from 2024 to present")

for start_date, end_date in day_ranges:
    print(f"Processing day: {start_date} to {end_date}")
    
    portfolio = download_portfolio_with_retry(start_date, end_date)

    month_rows = []
    fail_count = 0
    
    for sub in portfolio:
        accession = sub.metadata.content['accession-number']
        try:
            for doc in sub:
                if doc.extension == '.xml':
                    doc_rows = extract_fidi(doc.tables)
                    # Add accession number to each row
                    doc_rows = [dict(row, accession=accession) for row in doc_rows]
                    month_rows.extend(doc_rows)
        except Exception as e:
            print(f"Error processing {accession}: {e}")
            fail_count += 1
    
    print(f"Day {start_date}: {len(month_rows)} rows, {fail_count} failures")
    all_rows.extend(month_rows)
    total_fail_count += fail_count
    
    # Clean up portfolio for next iteration
    portfolio.delete()

print("Validating identifiers...")
validated_rows = validate_identifiers(all_rows)
print(f"Valid rows: {len(validated_rows)} (filtered from {len(all_rows)})")

print("Deduplicating and merging...")
final_rows = deduplicate_and_merge(validated_rows)
print(f"Final unique securities: {len(final_rows)}")

# Write all data to compressed CSV
output_filename = f'data/datasets/financial_security_identifiers_crosswalk.csv.gz'

with gzip.open(output_filename, 'wt', newline='') as csvfile:
    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
    writer.writerow(['cusip', 'isin', 'figi'])
    
    # Write rows, handling missing keys
    for row in final_rows:
        csv_row = [
            row.get('cusip', ''),
            row.get('isin', ''),
            row.get('figi', '')
        ]
        writer.writerow(csv_row)

print(f"Financial security identifiers data written to: {output_filename}")
print(f"Total rows: {len(all_rows)}")
print(f"Total failures: {total_fail_count}")