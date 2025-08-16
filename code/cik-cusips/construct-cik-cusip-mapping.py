from datamule import Portfolio
from utils import find_cusips_html, find_cusips_xml
import csv
from datetime import datetime
import gzip
import os

# Create datasets directory if it doesn't exist
os.makedirs('data/datasets', exist_ok=True)

portfolio = Portfolio('schedules-for-cusips')

rows = []
fail_count = 0

current_year = datetime.now().year
for year in range(1995, current_year + 1):
    print(f"Processing Year: {year}")
    portfolio.download_submissions(submission_type=['SC 13D','SC 13D/A',
                                                    'SC 13G','SC 13G/A',
                                                    'SCHEDULE 13D','SCHEDULE 13D/A',
                                                    'SCHEDULE 13G','SCHEDULE 13G/A'],
                                                    filing_date=(f'{year}-01-01',f'{year}-12-31'),
                                    document_type=['SC 13D','SC 13D/A',
                                                    'SC 13G','SC 13G/A',
                                                    'SCHEDULE 13D','SCHEDULE 13D/A',
                                                    'SCHEDULE 13G','SCHEDULE 13G/A'])

    for sub in portfolio:
        try:
            accession = sub.metadata.content['accession-number']
            filing_date = sub.metadata.content['filing-date']

            # validation check if any item is a list - issue w/metadata in malformed sgml
            if isinstance(accession,list):
                accession = accession[0]

            if isinstance(filing_date,list):
                filing_date = filing_date[0]

            subject_companies = sub.metadata.content['subject-company']
            
            # handles when company names change
            if not isinstance(subject_companies, list):
                subject_companies = [subject_companies]
            
            issuer_cik = subject_companies[0]['company-data']['cik']

            sub_cusips = []
            for doc in sub:
                if doc.extension == '.xml':
                    sub_cusips.extend(find_cusips_xml(doc.content.decode()))
                elif doc.extension in ['.htm','.html','.txt']:
                    sub_cusips.extend(find_cusips_html(doc.text))

            unique_cusips = list(set([item['cusip'] for item in sub_cusips]))
            for cusip in unique_cusips:
                rows.append((accession,filing_date,issuer_cik,cusip.upper()))

        except Exception as e:
            fail_count+=1
            print(f"Fail count {fail_count}: {e}")
	
    portfolio.delete()


output_filename = f'data/datasets/cik_cusip_crosswalk.csv.gz'

with gzip.open(output_filename, 'wt', newline='') as csvfile:
    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
    writer.writerow(['accession_number', 'filing_date', 'issuer_cik', 'cusip'])
    writer.writerows(rows)

print(f"CIK-CUSIP mapping data written to: {output_filename}")
print(f"Total rows: {len(rows)}")
print(f"Total failures: {fail_count}")