from datamule.sec.submissions.textsearch import query
from datetime import datetime
import pytz
import csv
import os
import gzip

def construct_sec_phrases(text_queries, file_path, start_date=None):
    """
    Search SEC filings for multiple text queries and write results to a single GZIP-compressed CSV file.
    Removes duplicate entries.
    
    Parameters:
    text_queries (list): List of text queries to search for (e.g. ["inclusion", "inclusive"])
    file_path (str): Path to output CSV file (will be appended with .gz)
    start_date (str, optional): Start date for search in YYYY-MM-DD format. Defaults to "2001-01-01".
    """
    if start_date is None:
        start_date = "2001-01-01"
    end_date = datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d")
    
    # Make sure file path ends with .gz
    if not file_path.endswith('.gz'):
        file_path = file_path + '.gz'
    
    # Create directory path if it doesn't exist
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Use a set to track unique rows (excluding the query term)
    unique_rows = set()
    all_results = []
    
    # Process each query and collect results
    for text_query in text_queries:
        print(f"Processing query: '{text_query}'")
        results = query(f'"{text_query}"', filing_date=(start_date, end_date), requests_per_second=5.0)
        
        for result in results:
            filing_date = result['_source']['file_date']
            ciks = result['_source']['ciks']
            accession_number = result['_id'].split(':')[0]
            
            for cik in ciks:
                # Create a row and its key for deduplication
                row = [text_query, filing_date, cik, accession_number]
                # Use only filing_date, cik, and accession_number for uniqueness check
                row_key = (filing_date, cik, accession_number)
                
                if row_key not in unique_rows:
                    unique_rows.add(row_key)
                    all_results.append(row)
                    
        print(f"Completed query: '{text_query}'")
    
    # Write the deduplicated results to GZIP compressed CSV
    with gzip.open(file_path, 'wt', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['query', 'filing_date', 'cik', 'accession_number'])
        writer.writerows(all_results)
    
    print(f"Wrote {len(all_results)} unique results to {file_path} (GZIP compressed)")