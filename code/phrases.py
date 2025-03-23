from datamule.sec.submissions.textsearch import query
from datetime import datetime
import pytz
import csv
import os
import gzip

def construct_sec_phrases(text_queries, file_path, start_date=None, submission_type=None):
    """
    Search SEC filings for multiple text queries and write results to a single GZIP-compressed CSV file.
    Preserves existing data in the file and appends new results.
    
    Parameters:
    text_queries (list): List of text queries to search for (e.g. ["inclusion", "inclusive"])
    file_path (str): Path to output CSV file (will be appended with .gz)
    start_date (str, optional): Start date for search in YYYY-MM-DD format. Defaults to "2001-01-01".
    submission_type (list, optional): Types of submissions to search (e.g. ["10-K", "10-Q"])
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
    
    # Load existing data if file exists
    existing_data = []
    existing_keys = set()
    
    if os.path.exists(file_path):
        print(f"Loading existing data from {file_path}")
        try:
            with gzip.open(file_path, 'rt', newline='') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)  # Skip header row
                for row in reader:
                    existing_data.append(row)
                    # Use filing_date, cik, and accession_number as key for deduplication
                    existing_keys.add((row[1], row[2], row[3]))
            print(f"Loaded {len(existing_data)} existing records")
        except Exception as e:
            print(f"Error loading existing data: {e}")
            # Continue with empty existing data if file can't be read
            pass
    
    # Process each query and collect results
    new_results = []
    
    for text_query in text_queries:
        print(f"Processing query: '{text_query}'")
        results = query(f'"{text_query}"', filing_date=(start_date, end_date), 
                        requests_per_second=5.0, quiet=True, submission_type=submission_type)
        
        for result in results:
            filing_date = result['_source']['file_date']
            ciks = result['_source']['ciks']
            accession_number = result['_id'].split(':')[0]
            
            for cik in ciks:
                # Create a row and its key for deduplication
                row = [text_query, filing_date, cik, accession_number]
                # Use filing_date, cik, and accession_number for uniqueness check
                row_key = (filing_date, cik, accession_number)
                
                if row_key not in existing_keys:
                    new_results.append(row)
                    existing_keys.add(row_key)
                    
        print(f"Completed query: '{text_query}'")
    
    # Combine existing and new data
    all_results = existing_data + new_results
    
    # Write the combined results to GZIP compressed CSV
    with gzip.open(file_path, 'wt', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['query', 'filing_date', 'cik', 'accession_number'])
        writer.writerows(all_results)
    
    print(f"Wrote {len(all_results)} total results ({len(new_results)} new) to {file_path} (GZIP compressed)")