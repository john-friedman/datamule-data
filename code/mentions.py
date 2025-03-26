from datamule.sec.submissions.textsearch import query
from datetime import datetime
import pytz
import csv
import os
import gzip

def construct_mentions(text_queries, file_path, start_date=None, submission_type=None, document_type=None):
    """
    Search SEC filings for multiple text queries and write results to a single GZIP-compressed CSV file.
    Preserves existing data in the file and appends new results.
    
    Parameters:
    text_queries (list): List of text queries to search for (e.g. ["inclusion", "inclusive"])
    file_path (str): Path to output CSV file (will be appended with .gz)
    start_date (str, optional): Start date for search in YYYY-MM-DD format. Defaults to "2001-01-01".
    submission_type (list, optional): Types of submissions to search (e.g. ["10-K", "10-Q"])
    document_type (str, optional): Type of document to filter by, matching the 'form' field (e.g. "10-K")
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
        with gzip.open(file_path, 'rt', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header row
            for row in reader:
                existing_data.append(row)
                # Use filing_date, cik, and accession_number as key for deduplication
                existing_keys.add((row[0], row[1], row[2]))
        print(f"Loaded {len(existing_data)} existing records")


    # Process each query and collect results
    new_results = []
    
    for text_query in text_queries:
        results = query(f'{text_query}', filing_date=(start_date, end_date), 
                        requests_per_second=4.0, quiet=True, submission_type=submission_type)
        
        for result in results:

            # Check if document_type filter is applied and matches

            if document_type is not None and result['_source'].get('form') not in document_type:
                continue
                
            filing_date = result['_source']['file_date']
            ciks = result['_source']['ciks']
            id_parts = result['_id'].split(':')
            accession_number = id_parts[0]
            filename = id_parts[1] if len(id_parts) > 1 else ""
            

            for cik in ciks:
                # Create a row with the new order: filing_date, cik, accession_number, filename
                row = [filing_date, cik, accession_number, filename]
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
        writer.writerow(['filing_date', 'cik', 'accession_number', 'filename'])
        writer.writerows(all_results)
    
    print(f"Wrote {len(all_results)} total results ({len(new_results)} new) to {file_path} (GZIP compressed)")