from datetime import datetime
import json

from datetime import datetime

def extract_earliest_filing_date(data):
    """Extract the earliest filing date from the full JSON data."""
    earliest_date = None
    earliest_date_str = None
    
    # Try to get dates from the filings.files array first
    if 'filings' in data and 'files' in data['filings'] and isinstance(data['filings']['files'], list):
        for file_info in data['filings']['files']:
            if isinstance(file_info, dict) and 'filingFrom' in file_info:
                file_date_str = file_info.get('filingFrom', '')
                
                # Skip if empty
                if not file_date_str:
                    continue
                
                try:
                    # Parse the date string into a datetime object
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    
                    # Check if this date is earlier than our current earliest date
                    if earliest_date is None or file_date < earliest_date:
                        earliest_date = file_date
                        earliest_date_str = file_date_str
                except ValueError:
                    print(f"Warning: Could not parse date '{file_date_str}'")
    
    # If no date found in files array, check filingDate array
    if earliest_date is None and 'filings' in data and 'recent' in data['filings']:
        if 'filingDate' in data['filings']['recent'] and isinstance(data['filings']['recent']['filingDate'], list):
            filing_dates = data['filings']['recent']['filingDate']
            for file_date_str in filing_dates:
                if not file_date_str:
                    continue
                
                try:
                    # Parse the date string into a datetime object
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    
                    # Check if this date is earlier than our current earliest date
                    if earliest_date is None or file_date < earliest_date:
                        earliest_date = file_date
                        earliest_date_str = file_date_str
                except ValueError:
                    print(f"Warning: Could not parse date '{file_date_str}'")
    
    return earliest_date_str

# Sample Microsoft JSON data
with open(r"C:\Users\jgfri\Downloads\CIK0000789019.json", "r") as file:
    microsoft_data = json.load(file)

# Test the function
earliest_date = extract_earliest_filing_date(microsoft_data)
print(f"\nFinal result - Earliest filing date for Microsoft: {earliest_date}")