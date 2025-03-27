import json
import os
from datetime import datetime

from datamule.sec.infrastructure.submissions_metadata import process_submissions_metadata
from mentions import construct_mentions

def save_progress(key, value):
    with open('updates.json') as f:
        data_dict = json.load(f)
    
    data_dict[key]['last_run'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_dict[key]['success'] = value

    with open('updates.json', 'w') as f:
        json.dump(data_dict, f,indent=4)

def load_progress():
    # Default empty progress dictionary
    progress_dict = {}
    
    # Load data_dict to check for keys
    with open('data.json') as f:
        data_dict = json.load(f)
    
    # Check if updates.json exists
    if os.path.exists('updates.json'):
        with open('updates.json', 'r') as f:
            progress_dict = json.load(f)

    
    # Check for missing keys - iterate through all mention types
    for category in data_dict['mentions']:
        mentions_dict = data_dict['mentions'][category]
        
        # Handle nested groups (like tariffs, dei, esg)
        if "query" not in mentions_dict:
            for key in mentions_dict:
                if key not in progress_dict:
                    progress_dict[key] = {
                        'last_run': None,
                        'success': False
                    }
        else:
            # Handle direct mention types
            if category not in progress_dict:
                progress_dict[category] = {
                    'last_run': None,
                    'success': False
                }

        if "submissions_metadata" not in progress_dict:
            progress_dict['submissions_metadata'] = {
                'last_run': None,
                'success': False
            }
    
    # Write back the updated progress dictionary
    with open('updates.json', 'w') as f:
        json.dump(progress_dict, f, indent=4)
    
    return progress_dict

    


def process_mentions(mentions_dict,start_date,key):
    try:
        if start_date is not None:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

        construct_mentions(text_queries=mentions_dict['query'],\
            file_path=f"data/mentions/{'_'.join(mentions_dict['submission_type'])}/{'_'.join(mentions_dict['document_type'])}/{key}.csv",\
            start_date=start_date, submission_type=mentions_dict['submission_type'], document_type=mentions_dict['document_type'])
        
        save_progress(key, True)
    except Exception as e:
        print(e)

def run_updates():

    # Load data_dict
    with open('data.json') as f:
        data_dict = json.load(f)

    # Load updates if exist
    updates = load_progress()

    # process mentions
    for mention in data_dict['mentions']:
        mentions_dict = data_dict['mentions'][mention]
        process_mentions(mentions_dict=mentions_dict, start_date=updates[mention]['last_run'],key=mention)
    
    # Process metadata
    try:
        process_submissions_metadata(output_dir="data/filer_metadata/")
        save_progress('submissions_metadata', True)
    except Exception as e:
        save_progress('submissions_metadata', True)

if __name__ == "__main__":
    run_updates()