import json
import os
from datetime import datetime

from datamule.sec.infrastructure.submissions_metadata import process_submissions_metadata
from phrases import construct_sec_phrases

def load_updates(update_file):
    if os.path.exists(update_file):
        with open(update_file, 'r') as f:
            return json.load(f)
    else:
        return {
            "submissions_metadata": None,
            "phrases": {
                "inclusive": None,
                "intersectional": None,
                "diversity": None,
                "equity": None,
                "carbon_emissions": None,
                "climate_risk": None
            }
        }

def save_updates(data, update_file):
    with open(update_file, 'w') as f:
        json.dump(data, f, indent=2)

def update_data(data, key, subkey=None):
    today = datetime.now().strftime("%Y-%m-%d")
    if subkey:
        data[key][subkey] = today
    else:
        data[key] = today
    return data

def run_updates(update_file="../update.json"):
    updates = load_updates(update_file)
    
    # Process metadata
    try:
        process_submissions_metadata(output_dir="../data/filer_metadata/")
        updates = update_data(updates, "submissions_metadata")
        save_updates(updates, update_file)
    except Exception as e:
        print(f"Metadata error: {e}")
    
    # Process phrases
    phrases = {
        "inclusive": ["inclusion", "inclusive"],
        "intersectional": ["intersectional"],
        "diversity": ["diversity"],
        "equity": ["equity"],
        "carbon_emissions": ["carbon emissions"],
        "climate_risk": ["climate risk"]
    }
    
    for key, query in phrases.items():
        try:
            start = updates["phrases"][key]
            construct_sec_phrases(
                start_date=start,
                text_queries=query,
                file_path=f"../data/phrases/{key}.csv"
            )
            updates = update_data(updates, "phrases", key)
            save_updates(updates, update_file)
        except Exception as e:
            print(f"{key} error: {e}")

run_updates()