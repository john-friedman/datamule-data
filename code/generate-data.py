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
        # Initialize with a flattened version of our ESG/DEI keywords
        return {
            "submissions_metadata": None,
            "phrases": {
                # Diversity terms
                "workforce_diversity": None,
                "gender_diversity": None,
                "racial_diversity": None,
                "supplier_diversity": None,
                "diversity_targets": None,
                
                # Equity terms
                "pay_equity": None,
                "opportunity_equity": None,
                
                # Inclusion terms
                "workplace_inclusion": None,
                "belonging": None,
                "accessibility": None,
                
                # Environmental terms
                "emissions": None,
                "climate_action": None,
                "energy": None,
                "resource_management": None,
                "climate_targets": None,
                
                # Social terms
                "community_impact": None,
                "human_rights": None,
                "health_safety": None,
                
                # Governance terms
                "board_diversity": None,
                "ethics": None,
                "risk_management": None,
                "disclosure": None,
                "governance_metrics": None,

                # 8-K phrases
                "layoff": None
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

def run_updates(update_file="update.json"):
    updates = load_updates(update_file)

    phrases_8k = {
        "layoff": ['layoff* OR "workforce reduction" OR "headcount reduction" OR "reduction in force" OR "staff reduction'],
    }

    for key, query in phrases_8k.items():
        try:
            start = updates["phrases"][key]
            construct_sec_phrases(
                start_date=start,
                text_queries=query,
                file_path=f"data/phrases/8k/{key}.csv",  # Changed from ../data/phrases/8k/
                submission_type=["8-K"]
            )
            updates = update_data(updates, "phrases", key)
            save_updates(updates, update_file)
        except Exception as e:
            print(f"{key} error: {e}")

    
    # Process phrases with the new ESG/DEI keywords structure
    dei_esg_phrases = {
        # Diversity terms
        "workforce_diversity": ['"workforce diversity"', '"diverse workforce"', '"diversity in workplace"'],
        "gender_diversity": ['"gender diversity"', '"gender representation"', '"gender balance"'],
        "racial_diversity": ['"racial diversity"', '"ethnic diversity"', '"racial representation"'],
        "supplier_diversity": ['"supplier diversity"', '"diverse suppliers"', '"diversity in supply chain"'],
        "diversity_targets": ['"diversity goals"', '"representation targets"', '"diversity metrics"'],
        
        # Equity terms
        "pay_equity": ['"pay equity"', '"wage equity"', '"compensation equity"', '"equal pay"'],
        "opportunity_equity": ['"equal opportunity"', '"opportunity equity"', '"equitable advancement"'],
        
        # Inclusion terms
        "workplace_inclusion": ['"workplace inclusion"', '"inclusive workplace"', '"inclusive environment"'],
        "belonging": ['"belonging"', '"employee belonging"', '"sense of belonging"'],
        "accessibility": ['"accessibility initiatives"', '"accessible workplace"', '"disability inclusion"'],
        
        # Environmental terms
        "emissions": ['"GHG emissions"', '"greenhouse gas emissions"', '"emissions reduction"', '"scope 1 emissions"', '"scope 2 emissions"', '"scope 3 emissions"'],
        "climate_action": ['"climate transition"', '"climate adaptation"', '"climate resilience"', '"climate strategy"'],
        "energy": ['"renewable energy"', '"clean energy"', '"energy efficiency"'],
        "resource_management": ['"water stewardship"', '"waste reduction"', '"circular economy"'],
        "climate_targets": ['"net zero target"', '"carbon neutral"', '"science-based targets"', '"sustainability metrics"', '"ESG metrics"'],
        
        # Social terms
        "community_impact": ['"community investment"', '"social impact"', '"community engagement"'],
        "human_rights": ['"human rights"', '"labor rights"', '"fair labor practices"'],
        "health_safety": ['"occupational health"', '"workplace safety"', '"employee wellbeing"'],
        
        # Governance terms
        "board_diversity": ['"board diversity"', '"diverse board"', '"board composition"'],
        "ethics": ['"business ethics"', '"ethical practices"', '"code of conduct"'],
        "risk_management": ['"ESG risk"', '"sustainability risk"', '"climate risk management"'],
        "disclosure": ['"ESG disclosure"', '"sustainability reporting"', '"TCFD disclosure"', '"SASB disclosure"'],
        "governance_metrics": ['"governance KPIs"', '"board performance metrics"', '"governance framework"']
    }
    
    for key, query in dei_esg_phrases.items():
        try:
            start = updates["phrases"][key]
            construct_sec_phrases(
                start_date=start,
                text_queries=query,
                file_path=f"data/phrases/10kq/{key}.csv",  # Changed from ../data/phrases/10kq/
                submission_type=["10-K", "10-Q"]
            )
            updates = update_data(updates, "phrases", key)
            save_updates(updates, update_file)
        except Exception as e:
            print(f"{key} error: {e}")




    # Process metadata
    try:
        process_submissions_metadata(output_dir="data/filer_metadata/")  # Changed from ../data/filer_metadata/
        updates = update_data(updates, "submissions_metadata")
        save_updates(updates, update_file)
    except Exception as e:
        print(f"Metadata error: {e}")

if __name__ == "__main__":
    run_updates()