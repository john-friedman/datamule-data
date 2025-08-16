import re

def find_cusips_html(text, distance=20):
    # Use f-string to inject the distance parameter into the regex
    pattern = rf'(CUSIP.{{0,{distance}}}\b([0-9A-HJ-NP-Z]{{8}}[0-9])\b|\b([0-9A-HJ-NP-Z]{{8}}[0-9])\b.{{0,{distance}}}CUSIP)'
    
    results = []
    for m in re.finditer(pattern, text, re.DOTALL):
        # Group 2 = CUSIP identifier when "CUSIP" comes first
        # Group 3 = CUSIP identifier when "CUSIP" comes after
        cusip_id = m.group(2) if m.group(2) else m.group(3)
        
        # Find the actual start position of the CUSIP identifier within the match
        cusip_start_in_match = m.group().find(cusip_id)
        cusip_index = m.start() + cusip_start_in_match
        
        results.append({'cusip': cusip_id, 'index': cusip_index})
    
    return results

import re

def find_cusips_xml(xml_content):
    # Simple regex to capture content between <issuerCusip> tags
    pattern = r'<issuercusip>([^<]{9})'
    
    results = []
    for m in re.finditer(pattern, xml_content.lower()):
        cusip = m.group(1)
        results.append({'cusip': cusip, 'index': m.start(1)})
    
    return results