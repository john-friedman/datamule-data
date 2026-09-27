import re
from collections import defaultdict

class UnionFind:
    """Union-Find data structure with path compression and union by rank."""
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x):
        """Find with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        """Union by rank."""
        px, py = self.find(x), self.find(y)
        if px == py:
            return False
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1
        return True
    
    def get_groups(self):
        """Return dictionary mapping root -> list of elements in that group."""
        groups = defaultdict(list)
        for i in range(len(self.parent)):
            groups[self.find(i)].append(i)
        return groups


def validate_identifiers(rows):
    """
    Validate financial identifiers using regex patterns and filter out invalid ones.
    
    Args:
        rows: List of dictionaries with financial identifiers
    
    Returns:
        List of dictionaries with only valid identifiers
    """
    # Regex patterns for financial identifiers
    patterns = {
        'cusip': re.compile(r'^[0-9A-Z]{9}$'),  # 9 alphanumeric characters
        'isin': re.compile(r'^[A-Z]{2}[0-9A-Z]{10}$'),  # 2 letters + 10 alphanumeric
        'figi': re.compile(r'^BBG[0-9A-Z]{9}$')  # BBG + 9 alphanumeric
    }
    
    validated_rows = []
    
    for row in rows:
        validated_row = {}
        
        # Copy accession if present
        if 'accession' in row:
            validated_row['accession'] = row['accession']
        
        # Validate each identifier
        for identifier in ['cusip', 'isin', 'figi']:
            if identifier in row and row[identifier]:
                value = str(row[identifier]).upper().strip()
                
                if patterns[identifier].match(value):
                    validated_row[identifier] = value
        
        # Only keep rows that have at least 2 valid identifiers
        identifier_count = sum(1 for key in ['cusip', 'isin', 'figi'] if key in validated_row)
        if identifier_count >= 2:
            validated_rows.append(validated_row)
    
    return validated_rows


def deduplicate_and_merge(rows):
    """
    Optimized deduplication using Union-Find and hash indices.
    Time complexity: O(n × α(n)) ≈ O(n)
    
    Args:
        rows: List of validated dictionaries with financial identifiers
    
    Returns:
        List of unique, merged dictionaries with maximum information per security
    """
    if not rows:
        return []
    
    n = len(rows)
    uf = UnionFind(n)
    
    # Build reverse indices for O(1) lookup
    indices = {
        'cusip': defaultdict(list),
        'isin': defaultdict(list),
        'figi': defaultdict(list)
    }
    
    # Single pass to build indices
    for i, row in enumerate(rows):
        for identifier_type in ['cusip', 'isin', 'figi']:
            if identifier_type in row and row[identifier_type]:
                value = row[identifier_type]
                indices[identifier_type][value].append(i)
    
    # Connect components using Union-Find
    for identifier_type in ['cusip', 'isin', 'figi']:
        for value, row_indices in indices[identifier_type].items():
            if len(row_indices) > 1:
                # Union all rows with this identifier value
                first = row_indices[0]
                for idx in row_indices[1:]:
                    uf.union(first, idx)
    
    # Get connected components
    groups = uf.get_groups()
    
    # Merge rows within each group
    final_rows = []
    for root, group_indices in groups.items():
        # Merge all identifiers from rows in this group
        merged = {}
        
        for idx in group_indices:
            row = rows[idx]
            for identifier in ['cusip', 'isin', 'figi']:
                if identifier in row and row[identifier]:
                    # Keep the first non-null value we find for each identifier
                    if identifier not in merged:
                        merged[identifier] = row[identifier]
        
        # Only include if we have at least 2 identifiers
        if sum(1 for k in ['cusip', 'isin', 'figi'] if k in merged) >= 2:
            final_rows.append(merged)
    
    return final_rows