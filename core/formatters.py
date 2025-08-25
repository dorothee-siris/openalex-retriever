"""Data formatting functions"""
import unicodedata
from typing import Dict, List, Optional, Any
import re

# Pre-compiled regex patterns
OPENALEX_PATTERN = re.compile(r'https://openalex.org/')
DOI_PATTERN = re.compile(r'https://doi.org/')

def clean_text_field(text: str) -> str:
    """Clean text fields by removing ALL line breaks and control characters"""
    if not text or not isinstance(text, str):
        return text
    
    # Remove all control characters
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', text)
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = text.replace('\u2028', ' ').replace('\u2029', ' ')
    text = ' '.join(text.split())
    return text.strip()

def format_abstract_optimized(inverted_index: Dict) -> str:
    """Optimized abstract reconstruction"""
    if not inverted_index:
        return ""
    
    try:
        positions = []
        words = []
        
        for word, pos_list in inverted_index.items():
            positions.extend(pos_list)
            words.extend([word] * len(pos_list))
        
        if not positions:
            return ""
        
        # Sort and join
        if len(positions) > 1000:
            import numpy as np
            sort_idx = np.argsort(positions)
            abstract_text = " ".join(np.array(words)[sort_idx])
        else:
            abstract_text = " ".join(word for _, word in sorted(zip(positions, words)))
        
        return clean_text_field(abstract_text)
    except Exception:
        return "[Abstract processing error]"

def format_authors_simple(authorships: List[Dict]) -> str:
    """Format authors information"""
    if not authorships:
        return ""
    
    authors = []
    for authorship in authorships:
        author_name = authorship.get('author', {}).get('display_name', 'Unknown').strip()
        if authorship.get("is_corresponding", False):
            author_name += " (corresponding)"
        authors.append(author_name)
    
    return " | ".join(authors)

def format_institutions(authorships: List[Dict]) -> str:
    """Format institutions with deduplication"""
    if not authorships:
        return ""
    
    seen = set()
    institutions = []
    
    for authorship in authorships:
        for inst in authorship.get("institutions", []):
            inst_id = inst.get("id", "")
            if inst_id and inst_id not in seen:
                seen.add(inst_id)
                inst_name = inst.get("display_name", "Unknown")
                inst_type = inst.get("type", "Unknown")
                inst_country = inst.get("country_code", "Unknown")
                inst_id_clean = OPENALEX_PATTERN.sub('', inst_id)
                institutions.append(f"{inst_name} ; {inst_type} ; {inst_country} ({inst_id_clean})")
    
    return " | ".join(institutions)

def format_raw_affiliation_strings(authorships: List[Dict]) -> str:
    """Format raw affiliation strings"""
    if not authorships:
        return ""
    
    affiliations = set()
    for authorship in authorships:
        for affiliation in authorship.get("raw_affiliation_strings", []):
            if affiliation and affiliation.strip():
                clean_affiliation = clean_text_field(affiliation.strip())
                clean_affiliation = unicodedata.normalize('NFC', clean_affiliation)
                affiliations.add(clean_affiliation)
    
    return " | ".join(sorted(affiliations))

def format_counts_by_year(counts: List[Dict]) -> str:
    """Format counts by year"""
    if not counts:
        return ""
    
    return " | ".join(
        f"{c.get('cited_by_count', 0)} ({c.get('year', 'Unknown')})"
        for c in sorted(counts, key=lambda x: x.get("year", 0), reverse=True)
    )

def format_topic_and_score(topics: List[Dict]) -> str:
    """Format topics with scores"""
    if not topics:
        return ""
    
    return " | ".join(
        f"{t.get('display_name', 'Unknown')} ; {t.get('score', 0):.4f}"
        for t in topics
    )

def format_concepts(concepts: List[Dict]) -> str:
    """Format concepts"""
    if not concepts:
        return ""
    
    concepts_by_level = {}
    for concept in concepts:
        level = concept.get("level", 0)
        if level not in concepts_by_level:
            concepts_by_level[level] = []
        concepts_by_level[level].append(
            f"{concept.get('display_name', 'Unknown')} ; {concept.get('score', 0):.4f} (level {level})"
        )
    
    return " | ".join(
        item for level in sorted(concepts_by_level.keys())
        for item in concepts_by_level[level]
    )

def format_sdgs(sdgs: List[Dict]) -> str:
    """Format SDGs"""
    if not sdgs:
        return ""
    
    return " | ".join(
        f"{sdg.get('display_name', 'Unknown')} ; {sdg.get('score', 0):.2f}"
        for sdg in sdgs
    )

def format_grants(grants: List[Dict]) -> str:
    """Format grants"""
    if not grants:
        return ""
    
    formatted = []
    for grant in grants:
        funder = grant.get("funder_display_name", "Unknown")
        award_id = grant.get("award_id", "")
        formatted.append(f"{funder} ({award_id})" if award_id else funder)
    
    return ", ".join(formatted)

def extract_author_position(pub: Dict, author_id: str) -> str:
    """Extract the position of an author in a publication"""
    authorships = pub.get('authorships', [])
    author_id_clean = author_id.lower()
    
    for i, authorship in enumerate(authorships):
        auth_id = authorship.get('author', {}).get('id', '').lower()
        if auth_id.endswith(author_id_clean):
            if i == 0:
                return "First"
            elif i == len(authorships) - 1:
                return "Last"
            else:
                return "Middle"
    return "Not found"