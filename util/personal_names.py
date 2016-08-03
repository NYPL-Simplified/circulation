from fuzzywuzzy import fuzz
from nameparser import HumanName
from nose.tools import set_trace
import re

from permanent_work_id import WorkIDCalculator;


"""Fallback algorithms for dealing with personal names when VIAF fails us."""

def is_corporate_name(display_name):
    """Does this display name look like a corporate name?"""
    corporations = ['National Geographic', 'Smithsonian Institution', 
        'Verlag', 'College', 'University',  
        'Harper & Brothers', 'Harper &amp; Brothers', 'Williams & Wilkins', 'Williams &amp; Wilkins', 
        'Estampie', 'Paul Taylor Dance', 'Gallery']

    for corporation in corporations:
        if corporation in display_name:
            # TODO: consider making case-insensitive
            return True

        if fuzz.ratio(corporation, display_name) > 90:
            return True

    c = display_name.lower().replace(".", "").replace(",", "")
    if (c.startswith('the ') or c.startswith('editor ') 
        or c.startswith('editors ') or c.endswith(' inc')
        or c.endswith(' llc') or c.startswith('compiled')):
        return True
    return False


def display_name_to_sort_name(display_name):
    c = display_name.lower()
    if c.endswith('.'):
        c = c[:-1]
    if is_corporate_name(display_name):
        return display_name
    
    parts = display_name.split(" ")
    if len(parts) == 1:
        return parts[0]
    else:
        return parts[-1] + ", " + " ".join(parts[:-1])


def normalize_contributor_name_for_matching(name):
    """
    Used to standardize author names before matching them to each other to identify best results 
    in VIAF author search feeds.

    Split the name into title, first, middle, last name, suffix, nickname, and set the parts in that order.
    Remove spacing around abbreviated initials, so 'George RR Martin' matches 'George R R Martin' (treat 
    two-letter words as initials).

    Run WorkIDCalculator.normalize_author on the name, which will convert to NFKD unicode, 
    de-lint special characters and spaces, and lowercase.

    TODO: Consider: Further remove periods, commas, dashes, and all non-word characters.
    TODO: consider what to do for multiple authors, like an et al or brothers grimm
    """

    # HumanName has a bug where two joiner words together cause an error.
    # This is a quickie hack to fix that.
    joiner_fix = re.compile('of the', re.IGNORECASE)
    name = joiner_fix.sub('of', name)

    name = HumanName(name)
    # name has title, first, middle, last, suffix, nickname
    name = u' '.join([name.title, name.first, name.middle, name.last, name.suffix, name.nickname])

    name = WorkIDCalculator.normalize_author(name)
    return name








