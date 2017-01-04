from fuzzywuzzy import fuzz
from nameparser import HumanName
from nose.tools import set_trace
import re
import unicodedata

from permanent_work_id import WorkIDCalculator;


"""Fallback algorithms for dealing with personal names when VIAF fails us."""

def is_corporate_name(display_name):
    """Does this display name look like a corporate name?"""
    corporations = ['National Geographic', 'Smithsonian Institution', 
        'Verlag', 'College', 'University',  
        'Harper & Brothers', 'Williams & Wilkins', 
        'Estampie', 'Paul Taylor Dance', 'Gallery']

    display_name = display_name.lower().replace(".", "").replace(",", "").replace("&amp;", "&")

    for corporation in corporations:
        if corporation.lower() in display_name:
            return True

        if fuzz.ratio(corporation, display_name) > 90:
            return True

    if (display_name.startswith('the ') or display_name.startswith('editor ') 
        or display_name.startswith('editors ') or display_name.endswith(' inc')
        or display_name.endswith(' llc') or display_name.startswith('compiled')):
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


def name_tidy(name):
    """
    Convert to NFKD unicode.
    Strip excessive whitespace.display_name.
    """
    name = unicodedata.normalize("NFKD", unicode(name))
    name = WorkIDCalculator.consecutiveCharacterStrip.sub(" ", name)
    name = name.strip()

    return name


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


def sort_name_to_display_name(sort_name):
    """
    Take the "Last Name, First Name"-formatted sort_name, and convert it 
    to a "First Name Last Name" format appropriate for displaying to patrons in a catalog listing.

    While the code attempts to do the best it can, name recognition gets complicated 
    really fast when there's more than one plain-format first name and one plain-format last name. 
    This code is meant to serve as first line of approximation.  If we later on can find better 
    human librarian-checked sort and display names in the Metadata Wrangler, we use those.
    
    :param sort_name Doe, Jane
    :return display_name Jane Doe
    """
    if not sort_name:
        return None

    name = HumanName(sort_name)
    # name has title, first, middle, last, suffix, nickname
    if name.nickname:
        name.nickname = '(' + name.nickname + ')'
    display_name = u' '.join([name.title, name.first, name.nickname, name.middle, name.last, name.suffix])

    display_name = name_tidy(display_name)

    return display_name






