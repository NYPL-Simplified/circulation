from fuzzywuzzy import fuzz
from nameparser import HumanName
from nose.tools import set_trace
import re
import unicodedata

from permanent_work_id import WorkIDCalculator;


"""Fallback algorithms for dealing with personal names when VIAF fails us."""

phdFix = re.compile("((. +)|(, ?))P(h|H)\.? *(D|d)(\.| |$){1}")
mdFix = re.compile("((. +)|(, ?))M\.? *D(\.| |$){1}")


def replaceMD(match):
    """
    :param match: a regular expression matched to a string
    """
    if not match or len(match.groups()) < 1:
        return match

    return match.groups()[0] + "MD"


def replacePhD(match):
    """
    :param match: a regular expression matched to a string
    """
    if not match or len(match.groups()) < 1:
        return match

    return match.groups()[0] + "PhD"


def contributor_name_match_ratio(name1, name2, normalize_names=True):
    """
    Returns a number between 0 and 100, representing the percent 
    match (Levenshtein Distance) between name1 and name2, 
    after each has been normalized.
    """
    if normalize_names:
        name1 = normalize_contributor_name_for_matching(name1)
        name2 = normalize_contributor_name_for_matching(name2)
    match_ratio = fuzz.ratio(name1, name2)
    return match_ratio


def is_corporate_name(display_name):
    """Does this display name look like a corporate name?"""

    corporations = ['National Geographic', 'Smithsonian Institution', 'Princeton', 

        'Verlag', 'College', 'University', 'Scholastic', 'Faculty of', 'Library', 

        'Harper & Brothers', 'Harper Collins', 'HarperCollins', 'Williams & Wilkins', 
        'Estampie', 'Paul Taylor Dance', 'Gallery', 'EMI Televisa', 'Mysterious Traveler', 

        'Association', 'International', 'National', 'Society', 'Team', 

        'History', 'Science', 

        u'\xa9', 'Copyright', '(C)', '&#169;', 

        'Professors', 'Multiple', 'Various',  
        'Full Cast', 'BBC', 'LTD', 'Limited', 'Productions', 'Visual Media', 'Radio Classics'
        ]

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


def is_one_name(human_name):
    """ Examples: 'Pope Francis', 'Prince'. """
    if name.first and not name.last:
        return True

    return False


def display_name_to_sort_name(display_name):
    """
    Take the "First Name Last Name"-formatted display_name, and convert it 
    to a "Last Name, First Name" format appropriate for searching and sorting by.

    Checks first if the display_name fits what we know of corporate entity business names.
    If yes, uses the whole name without re-converting it.

    Uses the HumanName library to try to parse the name into parts, and rearrange the parts into 
    desired order and format.
    """

    if not display_name:
        return None

    # TODO: to humanname: PhD, Ph.D. Sister, Queen are titles and suffixes

    # check if corporate, and if yes, return whole
    if is_corporate_name(display_name):
        return display_name

    # clean up the common PhD and MD suffixes, so HumanName recognizes them better
    display_name = phdFix.sub(replacePhD, display_name, re.I)
    display_name = mdFix.sub(replaceMD, display_name, re.I)
    
    # name has title, first, middle, last, suffix, nickname
    name = HumanName(display_name)

    if name.nickname:
        name.nickname = '(' + name.nickname + ')'

    if not name.last:
        # Examples: 'Pope Francis', 'Prince'.
        sort_name = u' '.join([name.first, name.middle, name.suffix, name.nickname])
        if name.title:
            sort_name = u''.join([name.title, ", ", sort_name])
    else:
        sort_name = u' '.join([name.first, name.middle, name.suffix, name.nickname, name.title])
        sort_name = u''.join([name.last, ", ", sort_name])

    sort_name = name_tidy(sort_name)

    return sort_name


def name_tidy(name):
    """
    Convert to NFKD unicode.
    Strip excessive whitespace.
    """
    name = unicodedata.normalize("NFKD", unicode(name))
    name = WorkIDCalculator.consecutiveCharacterStrip.sub(" ", name)

    name = name.strip()
    if name.endswith(','):
        name = name[:-1]

    return name.strip()


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






