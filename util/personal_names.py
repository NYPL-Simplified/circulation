from fuzzywuzzy import fuzz
from nameparser import HumanName

import re
import unicodedata
from builtins import str

from .permanent_work_id import WorkIDCalculator;


"""Fallback algorithms for dealing with personal names when VIAF fails us."""

phdFix = re.compile("((. +)|(, ?))P(h|H)\.? *(D|d)(\.| |$){1}")
mdFix = re.compile("((. +)|(, ?))M\.? *D(\.| |$){1}")
# omit exclamation point in case it can be part of stage name
# only match punctuation that's not part of name initials or title.
# so "Bitshifter, B." is OK, "Bitshifter, Bob Jr.", but "Bitshifter, Robert." is not.
trailingPunctuation = re.compile("(.*)(\w{4,})([?:.,;]*?)\Z")


def _replace_md(match):
    """
    If the "MD" professional title was matched, make sure it's got no punctuation in it.
    :param match: a regular expression matched to a string
    """
    if not match or len(match.groups()) < 1:
        return match

    return match.groups()[0] + "MD"


def _replace_phd(match):
    """
    If the "PhD" professional title was matched, make sure it's got no punctuation in it.
    :param match: a regular expression matched to a string
    """
    if not match or len(match.groups()) < 1:
        return match

    return match.groups()[0] + "PhD"


def _replace_end_punctuation(match):
    """
    If there was found to be improper punctuation at the end of the name string,
    clean it off.
    :param match: a regular expression matched to a string
    """
    if not match or len(match.groups()) < 3:
        return match

    return match.groups()[0] + match.groups()[1]


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

    corporations = [
        # magazines and scientific institutions by name
        'National Geographic', 'Smithsonian Institution',

        # educational institutions by name
        'Princeton',

        # educational institutions, general
        'Verlag', 'College', 'University', 'Scholastic', 'Faculty of', 'Library', "School of",
        'Professors',

        # publishing houses by name
        'Harper & Brothers', 'Harper Collins', 'HarperCollins', 'Williams & Wilkins',
        'Estampie', 'Paul Taylor Dance', 'Gallery', 'EMI Televisa', 'Mysterious Traveler',

        # group names, general
        'Association', 'International', 'National', 'Society', 'Team',

        # religious institutions
        "Church of", "Temple of",

        # subject names
        'History', 'Science',

        # copyrights and trademarks
        u'\xa9', 'Copyright', '(C)', '&#169;',

        # performing arts collaborations
        'Multiple', 'Various',
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
    display_name = name_tidy(display_name)

    # name has title, first, middle, last, suffix, nickname
    name = HumanName(display_name)


    # Note: When the first and middle names are initials that have come in with a space between them,
    # let them keep that space, to be consistent with initials with no periods, which would be more
    # easily algorithm-recognized if they were placed separately. So:
    # 'Classy, A. B.' and 'Classy Abe B.' and 'Classy A. Barney' and 'Classy, Abe Barney' and 'Classy, A B'.

    # This might go after a comma, or it might be someone's entire
    # name.
    base_name = u' '.join([name.title, name.first, name.middle, name.suffix]).strip()
    if not name.last:
        # Examples: 'Pope Francis', 'Prince'.
        sort_name = base_name
    else:
        if base_name:
            # A comma is used to separate the family name from the other
            # parts of the name.
            sort_name = name.last + ', ' + base_name
        else:
            # This person has _only_ a last name.
            sort_name = name.last

    # Regardless of how the name was processed, a nickname goes at the
    # end, in parentheses.
    if name.nickname:
        sort_name += ' (' + name.nickname + ')'

    # Remove excess spaces and the like.
    sort_name = name_tidy(sort_name)
    return sort_name


def name_tidy(name):
    """
    * Converts to NFKD unicode.
    * Strips excessive whitespace and trailing punctuation.
    * Normalizes PhD/MD suffixes.
    * Does not perform any potentially name-altering business logic, such as
        running HumanName parser or any other name part reorganization.
    * Does not perform any cleaning that would later need to be reversed,
        such as lowercasing.

    """
    name = unicodedata.normalize("NFKD",  str(name))
    name = WorkIDCalculator.consecutiveCharacterStrip.sub(" ", name)

    name = name.strip()

    # Check that we don't have illegitimate punctuation.  So in 'Classy, Abe.'
    # the period is probably an artifact of dirty data, but in 'Classy, A.'
    # the period is a legitimate part of the initials.
    name = trailingPunctuation.sub(_replace_end_punctuation, name, re.I)

    # clean up the common PhD and MD suffixes, so HumanName recognizes them better
    name = phdFix.sub(_replace_phd, name, re.I)
    name = mdFix.sub(_replace_md, name, re.I)

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






