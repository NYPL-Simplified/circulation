from nameparser import HumanName
from nose.tools import set_trace
import re

from permanent_work_id import WorkIDCalculator;


"""Fallback algorithms for dealing with personal names when VIAF fails us."""

def is_corporate_name(display_name):
    """Does this display name look like a corporate name?"""
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
    Run WorkIDCalculator.normalize_author on the name, which will convert to NFKD unicode, 
    de-lint special characters and spaces, and lowercase.

    Further remove periods, commas, dashes, and all non-word characters.
    Remove spacing around abbreviated initials, so 'George RR Martin' matches 'George R R Martin' (treat 
    two-letter words as initials).

    TODO: consider what to do for multiple authors
    """

    name = HumanName(name)
    # name has title, first, middle, last, suffix, nickname
    print "name.first=%s, name.middle=%s, name.last=%s, name.nickname=%s" % (name.first, name.middle, name.last, name.nickname)
    name = u' '.join([name.title, name.first, name.middle, name.last, name.suffix, name.nickname])

    name = WorkIDCalculator.normalize_author(name)
    return name



