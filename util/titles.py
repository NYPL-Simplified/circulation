from nose.tools import set_trace
import re

from permanent_work_id import WorkIDCalculator;



def normalize_title_for_matching(title):
    """
    Used to standardize book titles before matching them to each other to identify best results 
    in VIAF author search feeds.

    Run WorkIDCalculator.normalize_title on the name, which will convert to NFKD unicode, 
    de-lint special characters, and lowercase.
    """
    title = WorkIDCalculator.normalize_title(u''.join(title))
    return title


def unfluff_title(title):
    """
    Removes parts of the title that are deemed to be add-ons, like imprint information, 
    inserted subtitles and corporate names.
    For example, in:
    Hello World, edited by Bob Bobbinson
    Hello World: The True and Amazing Adventures of Bob
    Hello World (Unabridged)
    (TODO: later add logic for something like Hello World, Harvard University, publisher)
    we want to return "Hello World".
    """
    linted_title = title
    title_fluff = re.compile(r'(.*) (edited by|compiled by|published by|:|;|\(|\[).*', re.UNICODE)
    matched_pattern = title_fluff.match(title)

    if matched_pattern is not None:
        linted_title = matched_pattern.group(1)

    # now strip non-word characters
    title_fluff = re.compile('[\W_]+')
    linted_title = title_fluff.sub(' ', linted_title)
    # and remove double spacing that may result
    title_fluff = re.compile('[  ]+')
    linted_title = title_fluff.sub(' ', linted_title).lower().strip()

    return linted_title





