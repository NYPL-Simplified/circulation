
import re

from fuzzywuzzy import fuzz

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


def title_match_ratio(title1, title2):
    """
    Returns a number between 0 and 100, representing the percent
    match (Levenshtein Distance) between book title1 and book title2,
    after each has been normalized.
    """
    title1 = normalize_title_for_matching(title1)
    title2 = normalize_title_for_matching(title2)
    match_ratio = fuzz.ratio(title1, title2)
    return match_ratio


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





