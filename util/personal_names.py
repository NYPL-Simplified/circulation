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
