from os import sys, path

# Good overview of what is going on here:
# https://stackoverflow.com/questions/11536764/how-to-fix-attempted-relative-import-in-non-package-even-with-init-py
# Once we have a stable package name for core, it should be easier to do away with something like this
# for now we add the core component path to the sys.path when we are running these scripts
component_dir = path.dirname(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))

# Load the 'core' module as though this script were being run from
# the parent component (either circulation or metadata).
sys.path.append(component_dir)