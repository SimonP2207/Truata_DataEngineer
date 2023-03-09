"""
All directories/files are specified here for ease of use amongst task scripts
"""
from pathlib import Path

# Relevant directories
dirs = {'src': Path(__file__).parent}
dirs.update({
    'out': dirs['src'].parent / 'out',
    'files': dirs['src'] / 'files'
})

# Input files
files = {
    'groceries': dirs['files'] / 'groceries.csv',
    'airbnb': dirs['files'] / 'sf-airbnb-clean.parquet',
    'iris': dirs['files'] / 'iris.csv'
}
