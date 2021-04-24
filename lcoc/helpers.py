"""
Helper functions for working with data.
"""
#public
import datetime

def contains_filter_phrases(description, filter_phrases):
    """
    Returns True if any phrase in filter_phrases is also in description.
    """
    if isinstance(description, float):
        contains = False
    else:
        contains = any([phrase.lower() in  description.lower() for phrase in filter_phrases])       
    return contains

def todays_date():
    """
    Returns today's date in YYYYMMDD format.
    """
    now = datetime.datetime.now()
    date_str = "{0}{1}{2}".format(now.year, now.strftime('%m').zfill(2), now.strftime('%d').zfill(2))

    return date_str
