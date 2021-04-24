"""
Script takes a snapshot of URDB and saves it to working dir.
"""

import sys
sys.path.append('../../')
from evfast import urdb

urdb.DatabaseRates()
print("URDB snapshot downloaded.")