"""
Script takes a snapshot of AFDC and saves it to working dir.
"""

import sys
sys.path.append('../../')
from evfast import afdc

afdc.DCFastChargingLocator()
print("AFDC snapshot downloaded.")