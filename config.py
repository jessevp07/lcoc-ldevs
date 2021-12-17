import sys
import os
# analysis = os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis')
# sys.path.append(analysis)
# import directories as dir

# PATH variables
HOME_PATH = os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs')
DATA_PATH = os.path.join(HOME_PATH,'data')
OUTPUT_PATH = os.path.join(HOME_PATH,'outputs')
URDB_PATH = os.path.join(DATA_PATH,'urdb','usurdb_20220526.csv')
# FUEL_PRICE_PATH = os.path.join(DATA_PATH,'aaa','191101_fuel_prices_aa.csv')
AFDC_PATH = os.path.join(DATA_PATH,'afdc','afdc_20220526.csv')
EIA_RES_PATH = os.path.join(DATA_PATH,'eia','residential-electricity-prices','eia_residential_20.csv')
EIA_COM_PATH = os.path.join(DATA_PATH,'eia','commercial-electricity-prices','eia_commercial_20.csv')
EIAID_TO_UTILITY_CW_PATH = os.path.join(DATA_PATH,'eia','eiaid-crosswalk','eia_crosswalk_20.csv')
EIAID_TO_COUNTY_CW_PATH = os.path.join(DATA_PATH,'eia','eiaid-to-county','eia_service_territory_20.csv')
EIA_TO_COUNTY_LOOKUP_PATH = os.path.join(DATA_PATH,'eia','eiaid-to-county','eia_county_lookup_20.csv')
AEO_GAS_ELECTR_PROJ_PATH = os.path.join(DATA_PATH,'eia','15yr-gas-electricity-price-projections','eia_aeo21_gas_electricity_price_projections.csv')

DCFC_PROFILES_DICT = {'p1': os.path.join(DATA_PATH,'dcfc-load-profiles','dcfc_current_1plug-low_50kW.csv'),
                      'p2': os.path.join(DATA_PATH,'dcfc-load-profiles','dcfc_current_1plug-high_50kW.csv'),
                      'p3': os.path.join(DATA_PATH,'dcfc-load-profiles','dcfc_interm_4plugs_150kW.csv'),
                      'p4': os.path.join(DATA_PATH,'dcfc-load-profiles','dcfc_future_20plugs_150kW.csv')}