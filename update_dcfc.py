import sys
import logging
import config
import lcoc.urdb as urdb
import lcoc.afdc as afdc
import lcoc.processing as proc

p = sys.argv[1] #p = dcfc station profile name

logfile = "logs\dcfc_{}.log".format(p)
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(logfile)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

db = urdb.DatabaseRates(config.URDB_PATH)
logger.info("URDB loaded")

#filter expired rates
db.filter_stale_rates(industry='commercial')
logger.info("DCFC-{}: stale commercial rates filtered".format(p))

#classify rates by is_tier, is_seasonal, is_TOU
db.classify_rate_structures(industry='commercial')
logger.info("DCFC-{}: commercial tariffs classified".format(p))

#standardize units of reporting for commercial rates
db.com_rate_preprocessing()
logger.info("DCFC-{}: units standardized for reporting".format(p))

#filter commercial rates missing critical fields to approx the cost of electricity
db.additional_com_rate_filters()
logger.info("DCFC-{}: rates w/o critical fields filtered".format(p))

#filter rates containing certain phrases in 'filters/'
db.filter_on_phrases(industry='commercial')
logger.info("DCFC-{}: rates containing DCFC filter words filtered".format(p))

#combine base rate + adjusted rate
db.combine_rates(industry='commercial')
logger.info("DCFC-{}: combined base rate & adjusted rate".format(p))

#filter null rates
db.filter_null_rates(industry='commercial')
logger.info("DCFC-{}: filtered null rates".format(p))
logger.info("DCFC-{}: preprocessing complete".format(p))

outpath = 'outputs\\cost-of-electricity\\urdb-dcfc-rates\\'

load_profiles = config.DCFC_PROFILES_DICT

inpath = load_profiles[p]

#calculate annual electricity cost (rate-level)
db.calculate_annual_cost_dcfc({p:inpath}, outpath)
logger.info("DCFC-{}: annual electricity cost calculated at rate-level".format(p))

#calculate annual electricity cost (utility-level)
proc.dcfc_rates_to_utils()
logger.info("DCFC-{}: rates aggregated to utility-level".format(p))

#calculate annual electricity cost (county-level)
proc.dcfc_utils_to_county()
logger.info("DCFC-{}: utils joined to county".format(p))

#AFDC
stations = afdc.DCFastChargingLocator() #GET DCFC stations from AFDC
logger.info("DCFC-{}: DCFC station locations loaded from AFDC".format(p))
stations.join_county_geoid() #join to county (spatial)
logger.info("DCFC-{}: AFDC stations joined to counties".format(p))
stations.aggregate_counties_to_csv() #aggregate to county-lvl, output to .csv
logger.info("DCFC-{}: utils aggregated to county-level".format(p))

#calculate annual electricity cost (state-level)
proc.dcfc_county_to_state()
logger.info("DCFC-{}: state-level LCOC complete!".format(p))



