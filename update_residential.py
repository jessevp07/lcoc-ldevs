import logging
import pandas as pd
from multiprocessing import Process
import config
import lcoc.urdb as urdb
import lcoc.afdc as afdc 
import lcoc.processing as proc

logfile = "logs/res.log"
logger = logging.getLogger(__name__)
hdlr = logging.FileHandler(logfile)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

#load URDB
db = urdb.DatabaseRates(config.URDB_PATH)
logger.info("Residential - URDB loaded")

#filter expired rates
db.filter_stale_rates(industry='residential')
logger.info("Residential - stale rates filtered")

#classify rates by is_tier, is_seasonal, is_TOU, is_ev-specific (residential only)
db.classify_rate_structures(industry='residential')
logger.info("Residential - tariffs classified")

#filter demand rates (residential only)
db.filter_demand_rates(industry='residential')
logger.info("Residential - rates with demand charges filtered")

#filter rates containing certain phrases in 'filters/'
db.filter_on_phrases(industry='residential')
logger.info("Residential - rates containing filter phrases removed")

#combine base rate + adjusted rate
db.combine_rates(industry='residential')
logger.info("Residential - base rate & adjusted rate combined")

#filter null rates
db.filter_null_rates(industry='residential')
logger.info("Residential - null rates filtered")
logger.info("Residential - preprocessing complete!")

#calculate annual electricity cost (rates)
db.calculate_annual_energy_cost_residential()
logger.info("Residential - annual energy costs calculated")

#calculate annual electricity cost (utility-level)
proc.res_rates_to_utils()
logger.info("Residential - rates aggregated to utility-level")

#calculate annual electricity cost (state-level)
proc.res_utils_to_state()
logger.info("Residential - rates aggregated to state-level")

#calculate levelized costs of charging (state-level)
proc.calculate_state_residential_lcoc()
logger.info("Residential - state-level LCOC complete!")