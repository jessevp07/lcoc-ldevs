import glob
import pandas as pd
import numpy as np
import config
from lcoc import afdc

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

##### Functions #####

###################
### Residential ###
###################

def res_rates_to_utils(scenario = 'baseline',
                       urdb_rates_file = 'outputs/cost-of-electricity/urdb-res-rates/res_rates.csv',
                       eia_cw_file = config.EIAID_TO_UTILITY_CW_PATH,
                       eia_utils_file = config.EIA_RES_PATH,
                       outpath = 'outputs/cost-of-electricity/res-utilities/'):
    """
    Takes res urdb rates from urdb_path and combines with eia_utils_file to produce
    utility-lvl annual avg cost of electricity estimates under the following scenarios:
    'baseline' (replace eia cost of electricity w/ off-peak TOU rate, if applicable), 
    'no-tou' (eia cost of electricity only), 'tou-only' (only TOU rates
    from URDB are considered).
    """
    
    # Load/Preprocess EIA datasets 
    eiaid_cw = pd.read_csv(eia_cw_file)
    eiaid_cw = eiaid_cw[['eiaid', 'entity', 'state']]
    eiaid_utils = pd.read_csv(eia_utils_file)
    eiaid_utils.rename(columns={'avg_price_cents_per_kwh': 'eia_cost_per_kwh'}, inplace=True)
    eiaid_utils['eia_cost_per_kwh'] = eiaid_utils['eia_cost_per_kwh'] / 100
    eiaid_utils = eiaid_utils[eiaid_utils.eiaid!=99999]
    wm = lambda x: np.average(x, weights=eiaid_utils.loc[x.index, "customers"])
    f = {'customers': 'sum', 'eia_cost_per_kwh': wm}
    eiaid_utils = eiaid_utils.groupby(['entity', 'state']).agg(f).reset_index()
    #eiaid_utils.columns = eiaid_utils.columns.droplevel(1)
    eiaid_res_df = eiaid_cw.merge(eiaid_utils, how='right', on=['entity', 'state'])
    eiaid_res_df = eiaid_res_df.drop_duplicates()
    
    # Load URDB Rates
    urdb_rates = pd.read_csv(urdb_rates_file, low_memory=False)
    
    # Find Off-Peak TOU Price for URDB Rates
    all_tou_rates_df = urdb_rates[urdb_rates.is_tou_rate==1]
    eiaid_tou_rates_df = all_tou_rates_df.groupby('eiaid')['electricity_cost_per_kwh'].min().reset_index()
    eiaid_tou_rates_df.rename(columns={'electricity_cost_per_kwh': 'offpeak_tou_cost_per_kwh'}, inplace=True)

    # Baseline - {MIN((off-peak TOU, EIA average))}
    if scenario == "baseline": #default
        eiaid_res_df = eiaid_res_df.merge(eiaid_tou_rates_df, how='left', on='eiaid')
        tou_rates_used, costs_incl_tou = 0, []
        for i in range(len(eiaid_res_df)):
            eia_cost = eiaid_res_df.iloc[i].eia_cost_per_kwh
            offpeak_tou_cost = eiaid_res_df.iloc[i].offpeak_tou_cost_per_kwh
            low_cost = min([eia_cost, offpeak_tou_cost])
            if low_cost == offpeak_tou_cost:
                tou_rates_used+=1
            costs_incl_tou.append(low_cost)
        eiaid_res_df['cost_per_kwh'] = costs_incl_tou
    
        print("Complete, {0} utitilies represented ({1} TOU rates used).".format(len(eiaid_res_df), 
                                                                                     tou_rates_used))
        eiaid_res_df.to_csv(outpath+'res_utils.csv', index=False)
    
    # No-TOU - "Business as Usual", EIA averages used (upper bound)
    elif scenario == "no-tou":
        eiaid_res_df['cost_per_kwh'] = eiaid_res_df['eia_cost_per_kwh']
        
        print("Complete, {} utilities represented (no TOU rates used).".format(len(eiaid_res_df)))
        eiaid_res_df.to_csv(outpath+"upper_bnd_res_utils.csv", index=False)
        
    # TOU-Only - URDB TOU rates only (lower bound)
    elif scenario == "tou-only":
        eiaid_tou_rates_df['cost_per_kwh'] = eiaid_tou_rates_df['offpeak_tou_cost_per_kwh']
        eiaid_tou_rates_df = eiaid_tou_rates_df.merge(eiaid_res_df[['eiaid', 'state', 'customers']], how='inner', on='eiaid')
        
        print("Complete, {} utitilies represented (only TOU rates used).".format(len(eiaid_tou_rates_df)))
        eiaid_tou_rates_df.to_csv(outpath+"lower_bnd_res_utils.csv", index=False)
    
    else:
        raise ValueError('scenario not in ["baseline", "no_tou", "tou-only"]')
        
    return eiaid_res_df
              
        
def res_utils_to_state(utils_file = 'outputs/cost-of-electricity/res-utilities/res_utils.csv',
                       outfile = 'outputs/cost-of-electricity/res-states/res_states_baseline.csv'):
    """
    Takes utility-level cost of electricity and calculates customer-weighted state-level
    cost of electricity for the baseline scenario (TOU & No-TOU).
    """
    res_util_df = pd.read_csv(utils_file, low_memory=False)
    
    states, cost_per_kwh, customers = [], [], []
    for state in set(res_util_df['state']):
        temp_df = res_util_df[res_util_df['state'] == state]
        tot_customers = temp_df['customers'].sum()
        wgt_cost = ((temp_df['cost_per_kwh'] * temp_df['customers']) / tot_customers).sum()
        states.append(state)
        customers.append(tot_customers)
        cost_per_kwh.append(wgt_cost)

    state_df = pd.DataFrame({'state': states,
                             'customers': customers,
                             'cost_per_kwh': cost_per_kwh})

    #Add national estimate
    nat_customers = state_df['customers'].sum()
    nat_cost_per_kwh = ((state_df['cost_per_kwh'] * state_df['customers']) / nat_customers).sum()
    nat_df = pd.DataFrame({'state': ['US'],
                           'customers': [nat_customers],
                           'cost_per_kwh': [nat_cost_per_kwh]})
    state_df = pd.concat([state_df, nat_df]).reset_index(drop=True)
    state_df.to_csv(outfile, index=False)
    print('Complete, national cost of electricity is ${}/kWh.'.format(round(nat_cost_per_kwh,2)))
    
    
def calculate_state_residential_lcoc(coe_file = 'outputs/cost-of-electricity/res-states/res_states_baseline.csv',
                                     fixed_costs_path = 'data/fixed-costs/residential/',
                                     annual_maint_frac = 0.01, #Annual cost of maintenance (fraction of equip costs)
                                     veh_lifespan = 15,
                                     veh_kwh_per_100miles = 29.82, #source: EIA
                                     aavmt = 10781, #source: 2017 NHTS
                                     fraction_residential_charging = 0.81, #source: EPRI study
                                     fraction_home_l1_charging = 0.16, #source: EPRI study
                                     dr = 0.035, #source: Mercatus
                                     outfile = 'outputs/cost-of-charging/residential/res_states_baseline.csv'):
    """
    Function calculates the state-level residential levelized cost of charging, taking 
    into account the average cost of electricity, fixed costs, and equipment 
    maintenance.
    """
    
    # Load data
    df = pd.read_csv(coe_file)
    filenames = ['res_level1.txt', 'res_level2.txt']
    fixed_cost_files = [fixed_costs_path + filename for filename in filenames]
    
    fixed_costs = {}
    for file in fixed_cost_files:
        if 'level1' in file:
            plug_typ = 'L1'      
        elif 'level2' in file:
            plug_typ = 'L2'

        plug_typ_dict = {}
        with open (file) as f:
            for line in f:
                key, val = line.split(':')
                plug_typ_dict[key] = float(val)
            
        fixed_costs[plug_typ] = plug_typ_dict
        
    # Calculate lifetime EVSE cost of maintenance (assumed to be 1% of equipment cost annually)
    for plug_typ in fixed_costs.keys():
        
        discounted_lifetime_maint_cost = 0
        for i in range(1, veh_lifespan+1):
            ann_maint_cost = annual_maint_frac * fixed_costs[plug_typ]['equipment']
            discounted_ann_maint_cost = ann_maint_cost / (1+dr)**i
            discounted_lifetime_maint_cost += discounted_ann_maint_cost
        
        fixed_costs[plug_typ]['lifetime_evse_maint'] = discounted_lifetime_maint_cost
        
    # Calculate lifetime energy from residential charging
    lifetime_miles = veh_lifespan * aavmt
    veh_kwh_per_mile = veh_kwh_per_100miles / 100
    lifetime_energy_kwh = lifetime_miles * veh_kwh_per_mile
    lifetime_residential_energy_kwh = fraction_residential_charging * lifetime_energy_kwh
    
    # Calculate lvl fixed costs for residential L1, L2 charging
    try:
        lvl_fixed_costs_l1 = (fixed_costs['L1']['equipment'] + fixed_costs['L1']['installation'] \
        + fixed_costs['L1']['lifetime_evse_maint']) / lifetime_residential_energy_kwh
    except:
        lvl_fixed_costs_l1 = 0
    
    lvl_fixed_costs_l2 = (fixed_costs['L2']['equipment'] + fixed_costs['L2']['installation'] \
    + fixed_costs['L2']['lifetime_evse_maint']) / lifetime_residential_energy_kwh
    
    # Calculate single lvl fixed cost for residential charging
    lvl_fixed_costs_res = lvl_fixed_costs_l1 * fraction_home_l1_charging + lvl_fixed_costs_l2 * (1-fraction_home_l1_charging)
    
    # Calculate state-level residential LCOC, write to file
    df['lcoc_cost_per_kwh'] = df['cost_per_kwh'] + lvl_fixed_costs_res
    df = df[['state', 'lcoc_cost_per_kwh']]
    df.to_csv(outfile, index=False)
    nat_lcoc = round(float(df[df.state=='US']['lcoc_cost_per_kwh']), 2)
    print('LCOC calculation complete, national LCOC (residential) is ${}/kWh'.format(nat_lcoc))

###########################
### Workplace/Public L2 ###
###########################

def calculate_state_workplace_public_l2_lcoc(coe_path = config.EIA_COM_PATH,
                                     fixed_costs_file = 'data/fixed-costs/workplace-public-l2/com_level2.txt',
                                     equip_lifespan = 15,
                                     equip_utilization_kwh_per_day = 30, #source: INL
                                     outpath = 'outputs/cost-of-charging/workplace-public-l2/work_pub_l2_states_baseline.csv'):
    """
    Function calculates the state-level workplace/public-L2 levelized cost of charging, taking 
    into account the average cost of electricity, fixed costs, and equipment 
    maintenance.
    """
    
    # Load data
    df = pd.read_csv(coe_path)
    fixed_cost_dict = {}
    with open(fixed_costs_file) as f:
        for line in f:
            key, val = line.split(':')
            fixed_cost_dict[key] = float(val)
            
    ann_maint_cost = 0.01 * fixed_cost_dict['equipment']
    lifetime_maint_cost = ann_maint_cost * equip_lifespan
    fixed_cost_dict['lifetime_evse_maint'] = lifetime_maint_cost
    
    # Calculate lifetime energy output
    lifetime_evse_energy_kwh = equip_lifespan * 365 * equip_utilization_kwh_per_day
    
    # Calculate lvl fixed costs for commercial charging
    lvl_fixed_costs = (fixed_cost_dict['equipment'] + fixed_cost_dict['installation'] \
                       + fixed_cost_dict['lifetime_evse_maint']) / lifetime_evse_energy_kwh
    
    # Calculate state-level workplace/public-L2 LCOC, write to file
    df['cost'] = df['cost'] / 100
    df['lcoc_cost_per_kwh'] = df['cost'] + lvl_fixed_costs
    df.rename(columns={'description': 'state'}, inplace=True)
    df = df[['state', 'lcoc_cost_per_kwh']]
    df.to_csv(outpath, index=False)
    nat_lcoc = round(float(df[df.state=='US']['lcoc_cost_per_kwh']), 2)
    print('LCOC calculation complete, national LCOC (workplace/pub-L2) is ${}/kWh'.format(nat_lcoc))
                   
####################
### DCFC Station ###
####################

def dcfc_rates_to_utils(urdb_rates_files = config.DCFC_PROFILES_DICT,
                       outpath = 'outputs/cost-of-electricity/urdb-dcfc-utilities/'):
    """
    Aggregates dcfc urdb rates in urdb_rates_files by utility, keeping the minimum
    cost of electricity value.
    """
    
    for prof in urdb_rates_files.keys():
        rates_df = pd.read_csv(urdb_rates_files[prof], low_memory=False)
        cost_col = "{}_lvl_cost_per_kwh".format(prof)
        rates_df = rates_df[['eiaid', cost_col]]
        utils_df = rates_df.groupby('eiaid')[cost_col].min().reset_index()
        outfile = outpath + 'dcfc_utils_{}.csv'.format(prof)
        utils_df.to_csv(outfile, index=False)
        print('Utility-level results generated for {}.'.format(prof))

        
def dcfc_utils_to_county(urdb_util_files = {'p1':'outputs/cost-of-electricity/urdb-dcfc-utilities/dcfc_utils_p1.csv',
                                            'p2':'outputs/cost-of-electricity/urdb-dcfc-utilities/dcfc_utils_p2.csv',
                                            'p3':'outputs/cost-of-electricity/urdb-dcfc-utilities/dcfc_utils_p3.csv',
                                            'p4':'outputs/cost-of-electricity/urdb-dcfc-utilities/dcfc_utils_p4.csv'},
                       eia_territory_file = config.EIAID_TO_COUNTY_CW_PATH,
                       outpath = 'outputs/cost-of-electricity/urdb-dcfc-counties/'):
    """
    Joins DCFC cost of electricity for station profiles in urdb_util_files to eia_territory 
    file.
    """
    
    eiaid_territories = pd.read_csv(eia_territory_file)
    eiaid_territories = eiaid_territories[['eiaid', 'state', 'county']]
    
    for prof in urdb_util_files.keys():
        utils_df = pd.read_csv(urdb_util_files[prof], low_memory=False)
        county_df = eiaid_territories.merge(utils_df, on='eiaid', how='left')
        cost_col = "{}_lvl_cost_per_kwh".format(prof)
        county_df = county_df.groupby(['state', 'county'])[cost_col].median().reset_index()
        
        #For counties w/ no utilities in URDB, assign median cost of electricity
        median_coe = county_df[cost_col].median()
        county_df = county_df.fillna(median_coe)
        
        outfile = outpath + 'dcfc_counties_{}.csv'.format(prof)
        county_df.to_csv(outfile, index=False)
        print("County-level results generated for {}.".format(prof))
        

def dcfc_county_to_state(urdb_county_files = {'p1': 'outputs/cost-of-electricity/urdb-dcfc-counties/dcfc_counties_p1.csv',
                                              'p2': 'outputs/cost-of-electricity/urdb-dcfc-counties/dcfc_counties_p2.csv',
                                              'p3': 'outputs/cost-of-electricity/urdb-dcfc-counties/dcfc_counties_p3.csv',
                                              'p4': 'outputs/cost-of-electricity/urdb-dcfc-counties/dcfc_counties_p4.csv'},
                         afdc_counties_file = 'outputs/county-dcfc-counts/afdc_county_station_counts.csv',
                         outpath = 'outputs/cost-of-electricity/dcfc-states/'):
    """
    Function calculates state-level cost of electricity for profiles in urdb_county_files. Cost is
    weighted by the number of DCFC stations present within the county (AFDC).
    """

    afdc_df = pd.read_csv(afdc_counties_file)
    afdc_df.rename(columns={'county_name': 'county'}, inplace=True)
    afdc_df = afdc_df[['state', 'county', 'n_dcfc_stations']]

    for prof in urdb_county_files.keys():
        dcfc_county_df = pd.read_csv(urdb_county_files[prof], low_memory=False)
        dcfc_county_df = dcfc_county_df.merge(afdc_df, on=['state', 'county'], how='left')
        dcfc_county_df = dcfc_county_df.fillna(0)

        states, dcfc_stations, coe = [], [], []
        for state in set(dcfc_county_df['state']):
            state_df = dcfc_county_df[dcfc_county_df['state']==state]
            stations = state_df['n_dcfc_stations'].sum()
            cost_col = "{}_lvl_cost_per_kwh".format(prof)

            if stations > 0:
                cost = (state_df[cost_col] * state_df['n_dcfc_stations']).sum()/stations
            else:
                cost = state_df[cost_col].mean()
            states.append(state)
            dcfc_stations.append(stations)
            coe.append(cost)

        state_df = pd.DataFrame({'state': states,
                                 'n_dcfc_stations': dcfc_stations,
                                 cost_col: coe})
        # Add US row
        total_us_stations = state_df['n_dcfc_stations'].sum()
        nat_coe = ((state_df[cost_col] * state_df['n_dcfc_stations']) / total_us_stations).sum()
        nat_df = pd.DataFrame({'state': ['US'],
                               'n_dcfc_stations': [total_us_stations],
                               cost_col: [nat_coe]})
        state_df = pd.concat([state_df, nat_df]).reset_index(drop=True)
        outfile = outpath + 'dcfc_states_{}.csv'.format(prof)
        state_df.to_csv(outfile, index=False)
        print("State-level results generated for {}.".format(prof))
        
        
def combine_dcfc_profiles_into_single_lcoc(dcfc_lcoc_file = 'outputs/cost-of-charging/dcfc/dcfc_states_baseline.csv',
                                           load_profile_path = config.DCFC_PROFILES_DICT,
                                           afdc_path = config.AFDC_PATH):
    
    """
    Adds 'comb_lcoc' field to dcfc_lcoc_file that is the weighted average of each station profile lcoc. Weighting is by 
    load (total annual power) and how common stations of a similar size are in the real world (using AFDC station locations).
    """

    df = pd.read_csv(dcfc_lcoc_file)

    p1_df = pd.read_csv(load_profile_path['p1'])
    p2_df = pd.read_csv(load_profile_path['p2'])
    p3_df = pd.read_csv(load_profile_path['p3'])
    p4_df = pd.read_csv(load_profile_path['p4'])

    # Weight each profile by total annual load
    ann_p1_kw = p1_df['Power, kW'].sum()
    ann_p2_kw = p2_df['Power, kW'].sum()
    ann_small_station_kw = ann_p1_kw + ann_p2_kw
    ann_p3_kw = p3_df['Power, kW'].sum()
    ann_p4_kw = p4_df['Power, kW'].sum()
    total_kw = ann_p1_kw + ann_p2_kw + ann_p3_kw + ann_p4_kw

    small_station_pwr_wgt = ann_small_station_kw / total_kw
    p3_pwr_wgt = ann_p3_kw / total_kw
    p4_pwr_wgt = ann_p4_kw / total_kw
    
    # Weight each profile by occurances in the AFDC
    stations = afdc.DCFastChargingLocator(afdc_path)
    stations.categorize_by_plugcnt()
    stations_df = stations.station_data
    station_cat_cnts = stations_df['category'].value_counts(normalize=True)
    total_stations = station_cat_cnts.sum()
    
    # Calculate combined profile weights
    small_station_val = small_station_pwr_wgt * station_cat_cnts['s']
    p1_val = (ann_p1_kw/ann_small_station_kw) * small_station_val
    p2_val = (ann_p2_kw/ann_small_station_kw) * small_station_val
    p3_val = p3_pwr_wgt * station_cat_cnts['m']
    p4_val = p4_pwr_wgt * station_cat_cnts['l']
    total_val = p1_val + p2_val + p3_val + p4_val

    p1_wgt = p1_val / total_val
    p2_wgt = p2_val / total_val
    p3_wgt = p3_val / total_val
    p4_wgt = p4_val / total_val
    
    # Add 'comb_lcoc' field to dcfc_lcoc_file
    df['comb_lcoc'] = df['p1_lcoc'] * p1_wgt + df['p2_lcoc'] * p2_wgt + df['p3_lcoc'] * p3_wgt + df['p4_lcoc'] * p4_wgt
    df.to_csv(dcfc_lcoc_file, index=False)
    
    nat_lcoc = round(float(df[df.State=='US']['comb_lcoc']), 2)
    print('LCOC calculation complete, national LCOC (DCFC) is ${}/kWh'.format(nat_lcoc))

#####################
### Combined LCOC ###
#####################

def combine_res_work_dcfc_lcoc(res_lcoc_file = 'outputs/cost-of-charging/residential/res_states_baseline.csv',
                               wrk_lcoc_file = 'outputs/cost-of-charging/workplace-public-l2/work_pub_l2_states_baseline.csv',
                               dcfc_lcoc_file = 'outputs/cost-of-charging/dcfc/dcfc_states_baseline.csv',
                               res_wgt = 0.81,
                               wrk_wgt = 0.14,
                               dcfc_wgt = 0.05,
                               outfile = 'outputs/cost-of-charging/comb/comb_states_baseline.csv'):
    """
    Combines the LCOC for the three EV charging sites by the weights, res_wgt, wrk_gt, dcfc_wgt and outputs
    to outfile.
    """
    
    assert res_wgt + wrk_wgt + dcfc_wgt == 1.0, "Sum of weights must be exactly 1.0!"
    
    # Load data, standardize
    res_df = pd.read_csv(res_lcoc_file)
    res_df.rename(columns={'lcoc_cost_per_kwh': 'res_lcoc'}, inplace=True)
    wrk_df = pd.read_csv(wrk_lcoc_file)
    wrk_df.rename(columns={'lcoc_cost_per_kwh': 'wrk_lcoc'}, inplace=True)
    dcfc_df = pd.read_csv(dcfc_lcoc_file)
    dcfc_df.rename(columns={'State': 'state',
                            'comb_lcoc': 'dcfc_lcoc'}, inplace=True)
    dcfc_df = dcfc_df[['state', 'dcfc_lcoc']]
    
    # Merge datasets
    comb_df = res_df.merge(wrk_df, how='inner', on='state')
    comb_df = comb_df.merge(dcfc_df, how='inner', on='state')
    
    # Calculate comb LCOC, write to file
    comb_df['lcoc_cost_per_kwh'] = comb_df['res_lcoc'] * res_wgt + comb_df['wrk_lcoc'] * wrk_wgt + comb_df['dcfc_lcoc'] * dcfc_wgt
    comb_df = comb_df[['state', 'lcoc_cost_per_kwh']]
    comb_df.to_csv(outfile, index=False)
    nat_lcoc = round(float(comb_df[comb_df.state=='US']['lcoc_cost_per_kwh']), 2)
    print("Combined LCOC calculation complete, national LCOC is ${}/kWh".format(nat_lcoc))
