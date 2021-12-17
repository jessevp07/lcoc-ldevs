import glob
import pandas as pd
import numpy as np
import config
import os
from lcoc import afdc
from pathlib import Path
import xlwings as xw

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

##### Functions #####

##################
### FIPS DATA #####
##################

def state_county_FIPS(fips_state_file_path=os.path.join(config.DATA_PATH,'census_bureau','fips_state.txt'),
                        fips_state_county_file_path=os.path.join(config.DATA_PATH,'census_bureau','national_county.txt')):
    # US Census state and county fips data (downloaded from https://www.census.gov/library/reference/code-lists/ansi.html)
    # County FIPS and name changes documented here: https://www.cdc.gov/nchs/data/nvss/bridged_race/County-Geography-Changes-1990-present.pdf

    # state and state equivalents
    fips_state_dtype = {'STATE':object,'STUSAB':object,'STATE_NAME':object,'STATENS':object}
    fips_state = pd.read_csv(fips_state_file_path,delimiter='|',dtype=fips_state_dtype)
    fips_state.rename(columns={'STATE':'state_fips','STUSAB':'state','STATE_NAME':'state_name','STATENS':'statens'},inplace=True)
    fips_state['state_text'] = 'T' + fips_state.state_fips

    # county and county equivalents
    fips_state_county_dtype = {0:object,1:object,2:object,3:object,4:object}
    fips_state_county = pd.read_csv(fips_state_county_file_path,header=None,dtype=fips_state_county_dtype )
    fips_state_county_header = {0:'state',1:'state_fips',2:'county_fips',3:'county_name',4:'class_fips'}
    fips_state_county.rename(columns=fips_state_county_header,inplace=True)
    fips_state_county.loc[(fips_state_county['state_fips']=='02') & (fips_state_county['county_fips']=='270'),'county_fips'] = '158' # Effective July 1, 2015, Wade Hampton Census Area (FIPS code=02270) was renamed Kusilvak Census Area and assigned a new FIPS code
    fips_state_county.loc[(fips_state_county['state_fips']=='02') & (fips_state_county['county_fips']=='158'),'county_name'] = 'Kusilvak Census Area' # Effective July 1, 2015, Wade Hampton Census Area (FIPS code=02270) was renamed Kusilvak Census Area and assigned a new FIPS code
    fips_state_county.loc[(fips_state_county['state_fips']=='46') & (fips_state_county['county_fips']=='113'),'county_fips'] = '102' # Shannon County, SD renamed and renumbered in 2015
    fips_state_county.loc[(fips_state_county['state_fips']=='46') & (fips_state_county['county_fips']=='102'),'county_name'] = 'Oglala Lakota County' # Shannon County, SD renamed and renumbered in 2015
    fips_state_county = fips_state_county[(fips_state_county['state_fips']!='51') | (fips_state_county['county_fips']!='515')] # Bedford City (51515) merged with Bedford County in 2013, remove from dataset

    # add helper lookup columns
    fips_state_county['state_text'] = ('T' + fips_state_county.state_fips).astype('category')
    fips_state_county['county_text'] = ('T' + fips_state_county.state_fips + fips_state_county.county_fips).astype('category')
    fips_state_county['county'] = fips_state_county['county_name'].str.replace(' County','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' Parish','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' Census Area','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' City and Borough','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' Borough','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' Municipality','')
    fips_state_county['county'] = fips_state_county['county'].str.replace('St. ','St ',regex=True)
    fips_state_county['county'] = fips_state_county['county'].str.replace('Ste. ','Ste ',regex=True)
    fips_state_county['county'] = fips_state_county['county'].str.replace('Ste. ','Ste ',regex=True)
    fips_state_county['county'] = fips_state_county['county'].str.replace("'","")
    fips_state_county['county'] = fips_state_county['county'].str.replace('-','')
    fips_state_county['county'] = fips_state_county['county'].str.replace(' ','')
    fips_state_county['county_lwr'] = fips_state_county['county'].str.lower()

    # filter out territories
    fips_state_county = pd.DataFrame(fips_state_county.query('state != "AS" & state != "GU" & state != "MP" & state != "PR" & state != "UM" & state != "VI"'))
    # merge state name
    fips_state_county = fips_state_county.merge(fips_state[['state_name','state_text']],how='left',on='state_text',validate='many_to_one')
    
    return fips_state_county

##################
### EIA DATA #####
##################

def eia_data(eia_price_yr_c,eia_price_yr_s,aeo_yr,fips_state_county):
    ## Set import parameters
    ### year of data
    eia_price_yr_c = '2019' # c = census
    eia_price_yr_s = '2020' # s = sample
    aeo_yr = '2021'
    # Import raw EIA data
    ### set data types
    eia_prices_dtype = {'Entity':object, 'State':object, 'Customers (Count)':'int64','Sales (Megawatthours)':'float64', 'Revenues (Thousands Dollars)':'float64'}
    ## Import excel files
    print('Loading data.')
    eia_aeo_raw = pd.read_csv(os.path.join(config.DATA_PATH,'eia','15yr-gas-electricity-price-projections',f'eia_aeo{aeo_yr[-2:]}_gas_electricity_price_projections.csv'), skiprows=4)
    eia_res_prices_c_raw = pd.read_excel(os.path.join(config.DATA_PATH,'eia','residential-electricity-prices',f'table6_{eia_price_yr_c[-2:]}.xlsx'),skiprows=2,dtype=eia_prices_dtype)
    eia_com_prices_c_raw = pd.read_excel(os.path.join(config.DATA_PATH,'eia','commercial-electricity-prices',f'table7_{eia_price_yr_c[-2:]}.xlsx'),skiprows=2,dtype=eia_prices_dtype)
    eia_res_prices_s_raw = pd.read_excel(os.path.join(config.DATA_PATH,'eia','residential-electricity-prices',f'table6_{eia_price_yr_s[-2:]}.xlsx'),skiprows=2,dtype=eia_prices_dtype)
    eia_com_prices_s_raw = pd.read_excel(os.path.join(config.DATA_PATH,'eia','commercial-electricity-prices',f'table7_{eia_price_yr_s[-2:]}.xlsx'),skiprows=2,dtype=eia_prices_dtype)
    eiaid_county_terr_c = pd.read_excel(os.path.join(config.DATA_PATH,'eia','eiaid-crosswalk',f'Service_Territory_{eia_price_yr_c}.xlsx'))
    eiaid_county_terr_s = pd.read_excel(os.path.join(config.DATA_PATH,'eia','eiaid-crosswalk',f'Service_Territory_{eia_price_yr_s}.xlsx'))
    eiaid_county_terr_add = pd.read_csv(os.path.join(config.DATA_PATH,'eia','eiaid-to-county','eia_service_territory_addendum.csv'))
    eia_tx_delivery_raw = pd.read_excel(os.path.join(config.DATA_PATH,'eia','TX-delivery-companies',f'Delivery_Companies_{eia_price_yr_s}.xlsx'),skiprows=2,skipfooter=1)
    print('Data loaded.')

    # EIA Data Processing
    ## ANNUAL ENERGY OUTLOOK PRICE CHANGE SCENARIOS ##
    # Rename columns
    eia_aeo = eia_aeo_raw.rename(columns={'Year':'year',
        f'Residential: Electricity: United States: Reference case Indexed to {int(aeo_yr)-1} as percent': 'ref_res_elec_cost_sf',
        f'Residential: Electricity: United States: High oil price Indexed to {int(aeo_yr)-1} as percent': 'highoil_res_elec_cost_sf',
        f'Residential: Electricity: United States: Low oil price Indexed to {int(aeo_yr)-1} as percent': 'lowoil_res_elec_cost_sf',
        f'Transportation: Motor Gasoline: United States: Reference case Indexed to {int(aeo_yr)-1} as percent': 'ref_gas_cost_sf',
        f'Transportation: Motor Gasoline: United States: High oil price Indexed to {int(aeo_yr)-1} as percent': 'highoil_gas_cost_sf',
        f'Transportation: Motor Gasoline: United States: Low oil price Indexed to {int(aeo_yr)-1} as percent': 'lowoil_gas_cost_sf',
        f'Commercial: Electricity: United States: Reference case Indexed to {int(aeo_yr)-1} as percent':'ref_com_elec_cost_sf',
        f'Commercial: Electricity: United States: High oil price Indexed to {int(aeo_yr)-1} as percent':'highoil_com_elec_cost_sf',
        f'Commercial: Electricity: United States: Low oil price Indexed to {int(aeo_yr)-1} as percent':'lowoil_com_elec_cost_sf'})
    eia_aeo = eia_aeo.sort_values('year')
    eia_aeo.reset_index(drop=True,inplace=True)
    eia_aeo_projection_scenarios = ['ref_res_elec_cost_sf','highoil_res_elec_cost_sf','lowoil_res_elec_cost_sf',
                                    'ref_gas_cost_sf','highoil_gas_cost_sf','lowoil_gas_cost_sf',
                                    'ref_com_elec_cost_sf','highoil_com_elec_cost_sf','lowoil_com_elec_cost_sf']
    # Convert from percent to decimal
    for proj_scen in eia_aeo_projection_scenarios:
        eia_aeo[proj_scen] = 1 + eia_aeo[proj_scen] / 100

    print('AEO data processed.')

    ## RESIDENTIAL AND COMMERCIAL PRICE DATA ##
    price_dfs = [eia_res_prices_c_raw,eia_com_prices_c_raw,eia_res_prices_s_raw,eia_com_prices_s_raw]
    for price_df in price_dfs:
        # convert average price column to float
        price_df['Average Price (cents/kWh)'] = pd.to_numeric(price_df['Average Price (cents/kWh)'],errors='coerce')
        # rename columns
        eia_colname_dict = {'Entity':'entity', 'State':'state', 'Ownership':'ownership', 'Customers (Count)':'customers','Sales (Megawatthours)':'sales_mwh', 'Revenues (Thousands Dollars)':'revenue_thousands_dollars','Average Price (cents/kWh)':'avg_price_cents_per_kwh'}
        price_df.rename(columns=eia_colname_dict,inplace=True)
        # filter out state-level adjustments and Behind the Meter entities (no average price)
        adj_yr_c = 'Adjustment ' + eia_price_yr_c
        adj_yr_s = 'Adjustment ' + eia_price_yr_s
        price_df.query('entity != @adj_yr_c & ownership != "Behind the Meter"',inplace=True)

    ## UTILITY-STATE-COUNTY CROSSWALK FILE ##
    # rename columns
    columns_terr={'Data Year':'year', 'Utility Number':'eiaid', 'Utility Name':'entity','Short Form':'short_form', 'State':'state','County':'county'}
    eiaid_county_terr_c.rename(columns=columns_terr,inplace=True)
    eiaid_county_terr_s.rename(columns=columns_terr,inplace=True)
    # create subset with only unique EIAID-entity-state groups
    eiaid_state_terr_c = eiaid_county_terr_c[['eiaid','entity','state']].drop_duplicates()
    eiaid_state_terr_s = eiaid_county_terr_s[['eiaid','entity','state']].drop_duplicates()

    ## TEXAS DELIVERY COMPANY FILE ##
    # create and clean up dataframe
    eia_tx_delivery = eia_tx_delivery_raw.iloc[:,0:8]
    eia_tx_delivery = eia_tx_delivery[['Utility Name','State','Ownership']]
    eia_tx_delivery.rename(columns = {'Utility Name':'entity','State':'state','Ownership':'ownership'}, inplace = True)
    eia_tx_delivery['customers'] = np.nan
    eia_tx_delivery['sales_mwh'] = np.nan
    eia_tx_delivery['revenue_thousands_dollars'] = np.nan

    print(f'{eia_price_yr_c} census EIA residential electricity price data contains {len(eia_res_prices_c_raw)} utility-state groups, {eia_price_yr_s} sample EIA residential electricity price data contains {len(eia_res_prices_s_raw)} utility-state groups')
    print(f'{eia_price_yr_c} census EIA commercial electricity price data contains {len(eia_com_prices_c_raw)} utility-state groups, {eia_price_yr_s} census EIA commercial electricity price data contains {len(eia_com_prices_s_raw)} utility-state groups')

    ## EIA TERRITORY STATE PROCESSING ##
    # 4 possibilities:
    # - No changes = both, _s columns filled in with data
    # - Changed name = both, but _s columns are NAN
    # - No longer on list = left_only
    # - New entity = right_only

    versions = ['_c','_s']

    eiaid_state_terr_update = eiaid_state_terr_c.merge(eiaid_state_terr_s,how='left',on=['eiaid','state'],suffixes=versions,indicator='source_df')
    eiaid_state_terr_update['entity_is_equal'] = eiaid_state_terr_update['entity_c'] == eiaid_state_terr_update['entity_s']
    cond_terr_update = [eiaid_state_terr_update['source_df'] == 'left_only',
                        eiaid_state_terr_update['source_df'] == 'right_only',
                        (eiaid_state_terr_update['source_df'] == 'both') & (eiaid_state_terr_update['entity_is_equal'] == False)]
    out_terr_update = [f'Not in {eia_price_yr_s} list',
                    f'New in {eia_price_yr_s} list',
                    f'Name changed in {eia_price_yr_s} list']
    eiaid_state_terr_update['change_status'] = np.select(cond_terr_update,out_terr_update,'No change')

    ## EIA TERRITORY COUNTY PROCESSING ##
    # Data cleaning
    eiaid_county_terr_s.loc[(eiaid_county_terr_s['state']=='SD') & (eiaid_county_terr_s['county']=='Shannon'),'county'] = 'Oglala Lakota' # Shannon County, SD renamed and renumbered in 2015
    eiaid_county_terr_s.query('county != "Bedford City"',inplace=True) # Bedford City (51515) merged with Bedford County in 2013, remove from dataset
    eiaid_county_terr_s.loc[(eiaid_county_terr_s['state']=='AK') & (eiaid_county_terr_s['county']=='Wade Hampton'),'county'] = 'Kusilvak' # Wade Hampton, AK renamed and renumbered in 2015
    eiaid_county_terr_s.loc[(eiaid_county_terr_s['state']=='AK') & (eiaid_county_terr_s['county']=='Prince of Wales Ketchikan'),'county'] = 'Prince of Wales-Hyder'
    eiaid_county_terr_s.loc[(eiaid_county_terr_s['state']=='AK') & (eiaid_county_terr_s['county']=='Skagway Hoonah Angoon'),'county'] = 'Skagway'
    eiaid_county_terr_s.loc[(eiaid_county_terr_s['state']=='AK') & (eiaid_county_terr_s['county']=='Wrangell Petersburg'),'county'] = 'Wrangell'
    eiaid_county_terr_s['county_lwr'] = eiaid_county_terr_s['county'].str.replace(' ','')
    eiaid_county_terr_s['county_lwr'] = eiaid_county_terr_s['county_lwr'].str.replace('-','')
    eiaid_county_terr_s['county_lwr'] = eiaid_county_terr_s['county_lwr'].str.replace("'","")
    eiaid_county_terr_s['county_lwr'] = eiaid_county_terr_s['county_lwr'].str.lower()
    # Join county lookups
    eiaid_county_terr_s = eiaid_county_terr_s.merge(fips_state_county[['state','county_lwr','county_text']],how='left',on=['county_lwr','state'],validate='many_to_one')
    eiaid_county_terr_s['flag_nomatch'] = eiaid_county_terr_s['state'].isna() # check for counties without matches
    # print(f'Check for counties with no match (True values): {eiaid_county_terr_s["flag_nomatch"].describe()}') # all values false, meaning no missing county matches
    eiaid_county_terr_s.drop(columns=['flag_nomatch','county_lwr'],inplace=True)

    ## UPDATE ENTITIES ##
    print('Updating entities in Census price data with most recent Service Territory list')

    ## RESIDENTIAL ##
    eia_res_prices_c_update1 = eia_res_prices_c_raw.merge(eiaid_state_terr_update,how='left',left_on = ['entity','state'],right_on=['entity_c','state'],indicator=True,validate='one_to_one')
    eia_res_prices_c_update1 = eia_res_prices_c_update1[eia_res_prices_c_update1['change_status']!=f'Not in {eia_price_yr_s} list'] # Filter out entities not in sample year list
    eia_res_prices_c_update1['entity'] = np.where(eia_res_prices_c_update1['change_status']==f'Name changed in {eia_price_yr_s} list',eia_res_prices_c_update1['entity_s'],eia_res_prices_c_update1['entity']) # Update entities with name change
    # print('Entities with name change - Residential:')
    # display(eia_res_prices_c_update1[eia_res_prices_c_update1['change_status']==f'Name changed in {eia_price_yr_s} list'])
    # print('Entities missing from Service Territory list - Residential:')
    # display(eia_res_prices_c_update1[eia_res_prices_c_update1['_merge']!='both']) # entities in missing from the Service Territory df are the Texas REPs/RPMs

    ## COMMERCIAL ##
    eia_com_prices_c_update1 = eia_com_prices_c_raw.merge(eiaid_state_terr_update,how='left',left_on = ['entity','state'],right_on=['entity_c','state'],indicator=True,validate='one_to_one')
    eia_com_prices_c_update1 = eia_com_prices_c_update1[eia_com_prices_c_update1['change_status']!=f'Not in {eia_price_yr_s} list'] # Filter out entities not in sample year list
    eia_com_prices_c_update1['entity'] = np.where(eia_com_prices_c_update1['change_status']==f'Name changed in {eia_price_yr_s} list',eia_com_prices_c_update1['entity_s'],eia_com_prices_c_update1['entity']) # Update entities with name change
    # print('Entities with name change - Commercial:')
    # display(eia_com_prices_c_update1[eia_com_prices_c_update1['change_status']==f'Name changed in {eia_price_yr_s} list'])
    # print('Entities missing from Service Territory list - Commercial:')
    # display(eia_com_prices_c_update1[eia_com_prices_c_update1['_merge']!='both']) # entities in missing from the Service Territory df are the Texas REPs/RPMs

    ## TEXAS ##
    eia_tx_delivery_update1 = eia_tx_delivery.merge(eiaid_state_terr_update,how='left',left_on = ['entity','state'],right_on=['entity_c','state'],indicator=True,validate='one_to_one')
    eia_tx_delivery_update1 = eia_tx_delivery_update1[eia_tx_delivery_update1['change_status']!=f'Not in {eia_price_yr_s} list'] # Filter out entities not in sample year list
    eia_tx_delivery_update1['entity'] = np.where(eia_tx_delivery_update1['change_status']==f'Name changed in {eia_price_yr_s} list',eia_tx_delivery_update1['entity_s'],eia_tx_delivery_update1['entity']) # Update entities with name change
    # print('Entities with name change - Texas:')
    # display(eia_tx_delivery_update1[eia_tx_delivery_update1['change_status']==f'Name changed in {eia_price_yr_s} list'])
    # print('Entities missing from Service Territory list - Texas:')
    # eia_tx_delivery_update1[eia_tx_delivery_update1['_merge']!='both'] # entities in missing from the Service Territory df are the Texas REPs/RPMs

    ## UPDATE CENSUS PRICES WITH SAMPLE PRICES ##
    eia_res_prices_c_update1.drop(columns=['entity_c', 'entity_s', 'source_df', 'entity_is_equal', 'change_status', '_merge'],inplace=True)
    eia_com_prices_c_update1.drop(columns=['entity_c', 'entity_s', 'source_df', 'entity_is_equal', 'change_status', '_merge'],inplace=True)
    columns = ['ownership','customers','sales_mwh','revenue_thousands_dollars','avg_price_cents_per_kwh']

    ## RESIDENTIAL ##
    eia_res_prices_update2 = eia_res_prices_c_update1.merge(eia_res_prices_s_raw,how='left',on=['entity','state'],suffixes=versions,indicator='source_df')
    print(f'Number of residential utilities in sample but not in census data: {len(eia_res_prices_update2[eia_res_prices_update2["source_df"]=="right_only"])}') # check for new utilities

    # update census data with sample prices when available
    for col in columns:
        rows_s = eia_res_prices_update2['source_df'] == 'both'
        eia_res_prices_update2.loc[rows_s,col] = eia_res_prices_update2.loc[rows_s,col+'_s']
        rows_c = eia_res_prices_update2['source_df'] == 'left_only'
        eia_res_prices_update2.loc[rows_c,col] = eia_res_prices_update2.loc[rows_c,col+'_c']
    # drop columns
    for v in versions:
        columns_drop = eia_res_prices_update2.columns[v == eia_res_prices_update2.columns.str[-2:]]
        eia_res_prices_update2.drop(columns=columns_drop,inplace=True)
    eia_res_prices_update2.drop(columns='source_df',inplace=True)

    ## COMMERCIAL ##
    eia_com_prices_update2 = eia_com_prices_c_update1.merge(eia_com_prices_s_raw,how='left',on=['entity','state'],suffixes=versions,indicator='source_df')
    print(f'Number of commercial utilities in sample but not in census data: {len(eia_com_prices_update2[eia_com_prices_update2["source_df"]=="right_only"])}') # check for new utilities

    print('Updating census data with sample prices where applicable')
    for col in columns:
        rows_s = eia_com_prices_update2['source_df'] == 'both'
        eia_com_prices_update2.loc[rows_s,col] = eia_com_prices_update2.loc[rows_s,col+'_s']
        rows_c = eia_com_prices_update2['source_df'] == 'left_only'
        eia_com_prices_update2.loc[rows_c,col] = eia_com_prices_update2.loc[rows_c,col+'_c']
    # drop columns
    for v in versions:
        columns_drop = eia_com_prices_update2.columns[v == eia_com_prices_update2.columns.str[-2:]]
        eia_com_prices_update2.drop(columns=columns_drop,inplace=True)
    eia_com_prices_update2.drop(columns='source_df',inplace=True)

    ## TEXAS CROSSWALK RECONCILIATION ##
    print('Reconciling Texas Retail Energy Providers (REP) / Retail Power Marketer (RPM) and TDU mismatch')
    # note: in 2020, Retail Energy Provider is now called Retail Power Marketer
    eia_tx_delivery_update1.drop(columns=['entity_c', 'entity_s', 'source_df', 'entity_is_equal', 'change_status', '_merge'],inplace=True)
    retail_names = ['Retail Energy Provider','Retail Power Marketer']

    ## RESIDENTIAL ##
    res_retail_filter_keep = ((eia_res_prices_update2['ownership'] == retail_names[0]) | (eia_res_prices_update2['ownership'] == retail_names[1])) & (eia_res_prices_update2['state'] == 'TX')
    res_retail_filter_out = ((eia_res_prices_update2['ownership'] != retail_names[0]) & (eia_res_prices_update2['ownership'] != retail_names[1]))
    # calculate customer-weighted average price for all Texas RPMs
    eia_res_tx_REP = eia_res_prices_update2[res_retail_filter_keep].copy()
    res_avg_price_tx_rep_wtd = ((eia_res_tx_REP['customers'] * eia_res_tx_REP['avg_price_cents_per_kwh']) / eia_res_tx_REP['customers'].sum()).sum()
    # assign customer-weighted average REP price to Texas delivery companies
    eia_tx_delivery_update1['avg_price_cents_per_kwh'] = res_avg_price_tx_rep_wtd
    # update eia_res_prices_raw
    eia_res_prices_norep = eia_res_prices_update2[res_retail_filter_out].copy()
    eia_res_prices_update3 = pd.concat([eia_res_prices_norep,eia_tx_delivery_update1]).reset_index(drop=True)

    ## COMMERCIAL ##
    com_retail_filter_keep = ((eia_com_prices_update2['ownership'] == retail_names[0]) | (eia_com_prices_update2['ownership'] == retail_names[1])) & (eia_com_prices_update2['state'] == 'TX')
    com_retail_filter_out = ((eia_com_prices_update2['ownership'] != retail_names[0]) & (eia_com_prices_update2['ownership'] != retail_names[1]))
    # calculate customer-weighted average price for all Texas RPMs
    eia_com_tx_REP = eia_com_prices_update2[com_retail_filter_keep].copy()
    com_avg_price_tx_rep_wtd = ((eia_com_tx_REP['customers'] * eia_com_tx_REP['avg_price_cents_per_kwh']) / eia_com_tx_REP['customers'].sum()).sum()
    # assign customer-weighted average REP price to Texas delivery companies
    eia_tx_delivery_update1['avg_price_cents_per_kwh'] = com_avg_price_tx_rep_wtd
    # update eia_com_prices_raw
    eia_com_prices_norep = eia_com_prices_update2[com_retail_filter_out].copy()
    eia_com_prices_update3 = pd.concat([eia_com_prices_norep,eia_tx_delivery_update1]).reset_index(drop=True)

    print('Customer-weighted average residential price for Texas RPMs is {} cents per kWh'.format(round(res_avg_price_tx_rep_wtd,2)))
    # print('Updated residential electricity price data contains {} utility-state groups'.format(len(eia_res_prices_update3)))
    print('Customer-weighted average commercial price for Texas RPMs is {} cents per kWh'.format(round(com_avg_price_tx_rep_wtd,2)))
    # print('Updated commercial electricity price data contains {} utility-state groups'.format(len(eia_com_prices_update3)))

    # Finalize data for export

    ## RESIDENTIAL AND COMMERCIAL PRICE DATA ##
    # reorder columns and drop ownership column
    eia_residential = pd.DataFrame(eia_res_prices_update3[['entity', 'state', 'customers', 'sales_mwh','revenue_thousands_dollars', 'avg_price_cents_per_kwh', 'eiaid']])
    eia_commercial = pd.DataFrame(eia_res_prices_update3[['entity', 'state', 'customers', 'sales_mwh','revenue_thousands_dollars', 'avg_price_cents_per_kwh', 'eiaid']])
    # drop NAs
    eia_residential.dropna(subset=['eiaid'],inplace=True)
    eia_commercial.dropna(subset=['eiaid'],inplace=True) # drop NA values (7 utilities with <50 customers in a state that do not have corresponding EIAID)
    # convert EIAID to integer
    eia_residential['eiaid'] = eia_residential['eiaid'].astype('int64') 
    eia_commercial['eiaid'] = eia_commercial['eiaid'].astype('int64')

    ## UTILITY-STATE CROSSWALK FILE ##
    # drop year, short_form, county columns
    eia_crosswalk = pd.DataFrame(eiaid_county_terr_s[['eiaid', 'entity', 'state']].drop_duplicates())

    ## UTILITY-STATE-COUNTY CROSSWALK FILE ##
    # drop year, short_form columns
    eia_to_county = pd.DataFrame(eiaid_county_terr_s[['eiaid', 'entity', 'state', 'county','county_text']])
    eia_to_county = pd.concat([eia_to_county,eiaid_county_terr_add]).reset_index(drop=True)

    print(f'Final residential electricity price data contains {len(eia_residential)} utility-state groups')
    print(f'Final commercial electricity price data contains {len(eia_commercial)} utility-state groups')
    print(f'Final utility-state crosswalk data contains {len(eia_crosswalk)} utility-state groups')
    print(f'Final utility-state-county crosswalk data contains {len(eia_to_county)} utility-state-county groups')

    # UNIQUE COUNTY-COUNTY_TEXT LOOKUP FILE
    eia_county_lookup = eia_to_county[['state','county','county_text']].drop_duplicates()
    eia_us = pd.DataFrame(data={'state':'US','county':'US','county_text':'US'},index=[0])
    eia_county_lookup = pd.concat([eia_county_lookup,eia_us]).reset_index(drop=True)

    # Export files to .csv for use in LCOC calculations
    eia_aeo.to_csv(os.path.join(config.DATA_PATH,'eia','15yr-gas-electricity-price-projections',f'eia_price_scenarios_aeo{aeo_yr[-2:]}.csv'),index=False)
    eia_residential.to_csv(os.path.join(config.DATA_PATH,'eia','residential-electricity-prices',f'eia_residential_{eia_price_yr_s[-2:]}.csv'),index=False)
    eia_commercial.to_csv(os.path.join(config.DATA_PATH,'eia','commercial-electricity-prices',f'eia_commercial_{eia_price_yr_s[-2:]}.csv'),index=False)
    eia_crosswalk.to_csv(os.path.join(config.DATA_PATH,'eia','eiaid-crosswalk',f'eia_crosswalk_{eia_price_yr_s[-2:]}.csv'),index=False)
    eia_to_county.to_csv(os.path.join(config.DATA_PATH,'eia','eiaid-to-county',f'eia_service_territory_{eia_price_yr_s[-2:]}.csv'),index=False)
    eia_county_lookup.to_csv(os.path.join(config.DATA_PATH,'eia','eiaid-to-county',f'eia_county_lookup_{eia_price_yr_s[-2:]}.csv'),index=False)

    print('All data processed and exported to "eia" data folder.')

###################
### Residential ###
###################

def res_rates_to_utils(scenario = 'baseline',
                       urdb_rates_file = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-res-rates','res_rates.csv'),
                       eia_cw_file = config.EIAID_TO_UTILITY_CW_PATH,
                       eia_utils_file = config.EIA_RES_PATH,
                       outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-utilities')):
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
    ## Calculate simple average for Texas TDU counties without customer counts
    eiaid_utils_tx_delivery = eiaid_utils[eiaid_utils['customers'].isna()].groupby(['entity','state']).mean().reset_index()
    ## Calculate customer-weighted average cost per kWh for counties with customer data
    wm = lambda x: np.average(x, weights=eiaid_utils.loc[x.index, "customers"])
    f = {'customers': 'sum', 'eia_cost_per_kwh': wm}
    eiaid_utils = eiaid_utils[~eiaid_utils['customers'].isna()].groupby(['entity', 'state']).agg(f).reset_index()
    #eiaid_utils.columns = eiaid_utils.columns.droplevel(1)
    eiaid_utils = pd.concat([eiaid_utils,eiaid_utils_tx_delivery[['entity','state','customers','eia_cost_per_kwh']]])
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
        eiaid_res_df.to_csv(os.path.join(outpath,'res_utils.csv'), index=False)
    
    # No-TOU - "Business as Usual", EIA averages used (upper bound)
    elif scenario == "no-tou":
        eiaid_res_df['cost_per_kwh'] = eiaid_res_df['eia_cost_per_kwh']
        
        print("Complete, {} utilities represented (no TOU rates used).".format(len(eiaid_res_df)))
        eiaid_res_df.to_csv(os.path.join(outpath,'upper_bnd_res_utils.csv'), index=False)
        
    # TOU-Only - URDB TOU rates only (lower bound)
    elif scenario == "tou-only":
        eiaid_tou_rates_df['cost_per_kwh'] = eiaid_tou_rates_df['offpeak_tou_cost_per_kwh']
        eiaid_tou_rates_df = eiaid_tou_rates_df.merge(eiaid_res_df[['eiaid','entity','state','customers']], how='inner', on='eiaid')
        
        print("Complete, {} utilities represented (only TOU rates used).".format(len(eiaid_tou_rates_df)))
        eiaid_tou_rates_df.to_csv(os.path.join(outpath,'lower_bnd_res_utils.csv'), index=False)
    
    else:
        raise ValueError('scenario not in ["baseline", "no_tou", "tou-only"]')
        
    return eiaid_res_df
              
        
def res_utils_to_state(utils_file = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-utilities','res_utils.csv'),
                       outfile = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-states','res_states_baseline.csv')):
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

def res_utils_to_county_and_state(utils_file = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-utilities','res_utils.csv'),
                        util_county_cw_file = os.path.join(config.DATA_PATH,'eia','eiaid-to-county','eia_service_territory_19.csv'),
                        outfile_county = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-counties','res_counties_baseline.csv'),
                        outfile_state = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-states','res_states_baseline.csv')
                        ):
    """
    Takes utility-level cost of electricity and calculates average county-level
    cost of electricity.
    """
    res_util_df = pd.read_csv(utils_file, low_memory=False)
    util_county_cw_df = pd.read_csv(util_county_cw_file, dtype = {'eiaid':object}, low_memory=False)

    # join utility prices to crosswalk file
    util_county_join = util_county_cw_df.merge(res_util_df[['entity','state','customers','cost_per_kwh']],how='left',on=['entity','state'])

    # group by county, assign median price for counties with more than one utility
    county_df = util_county_join[['county_text','county','state','cost_per_kwh']].groupby(by=['county_text','county','state'],as_index=False).median()
        
    #For counties w/ no utilities, assign state's median cost of electricity
    county_df['cost_per_kwh'] = county_df.groupby(['state'], sort=False)['cost_per_kwh'].apply(lambda x: x.fillna(x.median()))
    
    print('County aggregation complete, {} counties represented'.format(len(county_df)))
    
    # group by state, calculate customer-weighted average price
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
    print('State aggregation complete, {} states (including national average [US]) represented'.format(len(state_df)))

    #Add national estimate
    nat_customers = state_df['customers'].sum()
    nat_cost_per_kwh = ((state_df['cost_per_kwh'] * state_df['customers']) / nat_customers).sum()
    nat_df_county = pd.DataFrame({'county_text':['US'],
                           'state': ['US'],
                           'county':['US'],
                           'cost_per_kwh': [nat_cost_per_kwh]})
    nat_df_state = pd.DataFrame({'state': ['US'],
                           'customers': [nat_customers],
                           'cost_per_kwh': [nat_cost_per_kwh]})
    
    # Export county and state aggregations to CSVs
    county_df = pd.concat([county_df, nat_df_county]).reset_index(drop=True)
    state_df = pd.concat([state_df, nat_df_state]).reset_index(drop=True)
    county_df.to_csv(outfile_county, index=False)
    state_df.to_csv(outfile_state, index=False)
    print('National weighted average complete, national cost of electricity is ${}/kWh.'.format(round(nat_cost_per_kwh,2)))

def calculate_state_residential_lcoc(coe_file = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-states','res_states_baseline.csv'),
                                     fixed_costs_path = os.path.join(config.DATA_PATH,'fixed-costs','residential',''),
                                     annual_maint_frac = 0.01, #Annual cost of maintenance (fraction of equip costs)
                                     veh_lifespan = 15,
                                     veh_kwh_per_100miles = 29.82, #source: EIA
                                     aavmt = 10781, #source: 2017 NHTS
                                     fraction_residential_charging = 0.81, #source: EPRI study
                                     fraction_home_l1_charging = 0.16, #source: EPRI study
                                     dr = 0.035, #source: Mercatus
                                     outfile = os.path.join(config.OUTPUT_PATH,'cost-of-charging','residential','res_states_baseline.csv')):
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

def calculate_county_residential_lcoc(  coe_file = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','res-counties','res_counties_baseline.csv'),
                                        county_fc_df=pd.read_csv(os.path.join(config.HOME_PATH,'outputs','veh_param','county_fuel_consumption.csv')),
                                        lifetime_vmt_df=pd.read_csv(os.path.join(config.HOME_PATH,'outputs','veh_param','lifetime_vmt.csv')),
                                        fixed_costs_path = os.path.join(config.DATA_PATH,'fixed-costs','residential'),
                                        fixed_costs_filenames = ['res_level1.txt', 'res_level2.txt'],
                                        annual_maint_frac = 0.02, #Annual cost of maintenance (fraction of equip costs)
                                        equip_lifespan = 10,
                                        charge_eff = 0.85,
                                        fraction_residential_charging = 0.81, #source: EPRI study
                                        fraction_home_l1_charging = 0.16, #source: EPRI study
                                        dr = 0.035, #source: Mercatus
                                        print_message = True,
                                        outfile = os.path.join(config.OUTPUT_PATH,'cost-of-charging','residential','res_counties_baseline.csv')):

    """
    Function calculates the county-level residential levelized cost of charging, taking 
    into account the average cost of electricity, fixed costs, and equipment 
    maintenance.
    """

    # Load data
    df = pd.read_csv(coe_file)
    filenames = fixed_costs_filenames
    fixed_cost_files = [os.path.join(fixed_costs_path,filenames[0]),os.path.join(fixed_costs_path,filenames[1])]

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

    # process data
    lifetime_energy_df = lifetime_vmt_df.set_index(keys='VEHCLASS')
    county_fc_df = county_fc_df.rename(columns={[x for x in county_fc_df.columns.tolist() if 'FC' in x][0]:'FC'})

    # Calculate lifetime EVSE cost of maintenance
    for plug_typ in fixed_costs.keys():
        for VC in lifetime_energy_df.index.tolist():
            discounted_lifetime_maint_cost = 0
            for i in range(1,equip_lifespan+1):
                ann_maint_cost = annual_maint_frac * fixed_costs[plug_typ]['equipment']
                discounted_ann_maint_cost = ann_maint_cost / (1+dr)**i
                discounted_lifetime_maint_cost += discounted_ann_maint_cost
                lifetime_energy_df.loc[VC,f'{plug_typ}_discounted_lifetime_maint_cost'] = discounted_lifetime_maint_cost

    lifetime_energy_df.reset_index(inplace=True)
    county_fc_life = county_fc_df.merge(lifetime_energy_df,how='left',on=['VEHCLASS'])

    # Calculate county-level residential lifetime energy from residential charging
    county_fc_life['lifetime_energy_kwh'] = county_fc_life['FC'] * county_fc_life['lifetime_vmt'] * (1/charge_eff)
    county_fc_life['lifetime_residential_energy_kwh'] = fraction_residential_charging * county_fc_life.lifetime_energy_kwh
    if fraction_residential_charging > 0:
        # Calculate county-level lvl fixed costs for residential L1, L2 charging
        county_fc_life['lvl_fixed_costs_l1'] = (fixed_costs['L1']['equipment'] + fixed_costs['L1']['installation'] \
        + county_fc_life[f'L1_discounted_lifetime_maint_cost']) / county_fc_life.lifetime_residential_energy_kwh

        county_fc_life['lvl_fixed_costs_l2'] = (fixed_costs['L2']['equipment'] + fixed_costs['L2']['installation'] \
        + county_fc_life[f'L2_discounted_lifetime_maint_cost']) / county_fc_life.lifetime_residential_energy_kwh

        # Calculate county-level single lvl fixed cost for residential charging
        county_fc_life['lvl_fixed_costs_res'] = county_fc_life['lvl_fixed_costs_l1'] * fraction_home_l1_charging + county_fc_life['lvl_fixed_costs_l2'] * (1-fraction_home_l1_charging)
    elif fraction_residential_charging == 0:
        county_fc_life['lvl_fixed_costs_res'] = 0

    # Calculate county-level residential LCOC, write to file
    df = df.merge(county_fc_life,how='left',on='county_text',validate='one_to_many')
    df['lcoc_cost_per_kwh'] = df['cost_per_kwh'] + df['lvl_fixed_costs_res']
    df = df[['county_text','VEHCLASS','county','state','lcoc_cost_per_kwh']]
    nat_lcoc = round(float(df[df.state=='US']['lcoc_cost_per_kwh'].mean()), 2)
    if print_message == True:
        print('County-level LCOC calculation complete, national LCOC (residential) is ${}/kWh'.format(nat_lcoc))
    if outfile != None:
        df.to_csv(outfile, index=False)
        return df
    else:
        return df


###########################
### Workplace/Public L2 ###
###########################

def com_utils_to_county(utils_file = os.path.join(config.DATA_PATH,'eia','commercial-electricity-prices','eia_commercial_19.csv'),
                        util_county_cw_file = os.path.join(config.DATA_PATH,'eia','eiaid-to-county','eia_service_territory_19.csv'),
                        outfile_county = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','com-counties','com_counties_baseline.csv'),
                        outfile_state = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','com-states','com_states_baseline.csv')
                        ):
    """
    Takes utility-level cost of electricity (commercial) and calculates average county-level cost of electricity.
    """
    com_util_df = pd.read_csv(utils_file, low_memory=False)
    util_county_cw_df = pd.read_csv(util_county_cw_file, dtype = {'eiaid':object}, low_memory=False)

    # join utility prices to crosswalk file
    com_util_df.rename(columns={'avg_price_cents_per_kwh':'cost_per_kwh'},inplace=True)
    com_util_df['cost_per_kwh'] = com_util_df['cost_per_kwh'] / 100
    util_county_join = util_county_cw_df.merge(com_util_df[['entity','state','customers','cost_per_kwh']],how='left',on=['entity','state'])

    # group by county, calculate median price for counties with more than one utility
    county_df = util_county_join[['county_text','county','state','cost_per_kwh']].groupby(by=['county_text','county','state'],as_index=False).median()
    # for counties w/ no utilities, assign state's median cost of electricity
    county_df['cost_per_kwh'] = county_df.groupby(['state'], sort=False)['cost_per_kwh'].apply(lambda x: x.fillna(x.median()))
    
    print('County aggregation complete, {} counties represented'.format(len(county_df)))
    
    # group by state, calculate customer-weighted average price
    states, cost_per_kwh, customers = [], [], []
    for state in set(com_util_df['state']):
        temp_df = com_util_df[com_util_df['state'] == state]
        tot_customers = temp_df['customers'].sum()
        wgt_cost = ((temp_df['cost_per_kwh'] * temp_df['customers']) / tot_customers).sum()
        states.append(state)
        customers.append(tot_customers)
        cost_per_kwh.append(wgt_cost)

    state_df = pd.DataFrame({'state': states,
                             'customers': customers,
                             'cost_per_kwh': cost_per_kwh})
    print('State aggregation complete, {} states (including national average [US]) represented'.format(len(state_df)))

    #Add national estimate
    nat_customers = state_df['customers'].sum()
    nat_cost_per_kwh = ((state_df['cost_per_kwh'] * state_df['customers']) / nat_customers).sum()
    nat_df_county = pd.DataFrame({'county_text':['US'],
                           'state': ['US'],
                           'county':['US'],
                           'cost_per_kwh': [nat_cost_per_kwh]})
    nat_df_state = pd.DataFrame({'state': ['US'],
                           'customers': [nat_customers],
                           'cost_per_kwh': [nat_cost_per_kwh]})
    
    # Export county and state aggregations to CSVs
    county_df = pd.concat([county_df, nat_df_county]).reset_index(drop=True)
    state_df = pd.concat([state_df, nat_df_state]).reset_index(drop=True)
    county_df.to_csv(outfile_county, index=False)
    state_df.to_csv(outfile_state, index=False)
    print('National weighted average complete, national commercial cost of electricity is ${}/kWh.'.format(round(nat_cost_per_kwh,2)))

def calculate_county_workplace_public_l2_lcoc(coe_path = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','com-counties','com_counties_baseline.csv'),
                                     fixed_costs_file = os.path.join(config.DATA_PATH,'fixed-costs','workplace-public-l2','com_level2.txt'),
                                     annual_maint_frac = 0.01,
                                     equip_lifespan = 10,
                                     equip_utilization_kwh_per_day = 30, #source: INL
                                     outpath = os.path.join(config.OUTPUT_PATH,'cost-of-charging','workplace-public-l2','work_pub_l2_counties_baseline.csv')):
    """
    Function calculates the county-level workplace/public-L2 levelized cost of charging, taking 
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
            
    ann_maint_cost = annual_maint_frac * fixed_cost_dict['equipment']
    lifetime_maint_cost = ann_maint_cost * equip_lifespan
    fixed_cost_dict['lifetime_evse_maint'] = lifetime_maint_cost
    
    # Calculate lifetime energy output
    lifetime_evse_energy_kwh = equip_lifespan * 365 * equip_utilization_kwh_per_day
    
    # Calculate lvl fixed costs for commercial charging
    lvl_fixed_costs = (fixed_cost_dict['equipment'] + fixed_cost_dict['installation'] \
                       + fixed_cost_dict['lifetime_evse_maint']) / lifetime_evse_energy_kwh
    
    # Calculate county-level workplace/public-L2 LCOC, write to file
    df['lcoc_cost_per_kwh'] = df['cost_per_kwh'] + lvl_fixed_costs
    df = df[['county_text','county','state', 'lcoc_cost_per_kwh']]
    df.to_csv(outpath, index=False)
    nat_lcoc = round(float(df[df.state=='US']['lcoc_cost_per_kwh']), 2)
    print('County-level LCOC calculation complete, national LCOC (workplace/pub-L2) is ${}/kWh'.format(nat_lcoc))

def calculate_state_workplace_public_l2_lcoc(coe_path = config.EIA_COM_PATH,
                                     fixed_costs_file = os.path.join(config.DATA_PATH,'fixed-costs','workplace-public-l2','com_level2.txt'),
                                     equip_lifespan = 15,
                                     equip_utilization_kwh_per_day = 30, #source: INL
                                     outpath = os.path.join(config.OUTPUT_PATH,'cost-of-charging','workplace-public-l2','work_pub_l2_states_baseline.csv')):
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
                        inpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-rates'),
                        outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','')):
    """
    Aggregates dcfc urdb rates in urdb_rates_files by utility, keeping the minimum
    cost of electricity value.
    """
    
    for prof in urdb_rates_files.keys():
        rates_df = pd.read_csv(os.path.join(inpath,'dcfc_rates_{}.csv'.format(prof)), low_memory=False)
        cost_col = "{}_lvl_cost_per_kwh".format(prof)
        rates_df = rates_df[['eiaid', cost_col]]
        utils_df = rates_df.groupby('eiaid')[cost_col].min().reset_index()
        outfile = os.path.join(outpath,'dcfc_utils_{prof}.csv'.format(prof))
        outfolder = Path(outfile).parent.resolve()
        if not os.path.exists(outfolder):
            os.mkdir(outfolder)
        utils_df.to_csv(outfile, index=False)
        print('Utility-level results generated for {}.'.format(prof))

        
def dcfc_utils_to_county(urdb_util_files = {'p1':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p1.csv'),
                                            'p2':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p2.csv'),
                                            'p3':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p3.csv'),
                                            'p4':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p4.csv')},
                       eia_territory_file = config.EIAID_TO_COUNTY_CW_PATH,
                       outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties','')):
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
        
        outfile = os.path.join(outpath,'dcfc_counties_{}.csv'.format(prof))
        county_df.to_csv(outfile, index=False)
        print("County-level results generated for {}.".format(prof))
        

def dcfc_county_to_state(urdb_county_files = {'p1': os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties','dcfc_counties_p1.csv'),
                                              'p2': os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties','dcfc_counties_p2.csv'),
                                              'p3': os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties','dcfc_counties_p3.csv'),
                                              'p4': os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties','dcfc_counties_p4.csv')},
                         afdc_counties_file = os.path.join(config.OUTPUT_PATH,'county-dcfc-counts','afdc_county_station_counts.csv'),
                         outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','dcfc-states')):
    """
    Function calculates state-level cost of electricity for profiles in urdb_county_files. Cost is
    weighted by the number of DCFC stations present within the county (AFDC).
    """

    afdc_df = pd.read_csv(afdc_counties_file)
    afdc_df.rename(columns={'county_name': 'county','State':'state'}, inplace=True)
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
        state_df.to_csv(os.path.join(outpath,f'dcfc_states_{prof}.csv'), index=False)
        print(f"State-level results generated for {prof}.")

def dcfc_utils_to_county_and_state(
        urdb_util_files = { 'p1':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p1.csv'),
                            'p2':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p2.csv'),
                            'p3':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p3.csv'),
                            'p4':os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-utilities','dcfc_utils_p4.csv')},
        eia_territory_file = config.EIAID_TO_COUNTY_CW_PATH,
        outpath_county = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-counties'),
        afdc_counties_file = os.path.join(config.OUTPUT_PATH,'county-dcfc-counts','afdc_county_station_counts.csv'),
        outpath_state = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','dcfc-states')):
    """
    Joins DCFC cost of electricity for station profiles in urdb_util_files to eia_territory 
    file. Function then calculates state-level cost of electricity for profiles in urdb_county_files. Cost is
    weighted by the number of DCFC stations present within the county (AFDC).
    """
    
    eiaid_territories = pd.read_csv(eia_territory_file)
    eiaid_territories = eiaid_territories[['eiaid', 'state', 'county']]

    afdc_df = pd.read_csv(afdc_counties_file)
    afdc_df.rename(columns={'county_name': 'county','State':'state'}, inplace=True)
    afdc_df = afdc_df[['state', 'county', 'n_dcfc_stations']]
    
    for prof in urdb_util_files.keys():
        utils_df = pd.read_csv(urdb_util_files[prof], low_memory=False)
        county_df = eiaid_territories.merge(utils_df, on='eiaid', how='left')
        cost_col = "{}_lvl_cost_per_kwh".format(prof)
        county_df = county_df.groupby(['state', 'county'])[cost_col].median().reset_index()
        
        #For counties w/ no utilities in URDB, assign state's median cost of electricity
        county_df[cost_col] = county_df.groupby(['state'], sort=False)[cost_col].apply(lambda x: x.fillna(x.median()))
        
        print("County-level results generated for {}.".format(prof))
    
        # Group by state, calculate station-weighted average price
        dcfc_county_df = county_df.merge(afdc_df, on=['state', 'county'], how='left')
        dcfc_county_df = dcfc_county_df.fillna(0)

        states, dcfc_stations, coe = [], [], []
        for state in set(dcfc_county_df['state']):
            state_df = dcfc_county_df[dcfc_county_df['state']==state]
            stations = state_df['n_dcfc_stations'].sum()

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
        nat_df_county = pd.DataFrame({'state': ['US'],
                        'county': ['US'],
                        cost_col: [nat_coe]})
        nat_df_state = pd.DataFrame({'state': ['US'],
                        'n_dcfc_stations': [total_us_stations],
                        cost_col: [nat_coe]})
        county_df = pd.concat([county_df, nat_df_county]).reset_index(drop=True)
        state_df = pd.concat([state_df, nat_df_state]).reset_index(drop=True)
        outfile_county = os.path.join(outpath_county,f'dcfc_counties_{prof}.csv')
        outfile_state = os.path.join(outpath_state,f'dcfc_states_{prof}.csv')
        county_df.to_csv(outfile_county, index=False)
        state_df.to_csv(outfile_state, index=False)
        print(f"State-level results generated for {prof}.")

def dcfc_county_to_lcoc_evifast(file_macrobook,file_evi_fast,dcfc_scenarios):
    # Run Macro
    # import xlwings as xw
    with xw.App(add_book=False) as app:
        app.properties(display_alerts=False)
        wb_evi_fast = app.books.open(fullname=file_evi_fast, update_links=False)
        wb = app.books.open(fullname=file_macrobook, update_links=False)
        print(app.books)
        wb.activate()
        macro_refresh_data = wb.macro('Module1.RefreshAllData')
        macro_evi_fast_dcfc = wb.macro('Module1.EVI_FAST_DCFC')
        macro_refresh_data()
        print('Data refreshed.')
        num_scenarios = len(dcfc_scenarios)
        for i in range(1,num_scenarios+1):
            macro_evi_fast_dcfc(dcfc_scenarios[i])
            print(f'EVI-FAST Tool completed for {dcfc_scenarios[i]} ({round((i/num_scenarios)*100,2)}%)')
        wb.save()
        print('EVI-FAST analysis complete for DCFC profiles.')

def dcfc_county_lcoc_results_proc(file_macrobook,outpath):
    # EVI-FAST Results Processing
    dcfc_counties_lcoc_all = pd.read_excel(file_macrobook,sheet_name='dcfc_counties_lcoc')
    scenarios = ['baseline','min','max']
    dcfc_counties_base = pd.DataFrame()
    dcfc_counties_min = pd.DataFrame()
    dcfc_counties_max = pd.DataFrame()
    scen_dfs = [dcfc_counties_base,dcfc_counties_min,dcfc_counties_max]

    for i,scen,scen_df in zip(range(len(scenarios)),scenarios,scen_dfs):
        cols = [x for x in dcfc_counties_lcoc_all.columns.to_list() if scen in x]
        scen_df = dcfc_counties_lcoc_all.reindex(columns=['state','county']+cols)
        for p in ['p1','p2','p3','p4']:
            scen_df.rename(columns={f'{scen}_dcfc_station_{p}_lcoc':f'{p}_lcoc'},inplace=True)
        scen_dfs[i] = scen_df
        scen_dfs[i].to_csv(os.path.join(outpath,f'dcfc_counties_{scen}.csv'),index=False)        
        
def combine_dcfc_profiles_into_single_lcoc(dcfc_lcoc_file = os.path.join(config.OUTPUT_PATH,'cost-of-charging','dcfc','dcfc_states_baseline.csv'),
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

def combine_county_dcfc_profiles_into_single_lcoc(dcfc_lcoc_file = os.path.join(config.OUTPUT_PATH,'cost-of-charging','dcfc','dcfc_counties_baseline.csv'),
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
    
    nat_lcoc = round(float(df[df.county=='US']['comb_lcoc']), 2)
    print('LCOC calculation complete, national LCOC (DCFC) is ${}/kWh'.format(nat_lcoc))

#####################
### Combined LCOC ###
#####################

def combine_res_work_dcfc_lcoc(res_lcoc_file = os.path.join(config.OUTPUT_PATH,'cost-of-charging','residential','res_states_baseline.csv'),
                               wrk_lcoc_file = os.path.join(config.OUTPUT_PATH,'cost-of-charging','workplace-public-l2','work_pub_l2_states_baseline.csv'),
                               dcfc_lcoc_file = os.path.join(config.OUTPUT_PATH,'cost-of-charging','dcfc','dcfc_states_baseline.csv'),
                               res_wgt = 0.81,
                               wrk_wgt = 0.14,
                               dcfc_wgt = 0.05,
                               outfile = os.path.join(config.OUTPUT_PATH,'cost-of-charging','comb','comb_states_baseline.csv')):
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
    dcfc_df.rename(columns={'baseline_dcfc_lcoc': 'dcfc_lcoc'}, inplace=True)
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

def combine_county_res_work_dcfc_lcoc(res_df, # = pd.read_csv(os.path.join(config.HOME_PATH,'outputs','cost-of-charging','residential','res_counties_baseline.csv'))
                               wrk_df, # = pd.read_csv(os.path.join(config.HOME_PATH,'outputs','cost-of-charging','workplace-public-l2','work_pub_l2_counties_baseline.csv'))
                               dcfc_df, # = pd.read_csv(os.path.join(config.HOME_PATH,'outputs','cost-of-charging','dcfc','dcfc_counties_baseline.csv'))
                               eia_county_lookup_df = pd.read_csv(config.EIA_TO_COUNTY_LOOKUP_PATH),
                               res_wgt = 0.81,
                               wrk_wgt = 0.14,
                               dcfc_wgt = 0.05,
                               print_message = True,
                               outpath = os.path.join(config.HOME_PATH,'outputs','cost-of-charging','comb')):
    """
    Combines the county-level LCOC for the three EV charging sites by the weights, res_wgt, wrk_wgt, dcfc_wgt and outputs
    to outfile.
    """
    
    assert res_wgt + wrk_wgt + dcfc_wgt == 1.0, "Sum of weights must be exactly 1.0!"

    # Load data, standardize
    # res_df = pd.read_csv(res_lcoc_file)
    res_df.rename(columns={'lcoc_cost_per_kwh': 'res_lcoc'}, inplace=True)
    # wrk_df = pd.read_csv(wrk_lcoc_file)
    wrk_df.rename(columns={'lcoc_cost_per_kwh': 'wrk_lcoc'}, inplace=True)
    # dcfc_df = pd.read_csv(dcfc_lcoc_file)
    # eia_county_lookup_df = pd.read_csv(eia_county_lookup_file)
    dcfc_df = dcfc_df.merge(eia_county_lookup_df,how='left',on=['state','county']).dropna()
    dcfc_df.rename(columns={'comb_lcoc': 'dcfc_lcoc'}, inplace=True)
    dcfc_df = dcfc_df[['county_text','dcfc_lcoc']]

    # Merge datasets
    comb_df = res_df.merge(wrk_df[['county_text','state','wrk_lcoc']], how='inner', on=['county_text','state'],validate='many_to_one')
    comb_df = comb_df.merge(dcfc_df,how='inner',on='county_text',validate='many_to_one')

    # Calculate comb LCOC, write to file
    comb_df['lcoc_cost_per_kwh'] = comb_df['res_lcoc'] * res_wgt + comb_df['wrk_lcoc'] * wrk_wgt + comb_df['dcfc_lcoc'] * dcfc_wgt
    comb_df = comb_df[['county_text','VEHCLASS','county','state','lcoc_cost_per_kwh']]
    nat_lcoc = round(float(comb_df[comb_df.state=='US']['lcoc_cost_per_kwh'].mean()), 2)
    if print_message == True:
        print("Combined county-level LCOC calculation complete, national LCOC is ${}/kWh".format(nat_lcoc))
    if outpath != None:
        comb_df.to_csv(os.path.join(outpath,'comb_counties_baseline.csv'), index=False)
    else:
        return comb_df