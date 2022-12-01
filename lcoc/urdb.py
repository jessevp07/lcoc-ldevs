"""
DatabaseRates object created from rates downloaded from the URDB, 
https://openei.org.
"""
#public
import sys
import logging
import warnings
import numpy as np
import pandas as pd
import os
import config as config
import lcoc.readwrite as readwrite
import lcoc.helpers as helpers

#settings
pd.options.mode.chained_assignment = None 

class DatabaseRates(object):
    """
    Object for working with data downloaded from NREL's Utility Rate 
    Database (URDB). Rates in the URDB are checked at updated annually by NREL 
    under funding from the U.S. Department of Energy's Solar Energy 
    Technologies Program, in partnership with Illinois State University's 
    Intstitute for Regulatory Policy Studies.

    Attributes
    -----------
    source:
        URL used to download URDB data
    rate_data: 
        pandas.DataFrame where each row represents a unique utility rate,
        unfiltered & unprocessed from the URDB.
    res_rate_data:
        pandas.DataFrame where each row represents a unique residential utility
        rate
    com_rate_data:
        pandas.DataFrame where each row represents a unique commerical utility
        rate
    prev_exists:
        Boolean indicating whether version of dataset has been previously ran
    """

    def __init__(self, urdb_file=None):
        # Download URDB data
        self.source='https://openei.org/apps/USURDB/download/usurdb.csv.gz'
        
        # Load URDB data
        if urdb_file is not None:
            self.rate_data = pd.read_csv(urdb_file, low_memory=False)
        else:
            self.rate_data = readwrite.read_urdb_data(self.source)
            
            # Assign rate_id
            self.rate_data['rate_id'] = list(range(1, len(self.rate_data)+1))
            
            # Save copy of URDB data (if unique) to data/urdb
            self.prev_exists = readwrite.write_urdb_rate_data(self.rate_data)
        
        # Separate residential & commercial rates into separate dfs
        self.res_rate_data = self.rate_data[self.rate_data['sector']=='Residential']
        self.com_rate_data = self.rate_data[self.rate_data['sector'].isin(['Commercial', 'Industrial'])]

    def filter_stale_rates(self, industry):
        """
        Removes rates w/ specified end date, so that only rates without 
        end dates remain (active rates). The industry arg must be "residential" 
        or "commercial".
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
        
        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("Industry must be 'residential' or 'commercial'!")

        df = df[df.enddate.isnull()]

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df     

    def classify_rate_structures(self, industry, ev_rate_words_file=os.path.join('filters','urdb_res_ev_specific_rate_words.txt')):
        """
        Adds five columns to self.res_rate_data and four to self.com_rate_data, 
        ['is_ev_rate' (residential only), is_demand_rate', 'is_tier_rate', '
        is_seasonal_rate', 'is_tou_rate'], that are binary classifiers 
        indicating whether a rate is a demand rate, tier rate, seasonal rate, and/or 
        TOU rate. Note that rates may be combinations of 1+ rate structure. 
        Tier rates and flat rates are mutally exclusive, meaning when 
        'is_tier_rate'==0, it is a flat rate.
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
            
            with open(ev_rate_words_file, 'r') as f:
                filters = f.read().splitlines()
            
            df['is_ev_rate'] = ((df.name.apply(lambda x: helpers.contains_filter_phrases(x, filters)==True))|
            (df.description.apply(lambda x: helpers.contains_filter_phrases(x, filters)))==True).map(int)

        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")
            
        # Classify by rate structure
        is_demand, is_tier, is_seasonal, is_tou = [], [], [], []

        tier_cols = []
        for tier in range(1,11): #period 0
            tier_cols.append('energyratestructure/period0/tier{}rate'.format(tier))

        for tier in range(1,8): #period 1
            tier_cols.append('energyratestructure/period1/tier{}rate'.format(tier))

        for per in range(2,6): #period 2-5
            for tier in range(1,5):
                tier_cols.append('energyratestructure/period{0}/tier{1}rate'.format(per, tier))

        for _, row in df.iterrows():
            # Demand rate check
            if (np.isnan(float(row['flatdemandstructure/period0/tier0rate'])) 
            and np.isnan(float(row['demandratestructure/period0/tier0rate']))):
                is_demand.append(0)
            
            else:
                is_demand.append(1)

            # Tier rate check
            tier_check = int(row[tier_cols].isnull().all()==False)
            is_tier.append(tier_check)

            # Seasonal & TOU rate check
            try:
                year_wkdays = [ls.replace('[', '').replace(',', '').replace(' ', '') for ls in str(row['energyweekdayschedule']).split(']')][:-2]
                year_wknds = [ls.replace('[', '').replace(',', '').replace(' ', '') for ls in str(row['energyweekendschedule']).split(']')][:-2]

                #seasonal
                if (len(set(year_wkdays))>1) or (len(set(year_wknds))>1):
                    seasonal=1
                else:
                    seasonal=0

                is_seasonal.append(seasonal)

                #TOU
                tous =[]
                for wkday_month, wknd_month in zip(year_wkdays, year_wknds):
                    if (len(set(wkday_month))>1) or (len(set(wknd_month))>1):
                        tous.append(1)
                    
                    else:
                        tous.append(0)
                
                if np.array(tous).sum()==0:
                    tou=0
                
                else:
                    tou=1

                is_tou.append(tou)
            
            except:
                is_seasonal.append(np.nan)
                is_tou.append(np.nan)

        df['is_demand_rate'] = is_demand
        df['is_tier_rate'] = is_tier
        df['is_seasonal_rate'] = is_seasonal
        df['is_tou_rate'] = is_tou

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df

    def generate_classification_tree_values(self, industry):
        """
        Returns dictionary of branch name: number of rates for each branch
        in the rate structure classification tree.
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
        
        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")

        class_tree_cnts = {}
        class_tree_cnts['demand'] = len(df[df.is_demand_rate==1])
        class_tree_cnts['no_demand'] = len(df[df.is_demand_rate==0])
        class_tree_cnts['demand/tier'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)])
        class_tree_cnts['demand/fixed'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)])
        class_tree_cnts['no_demand/tier'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)])
        class_tree_cnts['no_demand/fixed'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)])
        class_tree_cnts['demand/tier/seasonal'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)])
        class_tree_cnts['demand/tier/no_seasonal'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)])
        class_tree_cnts['demand/fixed/seasonal'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)])
        class_tree_cnts['demand/fixed/no_seasonal'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)])
        class_tree_cnts['no_demand/tier/seasonal'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)])
        class_tree_cnts['no_demand/tier/no_seasonal'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)])
        class_tree_cnts['no_demand/fixed/seasonal'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)])
        class_tree_cnts['no_demand/fixed/no_seasonal'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)])
        class_tree_cnts['demand/tier/seasonal/tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)&(df.is_tou_rate==1)])
        class_tree_cnts['demand/tier/seasonal/no_tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)&(df.is_tou_rate==0)])
        class_tree_cnts['demand/tier/no_seasonal/tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)&(df.is_tou_rate==1)])
        class_tree_cnts['demand/tier/no_seasonal/no_tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)&(df.is_tou_rate==0)])
        class_tree_cnts['demand/fixed/seasonal/tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)&(df.is_tou_rate==1)])
        class_tree_cnts['demand/fixed/seasonal/no_tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)&(df.is_tou_rate==0)])
        class_tree_cnts['demand/fixed/no_seasonal/tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)&(df.is_tou_rate==1)])
        class_tree_cnts['demand/fixed/no_seasonal/no_tou'] = len(df[(df.is_demand_rate==1)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)&(df.is_tou_rate==0)])
        class_tree_cnts['no_demand/tier/seasonal/tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)&(df.is_tou_rate==1)])
        class_tree_cnts['no_demand/tier/seasonal/no_tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==1)&(df.is_tou_rate==0)])
        class_tree_cnts['no_demand/tier/no_seasonal/tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)&(df.is_tou_rate==1)])
        class_tree_cnts['no_demand/tier/no_seasonal/no_tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==1)&(df.is_seasonal_rate==0)&(df.is_tou_rate==0)])
        class_tree_cnts['no_demand/fixed/seasonal/tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)&(df.is_tou_rate==1)])
        class_tree_cnts['no_demand/fixed/seasonal/no_tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==1)&(df.is_tou_rate==0)])
        class_tree_cnts['no_demand/fixed/no_seasonal/tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)&(df.is_tou_rate==1)])
        class_tree_cnts['no_demand/fixed/no_seasonal/no_tou'] = len(df[(df.is_demand_rate==0)&(df.is_tier_rate==0)&(df.is_seasonal_rate==0)&(df.is_tou_rate==0)])

        return class_tree_cnts
 
    def filter_demand_rates(self, industry):
        """
        Filters rates w/ demand charges.
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
        
        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")

        df = df[df.is_demand_rate==0]

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df

    def filter_on_phrases(self, industry, filters_path=os.path.join(config.HOME_PATH,'filters')):
        """Filters rates on lists of filter phrases: 
        filters/urdb_res_filters.txt for residential rates and
        filters/urdb_dcfc_filters.txt for commercial rates.
        """
        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
            filters_file = os.path.join(filters_path,'urdb_res_filters.txt')
        
        elif industry == 'commercial':
            df = self.com_rate_data
            filters_file = os.path.join(filters_path,'urdb_dcfc_filters.txt')
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")

        with open(filters_file, 'r') as f:
            filters = f.read().splitlines()
        
        filters = pd.Series(filters)
        filters = filters.str.strip()
        filters = filters.to_list()

        df = df[((df.name.apply(lambda x: helpers.contains_filter_phrases(x, filters)))==False)&
        ((df.description.apply(lambda x: helpers.contains_filter_phrases(x, filters)))==False)]

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df

    def additional_com_rate_filters(self):
        """
        Filters commercial rates missing critical fields for approximating the
        cost of electricity.
        """

        df = self.com_rate_data
        
        # Filter rates that don't use kW, kWh
        df = df[(df.demandrateunit == 'kW')|
                (df.flatdemandunit == 'kW')]

        # Filter rates in $/day (fixed charge)
        df = df[df.fixedchargeunits != '$/day']

        # Filter rates w/ min voltages higher than 900 V
        df = df[(df.voltageminimum <= 900)|(df.voltageminimum.isnull())]

        # Filter rates w/ coincident demand structure (can't predict utility peak dmnd)
        df = df[df['coincidentratestructure/period0/tier0rate'].isnull()]

        # Filter rates w/o energy rate information
        df = df[~df['energyratestructure/period0/tier0rate'].isnull()]

        self.com_rate_data = df

    def com_rate_preprocessing(self):
        """
        Standardizes units and reporting for commercial rates.
        """

        df = self.com_rate_data

        # Set: fixed charge = 0 when fixed charge == NULL 
        df['fixedchargefirstmeter'] = df['fixedchargefirstmeter'].fillna(0)
        df['fixedchargeunits'] = df['fixedchargeunits'].fillna('$/month')
        
        # Sun-func for converting to $/month
        def convert_to_dollars_per_month(units, charges):
            monthly_charges = []
            for unit, charge in zip(units, charges):
                
                if unit=='$/month':
                    monthly_charges.append(charge)
                
                elif unit=='$/day':
                    monthly_charges.append(charge*30)
                
                else:
                    raise ValueError('"{0}" unit not recognized'.format(unit))       
            
            return monthly_charges

        df['fixedchargefirstmeter'] = convert_to_dollars_per_month(df['fixedchargeunits'], df['fixedchargefirstmeter'])
        df['fixedchargeunits'] = '$/month'

        # Min demand constraint = 0 when NULL
        df['peakkwcapacitymin'] = df['peakkwcapacitymin'].fillna(0)
        df['peakkwhusagemin'] = df['peakkwhusagemin'].fillna(0)

        # Max demand contraint = inf when NULL
        df['peakkwcapacitymax'] = df['peakkwcapacitymax'].fillna(np.inf)
        df['peakkwhusagemax'] = df['peakkwhusagemax'].fillna(np.inf)

        self.com_rate_data = df

    def combine_rates(self, industry):
        """
        Adds 57 columns to self.res_rate_data and self.com_rate_data that are the 
        sum of the base rate and adjusted rate.
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
        
        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")

        #period 0 (11 tiers)
        df['energyrate/period0/tier0'] = df['energyratestructure/period0/tier0rate'] + df['energyratestructure/period0/tier0adj'].fillna(0)
        df['energyrate/period0/tier1'] = df['energyratestructure/period0/tier1rate'] + df['energyratestructure/period0/tier1adj'].fillna(0)
        df['energyrate/period0/tier2'] = df['energyratestructure/period0/tier2rate'] + df['energyratestructure/period0/tier2adj'].fillna(0)
        df['energyrate/period0/tier3'] = df['energyratestructure/period0/tier3rate'] + df['energyratestructure/period0/tier3adj'].fillna(0)
        df['energyrate/period0/tier4'] = df['energyratestructure/period0/tier4rate'] + df['energyratestructure/period0/tier4adj'].fillna(0)
        df['energyrate/period0/tier5'] = df['energyratestructure/period0/tier5rate'] + df['energyratestructure/period0/tier5adj'].fillna(0)
        df['energyrate/period0/tier6'] = df['energyratestructure/period0/tier6rate'] + df['energyratestructure/period0/tier6adj'].fillna(0)
        df['energyrate/period0/tier7'] = df['energyratestructure/period0/tier7rate'] + df['energyratestructure/period0/tier7adj'].fillna(0)
        df['energyrate/period0/tier8'] = df['energyratestructure/period0/tier8rate'] + df['energyratestructure/period0/tier8adj'].fillna(0)
        df['energyrate/period0/tier9'] = df['energyratestructure/period0/tier9rate'] + df['energyratestructure/period0/tier9adj'].fillna(0)
        df['energyrate/period0/tier10'] = df['energyratestructure/period0/tier10rate'] + df['energyratestructure/period0/tier10adj'].fillna(0)
        
        #period 1 (8 tiers)
        df['energyrate/period1/tier0'] = df['energyratestructure/period1/tier0rate'] + df['energyratestructure/period1/tier0adj'].fillna(0)
        df['energyrate/period1/tier1'] = df['energyratestructure/period1/tier1rate'] + df['energyratestructure/period1/tier1adj'].fillna(0)
        df['energyrate/period1/tier2'] = df['energyratestructure/period1/tier2rate'] + df['energyratestructure/period1/tier2adj'].fillna(0)
        df['energyrate/period1/tier3'] = df['energyratestructure/period1/tier3rate'] + df['energyratestructure/period1/tier3adj'].fillna(0)
        df['energyrate/period1/tier4'] = df['energyratestructure/period1/tier4rate'] + df['energyratestructure/period1/tier4adj'].fillna(0)
        df['energyrate/period1/tier5'] = df['energyratestructure/period1/tier5rate'] + df['energyratestructure/period1/tier5adj'].fillna(0)
        df['energyrate/period1/tier6'] = df['energyratestructure/period1/tier6rate'] + df['energyratestructure/period1/tier6adj'].fillna(0)
        df['energyrate/period1/tier7'] = df['energyratestructure/period1/tier7rate'] + df['energyratestructure/period1/tier7adj'].fillna(0)

        #period 2 (5 tiers)
        df['energyrate/period2/tier0'] = df['energyratestructure/period2/tier0rate'] + df['energyratestructure/period2/tier0adj'].fillna(0)
        df['energyrate/period2/tier1'] = df['energyratestructure/period2/tier1rate'] + df['energyratestructure/period2/tier1adj'].fillna(0)
        df['energyrate/period2/tier2'] = df['energyratestructure/period2/tier2rate'] + df['energyratestructure/period2/tier2adj'].fillna(0)
        df['energyrate/period2/tier3'] = df['energyratestructure/period2/tier3rate'] + df['energyratestructure/period2/tier3adj'].fillna(0)
        df['energyrate/period2/tier4'] = df['energyratestructure/period2/tier4rate'] + df['energyratestructure/period2/tier4adj'].fillna(0)

        #period 3 (5 tiers)
        df['energyrate/period3/tier0'] = df['energyratestructure/period3/tier0rate'] + df['energyratestructure/period3/tier0adj'].fillna(0)
        df['energyrate/period3/tier1'] = df['energyratestructure/period3/tier1rate'] + df['energyratestructure/period3/tier1adj'].fillna(0)
        df['energyrate/period3/tier2'] = df['energyratestructure/period3/tier2rate'] + df['energyratestructure/period3/tier2adj'].fillna(0)
        df['energyrate/period3/tier3'] = df['energyratestructure/period3/tier3rate'] + df['energyratestructure/period3/tier3adj'].fillna(0)
        df['energyrate/period3/tier4'] = df['energyratestructure/period3/tier4rate'] + df['energyratestructure/period3/tier4adj'].fillna(0)

        #period 4 (5 tiers)
        df['energyrate/period4/tier0'] = df['energyratestructure/period4/tier0rate'] + df['energyratestructure/period4/tier0adj'].fillna(0)
        df['energyrate/period4/tier1'] = df['energyratestructure/period4/tier1rate'] + df['energyratestructure/period4/tier1adj'].fillna(0)
        df['energyrate/period4/tier2'] = df['energyratestructure/period4/tier2rate'] + df['energyratestructure/period4/tier2adj'].fillna(0)
        df['energyrate/period4/tier3'] = df['energyratestructure/period4/tier3rate'] + df['energyratestructure/period4/tier3adj'].fillna(0)
        df['energyrate/period4/tier4'] = df['energyratestructure/period4/tier4rate'] + df['energyratestructure/period4/tier4adj'].fillna(0)

        #period 5 (5 tiers)
        df['energyrate/period5/tier0'] = df['energyratestructure/period5/tier0rate'] + df['energyratestructure/period5/tier0adj'].fillna(0)
        df['energyrate/period5/tier1'] = df['energyratestructure/period5/tier1rate'] + df['energyratestructure/period5/tier1adj'].fillna(0)
        df['energyrate/period5/tier2'] = df['energyratestructure/period5/tier2rate'] + df['energyratestructure/period5/tier2adj'].fillna(0)
        df['energyrate/period5/tier3'] = df['energyratestructure/period5/tier3rate'] + df['energyratestructure/period5/tier3adj'].fillna(0)
        df['energyrate/period5/tier4'] = df['energyratestructure/period5/tier4rate'] + df['energyratestructure/period5/tier4adj'].fillna(0)

        #period 6-23
        df['energyrate/period6/tier0'] = df['energyratestructure/period6/tier0rate'] + df['energyratestructure/period6/tier0adj'].fillna(0)
        df['energyrate/period7/tier0'] = df['energyratestructure/period7/tier0rate'] + df['energyratestructure/period7/tier0adj'].fillna(0)
        df['energyrate/period8/tier0'] = df['energyratestructure/period8/tier0rate'] + df['energyratestructure/period8/tier0adj'].fillna(0)
        df['energyrate/period9/tier0'] = df['energyratestructure/period9/tier0rate'] + df['energyratestructure/period9/tier0adj'].fillna(0)
        df['energyrate/period10/tier0'] = df['energyratestructure/period10/tier0rate'] + df['energyratestructure/period10/tier0adj'].fillna(0)
        df['energyrate/period11/tier0'] = df['energyratestructure/period11/tier0rate'] + df['energyratestructure/period11/tier0adj'].fillna(0)
        df['energyrate/period12/tier0'] = df['energyratestructure/period12/tier0rate'] + df['energyratestructure/period12/tier0adj'].fillna(0)
        df['energyrate/period13/tier0'] = df['energyratestructure/period13/tier0rate'] + df['energyratestructure/period13/tier0adj'].fillna(0)
        df['energyrate/period14/tier0'] = df['energyratestructure/period14/tier0rate'] + df['energyratestructure/period14/tier0adj'].fillna(0)
        df['energyrate/period15/tier0'] = df['energyratestructure/period15/tier0rate'] + df['energyratestructure/period15/tier0adj'].fillna(0)
        df['energyrate/period16/tier0'] = df['energyratestructure/period16/tier0rate'] + df['energyratestructure/period16/tier0adj'].fillna(0)
        df['energyrate/period17/tier0'] = df['energyratestructure/period17/tier0rate'] + df['energyratestructure/period17/tier0adj'].fillna(0)
        df['energyrate/period18/tier0'] = df['energyratestructure/period18/tier0rate'] + df['energyratestructure/period18/tier0adj'].fillna(0)
        df['energyrate/period19/tier0'] = df['energyratestructure/period19/tier0rate'] + df['energyratestructure/period19/tier0adj'].fillna(0)
        df['energyrate/period20/tier0'] = df['energyratestructure/period20/tier0rate'] + df['energyratestructure/period20/tier0adj'].fillna(0)
        df['energyrate/period21/tier0'] = df['energyratestructure/period21/tier0rate'] + df['energyratestructure/period21/tier0adj'].fillna(0)
        df['energyrate/period22/tier0'] = df['energyratestructure/period22/tier0rate'] + df['energyratestructure/period22/tier0adj'].fillna(0)
        df['energyrate/period23/tier0'] = df['energyratestructure/period23/tier0rate'] + df['energyratestructure/period23/tier0adj'].fillna(0)

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df

    def filter_null_rates(self, industry):
        """
        Filters rates with no cost information.
        """

        industry = industry.lower()
        if industry == 'residential':
            df = self.res_rate_data
        
        elif industry == 'commercial':
            df = self.com_rate_data
        
        else:
            raise ValueError("industry must be 'residential' or 'commercial'!")

        df = df.dropna(subset=['energyrate/period0/tier0'])

        if industry == 'residential':
            self.res_rate_data = df
        
        elif industry == 'commercial':
            self.com_rate_data = df

    def calculate_annual_energy_cost_residential(self, outpath=os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-res-rates')):
        """
        Calculates the annualized energy costs for residential rates. Estimates 
        account for seasonal, tier, and TOU rate structures. Key assumptions 
        include: 1) Charging occurs with the same freqency irregardless of 
        weekday vs. weekend or season (time of year); 2) Charging occurs with 
        the same frequency across rate tiers; 3) For TOU rates, charging will 
        always occur when it is cheapest to do so (off-peak). Adds 
        'electricity_cost_per_kwh' col to self.res_rate_data.
        """

        # Fixed Rates - incl. seasonal & TOU
        res_rates_fixed = self.res_rate_data[self.res_rate_data.is_tier_rate==0]
        avg_costs = []
        for i in range(len(res_rates_fixed)):
            month_rates = []
            
            #weekday
            for month in [ls.replace('[', '').replace(',', '').replace(' ', '').replace('L','') for ls in str(res_rates_fixed.iloc[i]['energyweekdayschedule']).split(']')][:-2]: #seasonal
                periods = (list(set(month)))
                day_rates = []
                
                for per in periods: #TOU
                    rate_str = 'energyrate/period{}/tier0'.format(per)
                    rate = res_rates_fixed.iloc[i][rate_str]
                    day_rates.append(rate)

                min_day_rate = min(np.array(day_rates))       
                month_rates.extend([min_day_rate]*5)

            #weekend
            for month in [ls.replace('[', '').replace(',', '').replace(' ', '').replace('L','') for ls in str(res_rates_fixed.iloc[i]['energyweekendschedule']).split(']')][:-2]: #seasonal
                periods = (list(set(month)))
                day_rates = []
                
                for per in periods: #TOU
                    rate_str = 'energyrate/period{}/tier0'.format(per)
                    rate = res_rates_fixed.iloc[i][rate_str]
                    day_rates.append(rate)

                min_day_rate = min(np.array(day_rates))       
                month_rates.extend([min_day_rate]*2)

            avg_cost = np.array(month_rates).mean() #dow-weighted cost
            avg_costs.append(avg_cost)
            
        res_rates_fixed['electricity_cost_per_kwh'] = avg_costs

        # Tier Rates - incl. seasonal & TOU
        res_rates_tier = self.res_rate_data[self.res_rate_data.is_tier_rate==1]
        avg_costs = []
        for i in range(len(res_rates_tier)): #tier rate = avg of all tiers
            avg_tier_rates = []
            avg_tier_month_rates = []
            for p in range(24):
                if p==0:
                    tier_rates = []
                    for t in range(11):
                        rate_str = 'energyrate/period{0}/tier{1}'.format(p,t)
                        rate = res_rates_tier.iloc[i][rate_str]
                        tier_rates.append(rate)
                    
                    with warnings.catch_warnings(): #supress warnings
                        warnings.simplefilter("ignore", category=RuntimeWarning)
                        avg_tier_rate = np.nanmean(np.array(tier_rates))  
                    
                    avg_tier_rates.append(avg_tier_rate)

                elif p==1:
                    tier_rates = []
                    for t in range(8):
                        rate_str = 'energyrate/period{0}/tier{1}'.format(p,t)
                        rate = res_rates_tier.iloc[i][rate_str]
                        tier_rates.append(rate)
                    
                    with warnings.catch_warnings(): #supress warnings
                        warnings.simplefilter("ignore", category=RuntimeWarning)
                        avg_tier_rate = np.nanmean(np.array(tier_rates))
    
                    avg_tier_rates.append(avg_tier_rate)

                elif p>=2 and p<6:
                    tier_rates = []
                    for t in range(5):
                        rate_str = 'energyrate/period{0}/tier{1}'.format(p,t)
                        rate = res_rates_tier.iloc[i][rate_str]
                        tier_rates.append(rate)
                    
                    with warnings.catch_warnings(): #supress warnings
                        warnings.simplefilter("ignore", category=RuntimeWarning)
                        avg_tier_rate = np.nanmean(np.array(tier_rates))

                    avg_tier_rates.append(avg_tier_rate)

                else:
                    rate_str = 'energyrate/period{0}/tier0'.format(p)
                    rate = res_rates_tier.iloc[i][rate_str]
                    avg_tier_rates.append(rate)
   

            #weekday rates
            months = [ls.replace('[', '').replace(',', '').replace(' ', '').replace('L','') for ls in str(res_rates_tier.iloc[i]['energyweekdayschedule']).split(']')][:-2] 
            for month in months: #seasonal
                periods = (list(set(month)))
                avg_rates = []
                for per in periods: #TOU
                    per = int(per)
                    avg_tier_rate = avg_tier_rates[per]
                    avg_rates.append(avg_tier_rate)

                min_avg_tier_day_rate = min(np.array(avg_rates))
                avg_tier_month_rates.extend([min_avg_tier_day_rate]*5)

            #weekend rates
            months = [ls.replace('[', '').replace(',', '').replace(' ', '').replace('L','') for ls in str(res_rates_tier.iloc[i]['energyweekendschedule']).split(']')][:-2]
            for month in months:
                periods = (list(set(month)))
                avg_rates = []
                for per in periods:
                    per = int(per)
                    avg_tier_rate = avg_tier_rates[per]
                    avg_rates.append(avg_tier_rate)

                min_avg_tier_day_rate = min(np.array(avg_rates))
                avg_tier_month_rates.extend([min_avg_tier_day_rate]*2)
                
            avg_cost = np.array(avg_tier_month_rates).mean() #dow-weighted cost
            avg_costs.append(avg_cost)

        res_rates_tier['electricity_cost_per_kwh'] = avg_costs
        res_df = pd.concat([res_rates_fixed, res_rates_tier], sort=False)
        res_df = res_df[res_df.electricity_cost_per_kwh>=0]  #remove negative rates
        self.res_rate_data = res_df
        
        self.res_rate_data.to_csv(os.path.join(outpath,'res_rates_v1.csv'), index=False)
        print("Complete, {} rates included.".format(len(self.res_rate_data)))
    
    def calculate_annual_energy_cost_residential_v2(self,outpath=os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-res-rates')):
        """
        V2 = faster version using pandas vectorization
        Calculates the annualized energy costs for residential rates. Estimates 
        account for seasonal, tier, and TOU rate structures. Key assumptions 
        include: 1) Charging occurs with the same frequency regardless of 
        weekday vs. weekend or season (time of year); 2) Charging occurs with 
        the same frequency across rate tiers; 3) For TOU rates, charging will 
        always occur when it is cheapest to do so (off-peak). Adds 
        'electricity_cost_per_kwh' col to self.res_rate_data.
        """

        res_df = self.res_rate_data
        # Create df with deconstructed energy rate structure columns
        cols_energyratestructure = [x for x in res_df.columns.tolist() if 'energyratestructure' in x]
        energyratestructure_key = pd.DataFrame(data={'col_name':cols_energyratestructure})
        energyratestructure_key[['energyratestructure','period','tier']] = energyratestructure_key['col_name'].str.split('/',expand=True)
        energyratestructure_key['period'] = energyratestructure_key['period'].str.replace('period','').astype(int)
        energyratestructure_key['tier'] = energyratestructure_key['tier'].str.replace('tier','')
        energyratestructure_key['type'] = energyratestructure_key['tier']
        energyratestructure_key['tier'] = energyratestructure_key['tier'].str.extract('(\d+)').astype(int)
        energyratestructure_key['type'] = energyratestructure_key['type'].str.replace('\d+','')
        # Create long format df with deconstructed energy rate parameters and corresponding values
        energy_rates_str = res_df.reindex(columns=['label']+cols_energyratestructure)
        energy_rates_str_l = energy_rates_str.melt(id_vars='label',var_name='col_name')
        energy_rates_str_l = energy_rates_str_l.merge(energyratestructure_key,how='left',on='col_name',validate='many_to_one')
        # Create wide format df with deconstructed energy rate parameters and corresponding values
        energy_rates_str_w = energy_rates_str_l.pivot(values='value',index=['label','period','tier'],columns='type').reset_index()
        energy_rates_str_w = energy_rates_str_w.sort_values(by=['label','period','tier'])
        energy_rates_str_w = energy_rates_str_w.rename_axis(None,axis=1)
        energy_rates_str_w = energy_rates_str_w.dropna(how='all',subset=['adj','max','rate','sell']) # drop rows with no data
        energy_rates_str_w['max'] = energy_rates_str_w['max'].fillna(np.inf)
        energy_rates_str_w['rate'] = energy_rates_str_w['rate'].fillna(0)
        energy_rates_str_w['adj'] = energy_rates_str_w['adj'].fillna(0)
        energy_rates_str_w['energy_cost'] = energy_rates_str_w['rate'] + energy_rates_str_w['adj']
        energy_rates_str_w['max'] = energy_rates_str_w['max'].astype('float64')
        energy_rates_str_w.drop(columns=['adj','rate','sell'],inplace=True)
        energy_rates_str_w = energy_rates_str_w.reset_index(drop=True)
        # Calculate average rate for each period (applicable to periods with multiple tiered rates)
        energy_rates_str_avg = energy_rates_str_w[['label','period','energy_cost']].groupby(by=['label','period'],as_index=False).mean()
        # Create long format schedule dfs with label-month-hour index and period value for weekday and weekend schedules
        # weekday
        energy_rates_wkday = res_df.reindex(columns=['label','energyweekdayschedule'])
        energy_rates_wkday['energyweekdayschedule_mod'] = res_df['energyweekdayschedule'].str.replace('L','')
        energy_rates_wkday[list(range(1,13))] = energy_rates_wkday['energyweekdayschedule_mod'].str.split('], ',expand=True)
        energy_rates_wkday_l = energy_rates_wkday.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
        energy_rates_wkday_l['hour_period'] = energy_rates_wkday_l['hour_period'].str.replace('[','')
        energy_rates_wkday_l['hour_period'] = energy_rates_wkday_l['hour_period'].str.replace(']','')
        energy_rates_wkday_l[list(range(0,24))] = energy_rates_wkday_l['hour_period'].str.split(', ',expand=True)
        energy_rates_wkday_l = energy_rates_wkday_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
        energy_rates_wkday_l['day_type'] = 'weekday'
        energy_rates_wkday_l['month'] = energy_rates_wkday_l['month'].astype(int)
        energy_rates_wkday_l['hour'] = energy_rates_wkday_l['hour'].astype(int)
        energy_rates_wkday_l['period'] = energy_rates_wkday_l['period'].astype(int)
        energy_rates_wkday_l = energy_rates_wkday_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
        # weekend
        energy_rates_wkend = res_df.reindex(columns=['label','energyweekendschedule'])
        energy_rates_wkend['energyweekendschedule_mod'] = res_df['energyweekendschedule'].str.replace('L','')
        energy_rates_wkend[list(range(1,13))] = energy_rates_wkend['energyweekendschedule_mod'].str.split('], ',expand=True)
        energy_rates_wkend_l = energy_rates_wkend.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
        energy_rates_wkend_l['hour_period'] = energy_rates_wkend_l['hour_period'].str.replace('[','')
        energy_rates_wkend_l['hour_period'] = energy_rates_wkend_l['hour_period'].str.replace(']','')
        energy_rates_wkend_l[list(range(0,24))] = energy_rates_wkend_l['hour_period'].str.split(', ',expand=True)
        energy_rates_wkend_l = energy_rates_wkend_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
        energy_rates_wkend_l['day_type'] = 'weekend'
        energy_rates_wkend_l['month'] = energy_rates_wkend_l['month'].astype(int)
        energy_rates_wkend_l['hour'] = energy_rates_wkend_l['hour'].astype(int)
        energy_rates_wkend_l['period'] = energy_rates_wkend_l['period'].astype(int)
        energy_rates_wkend_l = energy_rates_wkend_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
        # Concatenate weekday and weekend dfs
        energy_rates_sched_l = pd.concat([energy_rates_wkday_l,energy_rates_wkend_l])
        # Merge schedule and rates
        energy_rates_sched = energy_rates_sched_l.merge(energy_rates_str_avg,how='left',on=['label','period'])
        # Calculate minimum rate for each month and day type
        energy_rates_min = energy_rates_sched[['label','month','day_type','energy_cost']].groupby(by=['label','month','day_type'],as_index=False).min()
        # Weight rates by day type
        energy_rates_min['dow_wgt'] = energy_rates_min['day_type'].map({'weekday':5/7,'weekend':2/7})
        energy_rates_min['energy_cost_wtd'] = energy_rates_min['energy_cost'] * energy_rates_min['dow_wgt']
        # Calculate annual average rate
        energy_rates_month_avg = energy_rates_min[['label','month','energy_cost_wtd']].groupby(by=['label','month'],as_index=False).sum()
        energy_rates_avg = energy_rates_month_avg[['label','energy_cost_wtd']].groupby(by=['label'],as_index=False).mean()
        energy_rates_avg.rename(columns={'energy_cost_wtd':'electricity_cost_per_kwh'},inplace=True)
        # Merge to df
        res_df = res_df.merge(energy_rates_avg,how='left',on='label',validate='one_to_one')
        res_df = res_df[res_df.electricity_cost_per_kwh>=0] # remove negative rates
        self.res_rate_data = res_df

        self.res_rate_data.to_csv(os.path.join(outpath,'res_rates.csv'),index=False)
        print(f'Complete, {len(self.res_rate_data)} rates included.')


    def calculate_annual_cost_dcfc(self, 
                                   dcfc_load_profiles = config.DCFC_PROFILES_DICT,
                                   outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-rates'),
                                   log_lvl = 1):
        """
        Calculates the annualized average daily cost to charge for 
        commercial rates under an annual dcfc_load_profile. Estimates account 
        for demand, seasonal, tier, and TOU rate structures. Due to it's
        significant runtime, function outputs a .csv at outpath for each profile 
        in dcfc_load_profiles. The log_lvl parameter must be in [0,1,2] where higher
        levels reflect more verbose logs.
        """
        
        assert log_lvl in [0,1,2], "Unexpected log_lvl, must be in [0,1,2]"
        
        if log_lvl == 0:
            log_lbl = logging.WARNING
        
        elif log_lvl == 1:
            log_lbl = logging.INFO
        
        elif log_lvl == 2:
            log_lbl = logging.DEBUG
            
        logging.basicConfig(level=log_lbl)
       
        for p in dcfc_load_profiles.keys():
            # Load profile
            profile_path = dcfc_load_profiles[p]
            profile_df = pd.read_csv(profile_path, index_col=0, parse_dates=True)
            
            # Deconstruct timestamp
            months = profile_df.index.month
            days = profile_df.index.day
            hours = profile_df.index.hour
            minutes = profile_df.index.minute
            weekday = profile_df.index.weekday

            # Convert load profile -> energy profile
            energy_profile_df = pd.DataFrame({'month': months, 
                                              'day': days, 
                                              'hour': hours, 
                                              'minute': minutes, 
                                              'weekday': weekday, 
                                              'pwr_kw': profile_df['Power, kW']})
            energy_profile_df = energy_profile_df.sort_values(by=['month', 'day', 'hour', 'minute'])
            energy_profile_df = energy_profile_df.reset_index()
            energy_profile_df['energy_kwh'] = energy_profile_df['pwr_kw']/4
            energy_profile_df.to_csv(os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs','energy_profile_df.csv'))

            # Aggregate 15-min energy profile -> hourly energy profile
            hourly_energy_df = energy_profile_df.groupby(['month', 'day', 'hour', 'weekday'])['energy_kwh'].sum()
            hourly_energy_df = hourly_energy_df.reset_index()
            hourly_energy_df.to_csv(os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs','hourly_energy_df.csv'))

            # Aggregate hourly energy profile -> monthly energy profile
            monthly_energy_df = hourly_energy_df.groupby('month')['energy_kwh'].sum()
            monthly_energy_df = monthly_energy_df.reset_index()
            
            # Calculate peak power by month
            monthly_peak_pwr_df = energy_profile_df.groupby('month')['pwr_kw'].max()
            monthly_peak_pwr_df = monthly_peak_pwr_df.reset_index()

            # Calculate annual energy
            annual_energy_kwh = monthly_energy_df['energy_kwh'].sum()

            # Determine times of peak demand
            peak_demand_times = []
            for month, peak_pwr_kw in zip(range(1,13), monthly_peak_pwr_df['pwr_kw']):
                peak_demand_dow = energy_profile_df[(energy_profile_df.month==month)&\
                                                    (energy_profile_df.pwr_kw==peak_pwr_kw)]['weekday'].values[0]
                peak_demand_hod = energy_profile_df[(energy_profile_df.month==month)&\
                                                    (energy_profile_df.pwr_kw==peak_pwr_kw)]['hour'].values[0]
                peak_demand_time = (peak_demand_dow, peak_demand_hod)
                peak_demand_times.append(peak_demand_time)

            # Filter ineligible rates by peak capacity, energy consumption limits
            def is_eligible(rates, monthly_energy, monthly_peak_pwr):
                eligible = ((rates['peakkwcapacitymin'] <= monthly_peak_pwr.min())&
                (rates['peakkwcapacitymax'] >= monthly_peak_pwr.max())&
                (rates['peakkwhusagemin'] <= monthly_energy.min())&
                (rates['peakkwhusagemax'] >= monthly_energy.max()))
                return eligible

            eligibility = is_eligible(self.com_rate_data, monthly_energy_df['energy_kwh'], monthly_peak_pwr_df['pwr_kw'])

            self.com_rate_data['eligible'] = eligibility
            eligible_rates = self.com_rate_data[self.com_rate_data.eligible==True]
            # print_str = """rates determined to be ineligible for {} (violated peak capacity/energy consumption constraints)""".format(p)
            logging.info(f'{len(self.com_rate_data[self.com_rate_data.eligible==False])} rates determined to be ineligible for {p} (violated peak capacity/energy consumption constraints)')

            eligible_rates.to_csv(os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs','eligible_rates.csv'))
            print('eligible_rates.csv saved')

            ###                             ###
            ## Calculate cost of electricity ##
            ###                             ###

            # Energy rates == 0 if NULL; Max = inf if NULL
            for tier in range(11):
                maxim = 'energyratestructure/period0/tier{}max'.format(tier)
                rate = 'energyratestructure/period0/tier{}rate'.format(tier)
                adj = 'energyratestructure/period0/tier{}adj'.format(tier)
                eligible_rates[maxim] = eligible_rates[maxim].fillna(np.inf)
                eligible_rates[rate] = eligible_rates[rate].fillna(0)
                eligible_rates[adj] = eligible_rates[adj].fillna(0)
                
            for tier in range(8):
                maxim = 'energyratestructure/period1/tier{}max'.format(tier)
                rate = 'energyratestructure/period1/tier{}rate'.format(tier)
                adj = 'energyratestructure/period1/tier{}adj'.format(tier)
                eligible_rates[maxim] = eligible_rates[maxim].fillna(np.inf)
                eligible_rates[rate] = eligible_rates[rate].fillna(0)
                eligible_rates[adj] = eligible_rates[adj].fillna(0)

            for period in range(2,6):
                for tier in range(5):
                    maxim = 'energyratestructure/period{0}/tier{1}max'.format(period, tier)
                    rate = 'energyratestructure/period{0}/tier{1}rate'.format(period, tier)
                    adj = 'energyratestructure/period{0}/tier{1}adj'.format(period, tier)
                    eligible_rates[maxim] = eligible_rates[maxim].fillna(np.inf)
                    eligible_rates[rate] = eligible_rates[rate].fillna(0)
                    eligible_rates[adj] = eligible_rates[adj].fillna(0)
                    
            for period in range(6,24):
                maxim = 'energyratestructure/period{}/tier0max'.format(period)
                rate = 'energyratestructure/period{}/tier0rate'.format(period)
                adj = 'energyratestructure/period{}/tier0adj'.format(period)
                eligible_rates[maxim] = eligible_rates[maxim].fillna(np.inf)
                eligible_rates[rate] = eligible_rates[rate].fillna(0)
                eligible_rates[adj] = eligible_rates[adj].fillna(0)

            # Calculate annual fixed cost charge (1st meter)
            logging.info("Starting annual fixed cost calculations for {}...".format(p))
            eligible_rates['annual_fixed_cost'] = eligible_rates['fixedchargefirstmeter'] * 12
            eligible_rates = eligible_rates[eligible_rates.annual_fixed_cost >= 0]
            logging.info("Annual fixed cost calculations complete.")

            # Characterize rates (demand/no-demand)
            flat_dmd_rates = eligible_rates[~eligible_rates['flatdemandstructure/period0/tier0rate'].isnull()]
            flat_dmd_rates['demand_type'] = 'flat'

            tou_dmd_rates = eligible_rates[(eligible_rates['flatdemandstructure/period0/tier0rate'].isnull())&
                                           (~eligible_rates['demandratestructure/period0/tier0rate'].isnull())]
            tou_dmd_rates['demand_type'] = 'tou'

            no_dmd_rates = eligible_rates[(eligible_rates['flatdemandstructure/period0/tier0rate'].isnull())&
                                          (eligible_rates['demandratestructure/period0/tier0rate'].isnull())]
            no_dmd_rates['demand_type'] = 'none'

            # Demand Charge Rates = 0 when NULL; max = inf when NULL
            for tier in range(17):
                maxim = 'flatdemandstructure/period0/tier{}max'.format(tier)
                rate = 'flatdemandstructure/period0/tier{}rate'.format(tier)
                adj = 'flatdemandstructure/period0/tier{}adj'.format(tier)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)
                
            for tier in range(5):
                maxim = 'flatdemandstructure/period1/tier{}max'.format(tier)
                rate = 'flatdemandstructure/period1/tier{}rate'.format(tier)
                adj = 'flatdemandstructure/period1/tier{}adj'.format(tier)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)
                
            for tier in range(3):
                maxim = 'flatdemandstructure/period2/tier{}max'.format(tier)
                rate = 'flatdemandstructure/period2/tier{}rate'.format(tier)
                adj = 'flatdemandstructure/period2/tier{}adj'.format(tier)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)
                
            for period in range(3,8):
                maxim = 'flatdemandstructure/period{}/tier0max'.format(period)
                rate = 'flatdemandstructure/period{}/tier0rate'.format(period)
                adj = 'flatdemandstructure/period{}/tier0adj'.format(period)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)
                
            for period in range(2):
                for tier in range(16):
                    maxim = 'demandratestructure/period{0}/tier{1}max'.format(period, tier)
                    rate = 'demandratestructure/period{0}/tier{1}rate'.format(period, tier)
                    adj = 'demandratestructure/period{0}/tier{1}adj'.format(period, tier)
                    flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                    tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                    no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                    flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                    tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                    no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                    flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                    tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                    no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)

            for period in range(2, 4):
                for tier in range(3):
                    maxim = 'demandratestructure/period{0}/tier{1}max'.format(period, tier)
                    rate = 'demandratestructure/period{0}/tier{1}rate'.format(period, tier)
                    adj = 'demandratestructure/period{0}/tier{1}adj'.format(period, tier)
                    flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                    tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                    no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                    flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                    tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                    no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                    flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                    tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                    no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)

            for tier in range(2):
                maxim = 'demandratestructure/period4/tier{}max'.format(tier)
                rate = 'demandratestructure/period4/tier{}rate'.format(tier)
                adj = 'demandratestructure/period4/tier{}adj'.format(tier)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)
                    
            for period in range(5,9):
                maxim = 'demandratestructure/period{}/tier0max'.format(period)
                rate = 'demandratestructure/period{}/tier0rate'.format(period)
                adj = 'demandratestructure/period{}/tier0adj'.format(period)
                flat_dmd_rates[maxim] = flat_dmd_rates[maxim].fillna(np.inf)
                tou_dmd_rates[maxim] = tou_dmd_rates[maxim].fillna(np.inf)
                no_dmd_rates[maxim] = no_dmd_rates[maxim].fillna(np.inf)
                flat_dmd_rates[rate] = flat_dmd_rates[rate].fillna(0)
                tou_dmd_rates[rate] = tou_dmd_rates[rate].fillna(0)
                no_dmd_rates[rate] = no_dmd_rates[rate].fillna(0)
                flat_dmd_rates[adj] = flat_dmd_rates[adj].fillna(0)
                tou_dmd_rates[adj] = tou_dmd_rates[adj].fillna(0)
                no_dmd_rates[adj] = no_dmd_rates[adj].fillna(0)

            # Calculate annual demand charges
            logging.info("Starting annual demand cost calculations for {}...".format(p))
            ## Flat-demand rates
            annual_dmd_charges = []
            # flat_dmd_rates.to_csv(os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs','flat_dmd_rates.csv'))
            for i in range(len(flat_dmd_rates)):
                periods = []
                
                # month column labels changed from integer to string

                # for month in range(1,13):
                #     flat_dmd_mnth = "flatdemandmonth{}".format(month)
                #     period = int(flat_dmd_rates.iloc[i][flat_dmd_mnth])
                #     periods.append(period)
                
                month_list = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
                for month in month_list:
                    flat_dmd_mnth = "flatDemandMonth_{}".format(month)
                    period = int(flat_dmd_rates.iloc[i][flat_dmd_mnth])
                    periods.append(period)

                annual_dmd_charge = 0
                for month, period in zip(monthly_peak_pwr_df['month'], periods):
                    peak_pwr = monthly_peak_pwr_df[monthly_peak_pwr_df.month==month]['pwr_kw'].values[0]
                    if period == 0:
                        for tier in range(17):
                            maxim = 'flatdemandstructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'flatdemandstructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'flatdemandstructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = flat_dmd_rates.iloc[i][rate] + flat_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= flat_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue 
                                
                    elif period == 1:
                        for tier in range(5):
                            maxim = 'flatdemandstructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'flatdemandstructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'flatdemandstructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = flat_dmd_rates.iloc[i][rate] + flat_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= flat_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue
                                
                    elif period == 2:
                        for tier in range(3):
                            maxim = 'flatdemandstructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'flatdemandstructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'flatdemandstructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = flat_dmd_rates.iloc[i][rate] + flat_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= flat_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue
                                
                    else:
                        maxim = 'flatdemandstructure/period{0}/tier0max'.format(period)
                        rate = 'flatdemandstructure/period{0}/tier0rate'.format(period)
                        adj = 'flatdemandstructure/period{0}/tier0adj'.format(period)
                        dmd_rate = flat_dmd_rates.iloc[i][rate] + flat_dmd_rates.iloc[i][adj]
                        dmd_charge = peak_pwr * dmd_rate
                        annual_dmd_charge += dmd_charge

                annual_dmd_charges.append(annual_dmd_charge)
                
            flat_dmd_rates['annual_demand_cost'] = annual_dmd_charges
            flat_dmd_rates = flat_dmd_rates[flat_dmd_rates.annual_demand_cost>=0] #remove negative demand costs

            ## TOU-demand rates
            annual_dmd_charges = []
            for i in range(len(tou_dmd_rates)):
                wkday_dmd_tou_periods = []
                wknd_dmd_tou_periods = []
                for monthly_wkday_dmd_tou in tou_dmd_rates['demandweekdayschedule'].iloc[i].replace('L','').replace('[', '').split(']')[:-2]:
                    wkday_dmd_tou_period = monthly_wkday_dmd_tou.replace(' ','').split(',')
                    if len(wkday_dmd_tou_period) == 24:
                        wkday_dmd_tou_periods.append(wkday_dmd_tou_period)
                    
                    elif len(wkday_dmd_tou_period) == 25:
                        wkday_dmd_tou_periods.append(wkday_dmd_tou_period[1:])

                for monthly_wknd_dmd_tou in tou_dmd_rates['demandweekendschedule'].iloc[i].replace('L','').replace('[', '').split(']')[:-2]:
                    wknd_dmd_tou_period = monthly_wknd_dmd_tou.replace(' ','').split(',')
                    if len(wknd_dmd_tou_period) == 24:
                        wknd_dmd_tou_periods.append(wknd_dmd_tou_period)
                    
                    elif len(wknd_dmd_tou_period) == 25:
                        wknd_dmd_tou_periods.append(wknd_dmd_tou_period[1:])

                periods = []
                for month_idx, peak_time in enumerate(peak_demand_times):
                    dow = peak_time[0]
                    hr = peak_time[1]

                    if dow < 5:
                        periods.append(wkday_dmd_tou_periods[month_idx][hr])
                    
                    elif dow in [5,6]:
                        periods.append(wknd_dmd_tou_periods[month_idx][hr])

                annual_dmd_charge = 0
                for month, period in zip(monthly_peak_pwr_df['month'], periods):
                    peak_pwr = monthly_peak_pwr_df[monthly_peak_pwr_df.month==month]['pwr_kw'].values[0]
                        
                    if period in [0,1]:
                        for tier in range(16):
                            maxim = 'demandratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'demandratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'demandratestructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = tou_dmd_rates.iloc[i][rate] + tou_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= tou_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue             
                    
                    elif period in [2,3]:
                        for tier in range(3):
                            maxim = 'demandratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'demandratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'demandratestructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = tou_dmd_rates.iloc[i][rate] + tou_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= tou_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue             
                    
                    elif period == 4:
                        for tier in range(2):
                            maxim = 'demandratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'demandratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'demandratestructure/period{0}/tier{1}adj'.format(period, tier)
                            dmd_rate = tou_dmd_rates.iloc[i][rate] + tou_dmd_rates.iloc[i][adj]
                            dmd_charge = peak_pwr * dmd_rate
                            if peak_pwr <= tou_dmd_rates.iloc[i][maxim]:
                                annual_dmd_charge += dmd_charge
                                break
                            else:
                                continue    
                    
                    else:
                        maxim = 'demandratestructure/period{0}/tier0max'.format(period)
                        rate = 'demandratestructure/period{0}/tier0rate'.format(period)
                        adj = 'demandratestructure/period{0}/tier0adj'.format(period)
                        dmd_rate = tou_dmd_rates.iloc[i][rate] + tou_dmd_rates.iloc[i][adj]
                        dmd_charge = peak_pwr * dmd_rate
                        annual_dmd_charge += dmd_charge
            
                annual_dmd_charges.append(annual_dmd_charge)

            tou_dmd_rates['annual_demand_cost'] = annual_dmd_charges
            tou_dmd_rates = tou_dmd_rates[tou_dmd_rates.annual_demand_cost>=0] #remove negative demand costs

            ## No-demand rates
            no_dmd_rates['annual_demand_cost'] = 0
            logging.info("Annual demand cost calculations complete.")

            eligible_rates = pd.concat([flat_dmd_rates, tou_dmd_rates, no_dmd_rates])
            # eligible_rates.to_csv(os.path.join('C:\\','Users','Jesse Vega-Perkins','Documents','thesis_ev','02_analysis','lcoc-ldevs','eligible_rates.csv'))
            # print('eligible_rates.csv saved')

            # Calculate annual energy charges
            logging.info("Starting annual energy cost calculations for {0} ({1} total)...".format(p, len(eligible_rates)))
            annual_energy_costs = []
            for i in range(len(eligible_rates)):
                if (i % 10 == 0) and (i!=0):
                    logging.info("{0}/{1} rates completed".format(i, len(eligible_rates)))
                wkday_tou_periods = []
                wknd_tou_periods = []
                for monthly_wkday_tou in eligible_rates['energyweekdayschedule'].iloc[i].replace('L','').replace('[', '').split(']')[:-2]: 
                    wkday_tou_period = monthly_wkday_tou.replace(' ','').split(',')
                    if len(wkday_tou_period) == 24:
                        wkday_tou_periods.append(wkday_tou_period)
                    
                    elif len(wkday_tou_period) == 25:
                        wkday_tou_periods.append(wkday_tou_period[1:])

                for monthly_wknd_tou in eligible_rates['energyweekendschedule'].iloc[i].replace('L','').replace('[', '').split(']')[:-2]:
                    wknd_tou_period = monthly_wknd_tou.replace(' ','').split(',')
                    if len(wknd_tou_period) == 24:
                        wknd_tou_periods.append(wknd_tou_period)
                    
                    elif len(wknd_tou_period) == 25:
                        wknd_tou_periods.append(wknd_tou_period[1:])           
                
                periods = []
                for month, dow, hr, energy_kwh in zip(hourly_energy_df['month'], 
                                                      hourly_energy_df['weekday'], 
                                                      hourly_energy_df['hour'],
                                                      hourly_energy_df['energy_kwh']):
                    month_idx = month - 1

                    if dow < 5:
                        periods.append(wkday_tou_periods[month_idx][hr])
                    
                    elif dow in [5,6]:
                        periods.append(wknd_tou_periods[month_idx][hr])
                        
                annual_energy_cost = 0
                prev_month = 1 #init prev month var
                month_energy = 0 #init monthly energy tracking
                for month, period, energy_kwh in zip(hourly_energy_df['month'], periods, hourly_energy_df['energy_kwh']):
                    period = int(period)
                    
                    #update monthly energy
                    if month == prev_month:
                        month_energy += float(energy_kwh)
                    
                    else:
                        month_energy = energy_kwh
                    
                    prev_month = month

                    if period == 0:
                        for tier in range(11):
                            maxim = 'energyratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'energyratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'energyratestructure/period{0}/tier{1}adj'.format(period, tier)
                            energy_cost = eligible_rates.iloc[i][rate] + eligible_rates.iloc[i][adj]
                            hourly_energy_cost = energy_kwh * energy_cost
                            tier_max = float(eligible_rates.iloc[i][maxim])

                            if month_energy <= tier_max:
                                annual_energy_cost += hourly_energy_cost
                                break
                            else:
                                continue       
                    
                    elif period == 1:
                        for tier in range(8):
                            maxim = 'energyratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'energyratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'energyratestructure/period{0}/tier{1}adj'.format(period, tier)
                            energy_cost = eligible_rates.iloc[i][rate] + eligible_rates.iloc[i][adj]
                            hourly_energy_cost = energy_kwh * energy_cost
                            tier_max = float(eligible_rates.iloc[i][maxim])
                            if month_energy <= tier_max:
                                annual_energy_cost += hourly_energy_cost
                                break
                            else:
                                continue           
                    
                    elif period in range(2,6):
                        for tier in range(5):
                            maxim = 'energyratestructure/period{0}/tier{1}max'.format(period, tier)
                            rate = 'energyratestructure/period{0}/tier{1}rate'.format(period, tier)
                            adj = 'energyratestructure/period{0}/tier{1}adj'.format(period, tier)
                            energy_cost = eligible_rates.iloc[i][rate] + eligible_rates.iloc[i][adj]
                            hourly_energy_cost = energy_kwh * energy_cost
                            tier_max = float(eligible_rates.iloc[i][maxim])
                            if month_energy <= tier_max:
                                annual_energy_cost += hourly_energy_cost
                                break
                            else:
                                continue              
                    
                    else:
                        maxim = 'energyratestructure/period{0}/tier0max'.format(period)
                        rate = 'energyratestructure/period{0}/tier0rate'.format(period)
                        adj = 'energyratestructure/period{0}/tier0adj'.format(period)
                        energy_cost = eligible_rates.iloc[i][rate] + eligible_rates.iloc[i][adj]
                        hourly_energy_cost = energy_kwh * energy_cost
                        annual_energy_cost += hourly_energy_cost

                annual_energy_costs.append(annual_energy_cost)

            eligible_rates['annual_energy_cost'] = annual_energy_costs
            eligible_rates = eligible_rates[eligible_rates.annual_energy_cost>=0] #remove negative energy costs
            logging.info(f"{p} - Annual energy cost calculations complete.")

            eligible_rates['annual_cost_total'] = eligible_rates['annual_fixed_cost'] + eligible_rates['annual_demand_cost'] + eligible_rates['annual_energy_cost']
            new_field = f'{p}_lvl_cost_per_kwh'
            eligible_rates[new_field] = eligible_rates['annual_cost_total']/annual_energy_kwh

            eligible_rates.to_csv(os.path.join(outpath,f'dcfc_rates_{p}.csv'), index=False)

    def calculate_annual_cost_dcfc_v2(self, 
                                   dcfc_load_profiles = config.DCFC_PROFILES_DICT,
                                   outpath = os.path.join(config.OUTPUT_PATH,'cost-of-electricity','urdb-dcfc-rates'),
                                   log_lvl = 1):
        """
        Calculates the annualized average daily cost to charge for 
        commercial rates under an annual dcfc_load_profile. Estimates account 
        for demand, seasonal, tier, and TOU rate structures. Due to it's
        significant runtime, function outputs a .csv at outpath for each profile 
        in dcfc_load_profiles. The log_lvl parameter must be in [0,1,2] where higher
        levels reflect more verbose logs.
        """
        
        assert log_lvl in [0,1,2], "Unexpected log_lvl, must be in [0,1,2]"
        
        if log_lvl == 0:
            log_lbl = logging.WARNING
        
        elif log_lvl == 1:
            log_lbl = logging.INFO
        
        elif log_lvl == 2:
            log_lbl = logging.DEBUG
            
        logging.basicConfig(level=log_lbl)
       
        for p in dcfc_load_profiles.keys():
            # Load profile
            profile_path = dcfc_load_profiles[p]
            profile_df = pd.read_csv(profile_path, index_col=0, parse_dates=True)
            
            # Deconstruct timestamp
            months = profile_df.index.month
            days = profile_df.index.day
            hours = profile_df.index.hour
            minutes = profile_df.index.minute
            weekday = profile_df.index.weekday

            # Convert load profile -> energy profile
            energy_profile_df = pd.DataFrame({'month': months, 
                                              'day': days, 
                                              'hour': hours, 
                                              'minute': minutes, 
                                              'weekday': weekday, 
                                              'pwr_kw': profile_df['Power, kW']})
            energy_profile_df = energy_profile_df.sort_values(by=['month', 'day', 'hour', 'minute'])
            energy_profile_df = energy_profile_df.reset_index()
            energy_profile_df['energy_kwh'] = energy_profile_df['pwr_kw']/4

            # Aggregate 15-min energy profile -> hourly energy profile
            hourly_energy_df = energy_profile_df.groupby(['month', 'day', 'hour', 'weekday'])['energy_kwh'].sum()
            hourly_energy_df = hourly_energy_df.reset_index()

            # Aggregate hourly energy profile -> monthly energy profile
            monthly_energy_df = hourly_energy_df.groupby('month')['energy_kwh'].sum()
            monthly_energy_df = monthly_energy_df.reset_index()
            
            # Calculate peak power by month
            monthly_peak_pwr_df = energy_profile_df.groupby('month')['pwr_kw'].max()
            monthly_peak_pwr_df = monthly_peak_pwr_df.reset_index()
            monthly_peak_pwr_df = monthly_peak_pwr_df.merge(energy_profile_df[['month','pwr_kw','hour','weekday']],how='left',on=['month','pwr_kw'])
            monthly_peak_pwr_df['day_type'] = np.where(monthly_peak_pwr_df['weekday']<=4,'weekday','weekend')
            monthly_peak_pwr_df.drop(columns='weekday',inplace=True)

            # Calculate annual energy
            annual_energy_kwh = monthly_energy_df['energy_kwh'].sum()

            # Determine times of peak demand
            peak_demand_times = []
            for month, peak_pwr_kw in zip(range(1,13), monthly_peak_pwr_df['pwr_kw']):
                peak_demand_dow = energy_profile_df[(energy_profile_df.month==month)&\
                                                    (energy_profile_df.pwr_kw==peak_pwr_kw)]['weekday'].values[0]
                peak_demand_hod = energy_profile_df[(energy_profile_df.month==month)&\
                                                    (energy_profile_df.pwr_kw==peak_pwr_kw)]['hour'].values[0]
                peak_demand_time = (peak_demand_dow, peak_demand_hod)
                peak_demand_times.append(peak_demand_time)

            # Filter ineligible rates by peak capacity, energy consumption limits
            def is_eligible(rates, monthly_energy, monthly_peak_pwr):
                eligible = ((rates['peakkwcapacitymin'] <= monthly_peak_pwr.min())&
                (rates['peakkwcapacitymax'] >= monthly_peak_pwr.max())&
                (rates['peakkwhusagemin'] <= monthly_energy.min())&
                (rates['peakkwhusagemax'] >= monthly_energy.max()))
                return eligible

            eligibility = is_eligible(self.com_rate_data, monthly_energy_df['energy_kwh'], monthly_peak_pwr_df['pwr_kw'])

            self.com_rate_data['eligible'] = eligibility
            eligible_rates = self.com_rate_data[self.com_rate_data.eligible==True]
            logging.info(f'{len(self.com_rate_data[self.com_rate_data.eligible==False])} rates determined to be ineligible for {p} (violated peak capacity/energy consumption constraints)')

            ###                             ###
            ## Calculate cost of electricity ##
            ###                             ###

            #----------------------------------------------------------------------------
            ## Calculate annual fixed cost charge (1st meter) ##
            logging.info(f'Starting annual fixed cost calculations for {p}...')
            eligible_rates['annual_fixed_cost'] = eligible_rates['fixedchargefirstmeter'] * 12
            eligible_rates = eligible_rates[eligible_rates.annual_fixed_cost >= 0]
            logging.info("Annual fixed cost calculations complete.")

            #----------------------------------------------------------------------------
            ## Calculate annual demand charges ##

            # Characterize rates (demand/no-demand)
            flat_dmd_rates = eligible_rates[~eligible_rates['flatdemandstructure/period0/tier0rate'].isnull()]
            flat_dmd_rates['demand_type'] = 'flat'

            tou_dmd_rates = eligible_rates[(eligible_rates['flatdemandstructure/period0/tier0rate'].isnull())&
                                            (~eligible_rates['demandratestructure/period0/tier0rate'].isnull())]
            tou_dmd_rates['demand_type'] = 'tou'

            no_dmd_rates = eligible_rates[(eligible_rates['flatdemandstructure/period0/tier0rate'].isnull())&
                                            (eligible_rates['demandratestructure/period0/tier0rate'].isnull())]
            no_dmd_rates['demand_type'] = 'none'

            logging.info(f'Starting annual demand cost calculations for {p}...')

            ## FLAT-DEMAND RATES ##
            # Create df with deconstructed flat demand rate structure columns
            cols_flatdemandstructure = [x for x in flat_dmd_rates.columns.tolist() if 'flatdemandstructure' in x]
            flatdemandstructure_key = pd.DataFrame(data={'col_name':cols_flatdemandstructure})
            flatdemandstructure_key[['flatdemandstructure','period','tier']] = flatdemandstructure_key['col_name'].str.split('/',expand=True)
            flatdemandstructure_key['period'] = flatdemandstructure_key['period'].str.replace('period','').astype(int)
            flatdemandstructure_key['tier'] = flatdemandstructure_key['tier'].str.replace('tier','')
            flatdemandstructure_key['type'] = flatdemandstructure_key['tier']
            flatdemandstructure_key['tier'] = flatdemandstructure_key['tier'].str.extract('(\d+)').astype(int)
            flatdemandstructure_key['type'] = flatdemandstructure_key['type'].str.replace('\d+','')
            # Create long format df with deconstructed flat demand rate parameters and corresponding values
            flat_dmd_rates_str = flat_dmd_rates.reindex(columns=['label']+cols_flatdemandstructure)
            flat_dmd_rates_str_l = flat_dmd_rates_str.melt(id_vars='label',var_name='col_name')
            flat_dmd_rates_str_l = flat_dmd_rates_str_l.merge(flatdemandstructure_key,how='left',on='col_name',validate='many_to_one')
            # Create wide format df with deconstructed flat demand rate parameters and corresponding values
            flat_dmd_rates_str_w = flat_dmd_rates_str_l.pivot(values='value',index=['label','period','tier'],columns='type').reset_index()
            flat_dmd_rates_str_w = flat_dmd_rates_str_w.sort_values(by=['label','period','tier'])
            flat_dmd_rates_str_w = flat_dmd_rates_str_w.rename_axis(None,axis=1)
            flat_dmd_rates_str_w['max'] = flat_dmd_rates_str_w['max'].fillna(np.inf)
            flat_dmd_rates_str_w['rate'] = flat_dmd_rates_str_w['rate'].fillna(0)
            flat_dmd_rates_str_w['adj'] = flat_dmd_rates_str_w['adj'].fillna(0)
            flat_dmd_rates_str_w['dmd_rate'] = flat_dmd_rates_str_w['rate'] + flat_dmd_rates_str_w['adj']
            flat_dmd_rates_str_w['max'] = flat_dmd_rates_str_w['max'].astype('float64')
            flat_dmd_rates_str_w.drop(columns=['adj','rate'],inplace=True)
            flat_dmd_rates_str_w = flat_dmd_rates_str_w.reset_index(drop=True)
            # Create df with flat demand schedule: month with corresponding period
            cols_flatdemand_sched = [x for x in flat_dmd_rates.columns.tolist() if 'flatDemandMonth' in x]
            flat_dmd_rates_sched = flat_dmd_rates.reindex(columns=['label']+cols_flatdemand_sched)
            flat_dmd_rates_sched_l = flat_dmd_rates_sched.melt(id_vars='label',value_vars=cols_flatdemand_sched,var_name='month',value_name='period')
            flat_dmd_rates_sched_l['month_txt'] = flat_dmd_rates_sched_l['month'].str.replace('flatDemandMonth_','')
            dict_month = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6, 'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
            flat_dmd_rates_sched_l['month'] = flat_dmd_rates_sched_l['month_txt'].map(dict_month)
            # Merge monthly_peak_pwr_df to schedule
            flat_dmd_rates_byper_bytier = flat_dmd_rates_sched_l.merge(monthly_peak_pwr_df,how='left',on='month')
            flat_dmd_rates_byper_bytier = flat_dmd_rates_byper_bytier.merge(flat_dmd_rates_str_w,how='left',on=['label','period'])
            flat_dmd_rates_byper_bytier['max_cumulative'] = flat_dmd_rates_byper_bytier.groupby(['label','month','period'])['max'].cumsum()
            # determine excess power above tier threshold and shift down to assign to next tier
            flat_dmd_rates_byper_bytier['dmd_excess'] = np.where(flat_dmd_rates_byper_bytier['pwr_kw'] > flat_dmd_rates_byper_bytier['max_cumulative'], 
                                                                    flat_dmd_rates_byper_bytier['pwr_kw'] - flat_dmd_rates_byper_bytier['max_cumulative'],0)
            flat_dmd_rates_byper_bytier['dmd_excess_shift'] = flat_dmd_rates_byper_bytier.groupby(['label','month','period'])['dmd_excess'].shift()
            # select final power value for each tier based on several conditions
            flat_dmd_rates_byper_bytier['dmd_tier'] = np.select(  condlist=[flat_dmd_rates_byper_bytier['dmd_excess']>0, 
                                                                                    flat_dmd_rates_byper_bytier['dmd_excess_shift']>0,
                                                                                    flat_dmd_rates_byper_bytier['dmd_excess_shift'].isna()],
                                                                                choicelist=[flat_dmd_rates_byper_bytier['max'],
                                                                                    flat_dmd_rates_byper_bytier['dmd_excess_shift'],
                                                                                    flat_dmd_rates_byper_bytier['pwr_kw']],default=0)
            # calculate total flat demand cost
            flat_dmd_rates_byper_bytier['dmd_cost_total'] = flat_dmd_rates_byper_bytier['dmd_rate'] * flat_dmd_rates_byper_bytier['dmd_tier']
            # calculate annual energy cost for each eligible rate and combine into one df
            annual_flat_dmd_cost_df = flat_dmd_rates_byper_bytier[['label','dmd_cost_total']].groupby(by='label').sum()
            annual_flat_dmd_cost_df.rename(columns={'dmd_cost_total':'annual_demand_cost'},inplace=True)
            flat_dmd_rates = flat_dmd_rates.merge(annual_flat_dmd_cost_df,how='left',on='label')
            flat_dmd_rates = flat_dmd_rates[flat_dmd_rates.annual_demand_cost>=0] #remove negative demand costs

            ## TOU-DEMAND RATES ##
            # Create df with deconstructed tou demand rate structure columns
            cols_demandratestructure = [x for x in tou_dmd_rates.columns.tolist() if 'demandratestructure' in x]
            demandratestructure_key = pd.DataFrame(data={'col_name':cols_demandratestructure})
            demandratestructure_key[['demandratestructure','period','tier']] = demandratestructure_key['col_name'].str.split('/',expand=True)
            demandratestructure_key['period'] = demandratestructure_key['period'].str.replace('period','').astype(int)
            demandratestructure_key['tier'] = demandratestructure_key['tier'].str.replace('tier','')
            demandratestructure_key['type'] = demandratestructure_key['tier']
            demandratestructure_key['tier'] = demandratestructure_key['tier'].str.extract('(\d+)').astype(int)
            demandratestructure_key['type'] = demandratestructure_key['type'].str.replace('\d+','')
            # Create long format df with deconstructed tou demand rate parameters and corresponding values
            tou_dmd_rates_str = tou_dmd_rates.reindex(columns=['label']+cols_demandratestructure)
            tou_dmd_rates_str_l = tou_dmd_rates_str.melt(id_vars='label',var_name='col_name')
            tou_dmd_rates_str_l = tou_dmd_rates_str_l.merge(demandratestructure_key,how='left',on='col_name',validate='many_to_one')
            # Create wide format df with deconstructed tou demand rate parameters and corresponding values
            tou_dmd_rates_str_w = tou_dmd_rates_str_l.pivot(values='value',index=['label','period','tier'],columns='type').reset_index()
            tou_dmd_rates_str_w = tou_dmd_rates_str_w.sort_values(by=['label','period','tier'])
            tou_dmd_rates_str_w = tou_dmd_rates_str_w.rename_axis(None,axis=1)
            tou_dmd_rates_str_w['max'] = tou_dmd_rates_str_w['max'].fillna(np.inf)
            tou_dmd_rates_str_w['rate'] = tou_dmd_rates_str_w['rate'].fillna(0)
            tou_dmd_rates_str_w['adj'] = tou_dmd_rates_str_w['adj'].fillna(0)
            tou_dmd_rates_str_w['dmd_rate'] = tou_dmd_rates_str_w['rate'] + tou_dmd_rates_str_w['adj']
            tou_dmd_rates_str_w['max'] = tou_dmd_rates_str_w['max'].astype('float64')
            tou_dmd_rates_str_w.drop(columns=['adj','rate'],inplace=True)
            tou_dmd_rates_str_w = tou_dmd_rates_str_w.reset_index(drop=True)
            # Create long format schedule dfs with label-month-hour index and period value for weekday and weekend schedules
            # weekday
            tou_dmd_rates_wkday = tou_dmd_rates.reindex(columns=['label','demandweekdayschedule'])
            tou_dmd_rates_wkday['demandweekdayschedule_mod'] = tou_dmd_rates['demandweekdayschedule'].str.replace('L','')
            tou_dmd_rates_wkday[list(range(1,13))] = tou_dmd_rates_wkday['demandweekdayschedule_mod'].str.split('], ',expand=True)
            tou_dmd_rates_wkday_l = tou_dmd_rates_wkday.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
            tou_dmd_rates_wkday_l['hour_period'] = tou_dmd_rates_wkday_l['hour_period'].str.replace('[','')
            tou_dmd_rates_wkday_l['hour_period'] = tou_dmd_rates_wkday_l['hour_period'].str.replace(']','')
            tou_dmd_rates_wkday_l[list(range(0,24))] = tou_dmd_rates_wkday_l['hour_period'].str.split(', ',expand=True)
            tou_dmd_rates_wkday_l = tou_dmd_rates_wkday_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
            tou_dmd_rates_wkday_l['day_type'] = 'weekday'
            tou_dmd_rates_wkday_l['month'] = tou_dmd_rates_wkday_l['month'].astype(int)
            tou_dmd_rates_wkday_l['hour'] = tou_dmd_rates_wkday_l['hour'].astype(int)
            tou_dmd_rates_wkday_l['period'] = tou_dmd_rates_wkday_l['period'].astype(int)
            tou_dmd_rates_wkday_l = tou_dmd_rates_wkday_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
            # weekend
            tou_dmd_rates_wkend = tou_dmd_rates.reindex(columns=['label','demandweekendschedule'])
            tou_dmd_rates_wkend['demandweekendschedule_mod'] = tou_dmd_rates['demandweekendschedule'].str.replace('L','')
            tou_dmd_rates_wkend[list(range(1,13))] = tou_dmd_rates_wkend['demandweekendschedule_mod'].str.split('], ',expand=True)
            tou_dmd_rates_wkend_l = tou_dmd_rates_wkend.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
            tou_dmd_rates_wkend_l['hour_period'] = tou_dmd_rates_wkend_l['hour_period'].str.replace('[','')
            tou_dmd_rates_wkend_l['hour_period'] = tou_dmd_rates_wkend_l['hour_period'].str.replace(']','')
            tou_dmd_rates_wkend_l[list(range(0,24))] = tou_dmd_rates_wkend_l['hour_period'].str.split(', ',expand=True)
            tou_dmd_rates_wkend_l = tou_dmd_rates_wkend_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
            tou_dmd_rates_wkend_l['day_type'] = 'weekend'
            tou_dmd_rates_wkend_l['month'] = tou_dmd_rates_wkend_l['month'].astype(int)
            tou_dmd_rates_wkend_l['hour'] = tou_dmd_rates_wkend_l['hour'].astype(int)
            tou_dmd_rates_wkend_l['period'] = tou_dmd_rates_wkend_l['period'].astype(int)
            tou_dmd_rates_wkend_l = tou_dmd_rates_wkend_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
            # Concatenate weekday and weekend dfs
            tou_dmd_rates_sched_l = pd.concat([tou_dmd_rates_wkday_l,tou_dmd_rates_wkend_l])
            # Merge monthly_peak_pwr_df to schedule
            tou_dmd_rates_byper_bytier = tou_dmd_rates_sched_l.merge(monthly_peak_pwr_df,how='left',on=['month','hour','day_type'])
            tou_dmd_rates_byper_bytier = tou_dmd_rates_byper_bytier.dropna(axis=0,subset=['pwr_kw'])
            tou_dmd_rates_byper_bytier = tou_dmd_rates_byper_bytier.merge(tou_dmd_rates_str_w,how='left',on=['label','period'])
            tou_dmd_rates_byper_bytier['max_cumulative'] = tou_dmd_rates_byper_bytier.groupby(['label','month','period'])['max'].cumsum()
            # determine excess power above tier threshold and shift down to assign to next tier
            tou_dmd_rates_byper_bytier['dmd_excess'] = np.where(tou_dmd_rates_byper_bytier['pwr_kw'] > tou_dmd_rates_byper_bytier['max_cumulative'], 
                                                                    tou_dmd_rates_byper_bytier['pwr_kw'] - tou_dmd_rates_byper_bytier['max_cumulative'],0)
            tou_dmd_rates_byper_bytier['dmd_excess_shift'] = tou_dmd_rates_byper_bytier.groupby(['label','month','period'])['dmd_excess'].shift()
            # select final power value for each tier based on several conditions
            tou_dmd_rates_byper_bytier['dmd_tier'] = np.select(  condlist=[tou_dmd_rates_byper_bytier['dmd_excess']>0, 
                                                                                    tou_dmd_rates_byper_bytier['dmd_excess_shift']>0,
                                                                                    tou_dmd_rates_byper_bytier['dmd_excess_shift'].isna()],
                                                                                choicelist=[tou_dmd_rates_byper_bytier['max'],
                                                                                    tou_dmd_rates_byper_bytier['dmd_excess_shift'],
                                                                                    tou_dmd_rates_byper_bytier['pwr_kw']],default=0)
            # calculate total tou demand cost
            tou_dmd_rates_byper_bytier['dmd_cost_total'] = tou_dmd_rates_byper_bytier['dmd_rate'] * tou_dmd_rates_byper_bytier['dmd_tier']
            # calculate annual energy cost for each eligible rate and combine into one df
            annual_tou_dmd_cost_df = tou_dmd_rates_byper_bytier[['label','dmd_cost_total']].groupby(by='label').sum()
            annual_tou_dmd_cost_df.rename(columns={'dmd_cost_total':'annual_demand_cost'},inplace=True)
            # merge annual energy cost to eligible rates
            tou_dmd_rates = tou_dmd_rates.merge(annual_tou_dmd_cost_df,how='left',on='label')
            tou_dmd_rates = tou_dmd_rates[tou_dmd_rates.annual_demand_cost>=0] #remove negative demand costs

            ## NO-DEMAND RATES ##
            no_dmd_rates['annual_demand_cost'] = 0

            logging.info("Annual demand cost calculations complete.")

            eligible_rates = pd.concat([flat_dmd_rates, tou_dmd_rates, no_dmd_rates])

            #----------------------------------------------------------------------------
            ##  Calculate annual energy charges ##
            logging.info(f'Starting annual energy cost calculations for {p}')
            # Create df with deconstructed energy rate structure columns
            cols_energyratestructure = [x for x in eligible_rates.columns.tolist() if 'energyratestructure' in x]
            energyratestructure_key = pd.DataFrame(data={'col_name':cols_energyratestructure})
            energyratestructure_key[['energyratestructure','period','tier']] = energyratestructure_key['col_name'].str.split('/',expand=True)
            energyratestructure_key['period'] = energyratestructure_key['period'].str.replace('period','').astype(int)
            energyratestructure_key['tier'] = energyratestructure_key['tier'].str.replace('tier','')
            energyratestructure_key['type'] = energyratestructure_key['tier']
            energyratestructure_key['tier'] = energyratestructure_key['tier'].str.extract('(\d+)').astype(int)
            energyratestructure_key['type'] = energyratestructure_key['type'].str.replace('\d+','')
            # Create long format df with deconstructed energy rate parameters and corresponding values
            energy_rates_str = eligible_rates.reindex(columns=['label']+cols_energyratestructure)
            energy_rates_str_l = energy_rates_str.melt(id_vars='label',var_name='col_name')
            energy_rates_str_l = energy_rates_str_l.merge(energyratestructure_key,how='left',on='col_name',validate='many_to_one')
            # Create wide format df with deconstructed energy rate parameters and corresponding values
            energy_rates_str_w = energy_rates_str_l.pivot(values='value',index=['label','period','tier'],columns='type').reset_index()
            energy_rates_str_w = energy_rates_str_w.sort_values(by=['label','period','tier'])
            energy_rates_str_w = energy_rates_str_w.rename_axis(None,axis=1)
            energy_rates_str_w = energy_rates_str_w.dropna(how='all',subset=['adj','max','rate','sell']) # drop rows with no data
            energy_rates_str_w['max'] = energy_rates_str_w['max'].fillna(np.inf)
            energy_rates_str_w['rate'] = energy_rates_str_w['rate'].fillna(0)
            energy_rates_str_w['adj'] = energy_rates_str_w['adj'].fillna(0)
            energy_rates_str_w['energy_cost'] = energy_rates_str_w['rate'] + energy_rates_str_w['adj']
            energy_rates_str_w['max'] = energy_rates_str_w['max'].astype('float64')
            energy_rates_str_w.drop(columns=['adj','rate','sell'],inplace=True)
            energy_rates_str_w = energy_rates_str_w.reset_index(drop=True)
            # Create long format schedule dfs with label-month-hour index and period value for weekday and weekend schedules
            # weekday
            energy_rates_wkday = eligible_rates.reindex(columns=['label','energyweekdayschedule'])
            energy_rates_wkday['energyweekdayschedule_mod'] = eligible_rates['energyweekdayschedule'].str.replace('L','')
            energy_rates_wkday[list(range(1,13))] = energy_rates_wkday['energyweekdayschedule_mod'].str.split('], ',expand=True)
            energy_rates_wkday_l = energy_rates_wkday.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
            energy_rates_wkday_l['hour_period'] = energy_rates_wkday_l['hour_period'].str.replace('[','')
            energy_rates_wkday_l['hour_period'] = energy_rates_wkday_l['hour_period'].str.replace(']','')
            energy_rates_wkday_l[list(range(0,24))] = energy_rates_wkday_l['hour_period'].str.split(', ',expand=True)
            energy_rates_wkday_l = energy_rates_wkday_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
            energy_rates_wkday_l['day_type'] = 'weekday'
            energy_rates_wkday_l['month'] = energy_rates_wkday_l['month'].astype(int)
            energy_rates_wkday_l['hour'] = energy_rates_wkday_l['hour'].astype(int)
            energy_rates_wkday_l['period'] = energy_rates_wkday_l['period'].astype(int)
            energy_rates_wkday_l = energy_rates_wkday_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
            # weekend
            energy_rates_wkend = eligible_rates.reindex(columns=['label','energyweekendschedule'])
            energy_rates_wkend['energyweekendschedule_mod'] = eligible_rates['energyweekendschedule'].str.replace('L','')
            energy_rates_wkend[list(range(1,13))] = energy_rates_wkend['energyweekendschedule_mod'].str.split('], ',expand=True)
            energy_rates_wkend_l = energy_rates_wkend.melt(id_vars='label',value_vars=list(range(1,13)),var_name='month',value_name='hour_period')
            energy_rates_wkend_l['hour_period'] = energy_rates_wkend_l['hour_period'].str.replace('[','')
            energy_rates_wkend_l['hour_period'] = energy_rates_wkend_l['hour_period'].str.replace(']','')
            energy_rates_wkend_l[list(range(0,24))] = energy_rates_wkend_l['hour_period'].str.split(', ',expand=True)
            energy_rates_wkend_l = energy_rates_wkend_l.melt(id_vars=['label','month'],value_vars=list(range(0,24)),var_name='hour',value_name='period')
            energy_rates_wkend_l['day_type'] = 'weekend'
            energy_rates_wkend_l['month'] = energy_rates_wkend_l['month'].astype(int)
            energy_rates_wkend_l['hour'] = energy_rates_wkend_l['hour'].astype(int)
            energy_rates_wkend_l['period'] = energy_rates_wkend_l['period'].astype(int)
            energy_rates_wkend_l = energy_rates_wkend_l.sort_values(by=['label','month','hour','period']).reset_index(drop=True)
            # Concatenate weekday and weekend dfs
            energy_rates_sched_l = pd.concat([energy_rates_wkday_l,energy_rates_wkend_l])
            # Process hourly_energy_df
            hourly_energy_df.query('energy_kwh > 0',inplace=True) # drop hours with 0 energy use to reduce dataframe size
            hourly_energy_df['day_type'] = np.where(hourly_energy_df['weekday']<=4,'weekday','weekend') # categorize day type
            # Merge schedules to hourly_energy_df
            hourly_energy_df_sched = hourly_energy_df.merge(energy_rates_sched_l,how='left',on=['month','hour','day_type'])
            hourly_energy_df_sched = hourly_energy_df_sched.sort_values(by=['label','month','period','day','hour'])
            # Max Usage Units == kWh daily
            daily_energy_byper = hourly_energy_df_sched[['label','month','day','period','energy_kwh']].groupby(by=['label','month','day','period']).sum().reset_index()
            daily_energy_byper_bytier = daily_energy_byper.merge(energy_rates_str_w,how='left',on=['label','period'])
            daily_energy_byper_bytier_kWhdaily = daily_energy_byper_bytier.query('unit == "kWh daily"')
            daily_energy_byper_bytier_kWhdaily['max_cumulative'] = daily_energy_byper_bytier_kWhdaily.groupby(['label','month','day','period'])['max'].cumsum()
            daily_energy_byper_bytier_kWhdaily['energy_excess'] = np.where(daily_energy_byper_bytier_kWhdaily['energy_kwh'] > daily_energy_byper_bytier_kWhdaily['max_cumulative'], daily_energy_byper_bytier_kWhdaily['energy_kwh'] - daily_energy_byper_bytier_kWhdaily['max_cumulative'],0)
            daily_energy_byper_bytier_kWhdaily['energy_excess_shift'] = daily_energy_byper_bytier_kWhdaily.groupby(['label','month','day','period'])['energy_excess'].shift()
            daily_energy_byper_bytier_kWhdaily['energy_tier'] = np.select(  condlist=[daily_energy_byper_bytier_kWhdaily['energy_excess']>0, 
                                                                                daily_energy_byper_bytier_kWhdaily['energy_excess_shift']>0,
                                                                                daily_energy_byper_bytier_kWhdaily['energy_excess_shift'].isna()],
                                                                            choicelist=[daily_energy_byper_bytier_kWhdaily['max'],
                                                                                daily_energy_byper_bytier_kWhdaily['energy_excess_shift'],
                                                                                daily_energy_byper_bytier_kWhdaily['energy_kwh']],default=0)
            daily_energy_byper_bytier_kWhdaily['energy_cost_total'] = daily_energy_byper_bytier_kWhdaily['energy_cost'] * daily_energy_byper_bytier_kWhdaily['energy_tier']
            # Max Usage Units == kWh or kWh/kW
            monthly_energy_byper = hourly_energy_df_sched[['label','month','period','energy_kwh']].groupby(by=['label','month','period']).sum().reset_index()
            monthly_energy_byper_bytier = monthly_energy_byper.merge(energy_rates_str_w,how='left',on=['label','period'])
            monthly_energy_byper_bytier = monthly_energy_byper_bytier.merge(monthly_peak_pwr_df,how='left',on='month')
            monthly_energy_byper_bytier_nokWhdaily = monthly_energy_byper_bytier.query('unit != "kWh daily"')
            # determine final tier threshold (max_final) based on Max Usage units
            monthly_energy_byper_bytier_nokWhdaily['max_final'] = np.where(monthly_energy_byper_bytier_nokWhdaily['unit']=='kWh/kW',
                                                                            monthly_energy_byper_bytier_nokWhdaily['max']*monthly_energy_byper_bytier_nokWhdaily['pwr_kw'],
                                                                            monthly_energy_byper_bytier_nokWhdaily['max'])
            monthly_energy_byper_bytier_nokWhdaily['max_cumulative'] = monthly_energy_byper_bytier_nokWhdaily.groupby(['label','month','period'])['max_final'].cumsum()
            # determine excess energy above tier threshold and shift down to assign to next tier
            monthly_energy_byper_bytier_nokWhdaily['energy_excess'] = np.where(monthly_energy_byper_bytier_nokWhdaily['energy_kwh'] > monthly_energy_byper_bytier_nokWhdaily['max_cumulative'], monthly_energy_byper_bytier_nokWhdaily['energy_kwh'] - monthly_energy_byper_bytier_nokWhdaily['max_cumulative'],0)
            monthly_energy_byper_bytier_nokWhdaily['energy_excess_shift'] = monthly_energy_byper_bytier_nokWhdaily.groupby(['label','month','period'])['energy_excess'].shift()
            # select final energy value for each tier based on several conditions
            monthly_energy_byper_bytier_nokWhdaily['energy_tier'] = np.select(  condlist=[monthly_energy_byper_bytier_nokWhdaily['energy_excess']>0, 
                                                                                    monthly_energy_byper_bytier_nokWhdaily['energy_excess_shift']>0,
                                                                                    monthly_energy_byper_bytier_nokWhdaily['energy_excess_shift'].isna()],
                                                                                choicelist=[monthly_energy_byper_bytier_nokWhdaily['max_final'],
                                                                                    monthly_energy_byper_bytier_nokWhdaily['energy_excess_shift'],
                                                                                    monthly_energy_byper_bytier_nokWhdaily['energy_kwh']],default=0)
            # calculate total energy cost
            monthly_energy_byper_bytier_nokWhdaily['energy_cost_total'] = monthly_energy_byper_bytier_nokWhdaily['energy_cost'] * monthly_energy_byper_bytier_nokWhdaily['energy_tier']
            # calculate annual energy cost for each eligible rate and combine into one df
            annual_energy_cost_df_kWhdaily = daily_energy_byper_bytier_kWhdaily[['label','energy_cost_total']].groupby(by='label').sum()
            annual_energy_cost_df_nokWhdaily = monthly_energy_byper_bytier_nokWhdaily[['label','energy_cost_total']].groupby(by='label').sum()
            annual_energy_cost_df = pd.concat([annual_energy_cost_df_kWhdaily,annual_energy_cost_df_nokWhdaily])
            annual_energy_cost_df.rename(columns={'energy_cost_total':'annual_energy_cost'},inplace=True)
            annual_energy_cost_df = annual_energy_cost_df.reset_index()
            # merge annual energy cost to eligible rates
            eligible_rates = eligible_rates.merge(annual_energy_cost_df,how='left',on='label')
            eligible_rates = eligible_rates[eligible_rates.annual_energy_cost>=0] #remove negative energy costs
            logging.info(f'{p} - Annual energy cost calculations complete.')

            #----------------------------------------------------------------------------
            # calculate total annual costs
            eligible_rates['annual_cost_total'] = eligible_rates['annual_fixed_cost'] + eligible_rates['annual_demand_cost'] + eligible_rates['annual_energy_cost']
            new_field = f'{p}_lvl_cost_per_kwh'
            eligible_rates[new_field] = eligible_rates['annual_cost_total']/annual_energy_kwh

            eligible_rates.to_csv(os.path.join(outpath,f'dcfc_rates_{p}.csv'), index=False)