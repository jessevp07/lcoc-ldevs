import pandas as pd
import geopandas as gpd 


def state_to_abbrev(state_name):
    """
    Takes state_name str and returns state abbreviation (if applicable).
    """
    state_name_cw = {'Alabama': 'AL',
                     'Alaska': 'AK',
                     'Arizona': 'AZ',
                     'Arkansas': 'AR',
                     'California': 'CA',
                     'Colorado': 'CO',
                     'Connecticut': 'CT',
                     'District of Columbia': 'DC',
                     'Delaware': 'DE',
                     'Florida': 'FL',
                     'Georgia': 'GA',
                     'Hawaii': 'HI',
                     'Iowa': 'IA',
                     'Idaho': 'ID',
                     'Illinois': 'IL',
                     'Indiana': 'IN',
                     'Kansas': 'KS',
                     'Kentucky': 'KY',
                     'Louisiana': 'LA',
                     'Massachusetts': 'MA',
                     'Maryland': 'MD',
                     'Maine': 'ME',
                     'Michigan': 'MI',
                     'Minnesota': 'MN',
                     'Missouri': 'MO',
                     'Mississippi': 'MS',
                     'Montana': 'MT',
                     'North Carolina': 'NC',
                     'North Dakota': 'ND',
                     'Nebraska': 'NE',
                     'New Hampshire': 'NH',
                     'New Jersey': 'NJ',
                     'New Mexico': 'NM',
                     'Nevada': 'NV',
                     'New York': 'NY',
                     'Ohio': 'OH',
                     'Oklahoma': 'OK',
                     'Oregon': 'OR',
                     'Pennsylvania': 'PA',
                     'Rhode Island': 'RI',
                     'South Carolina': 'SC',
                     'South Dakota': 'SD',
                     'Tennessee': 'TN',
                     'Texas': 'TX',
                     'Utah': 'UT',
                     'Virginia': 'VA',
                     'Vermont': 'VT',
                     'Washington': 'WA',
                     'Wisconsin': 'WI',
                     'West Virginia': 'WV',
                     'Wyoming': 'WY'}

    try:
        abbrev = state_name_cw[state_name]
    except:
        abbrev = None

    return abbrev

def merge_locs_with_shp(lfcs_file, state_shp_file, outfile):
    """
    Adds 'lfcs_bev' to state_shp_file. 
    """
    df = pd.read_csv(lfcs_file)
    df.rename(columns={'state': 'state_abbrev'}, inplace=True)
    gdf = gpd.read_file(state_shp_file)

    gdf['state_abbrev'] = gdf['NAME'].apply(state_to_abbrev)
    gdf.dropna(subset=['state_abbrev'], inplace=True)
    gdf = gdf.merge(df, on='state_abbrev', how='inner')
    gdf.to_file(outfile)
    print('Merge complete.')


if __name__ == "__main__":
    locs_file = 'state_results.csv'
    state_shp_file = '../../data/gis/2017_states/cb_2017_us_state_500k.shp'
    outfile = '../../data/gis/2017_states/state_lfcs.shp'
    
    merge_locs_with_shp(lfcs_file, state_shp_file, outfile)
