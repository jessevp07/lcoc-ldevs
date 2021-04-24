"""
I/O functions
"""
#public
import os
import glob
import gzip
import filecmp
import urllib3
import requests
import subprocess
import pandas as pd
from io import BytesIO, StringIO

#private
import lcoc.helpers as helpers

#settings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_urdb_data(source):
    """
    Function downloads, unpacks, and loads raw URDB rate data from 'source'
    and returns a Pandas DataFrame containing this data.
    """
    
    response = requests.get(source, verify=False)
    code = response.status_code
    assert response.ok, "HTTP Error: Response code {}".format(code)
    bytes_io = BytesIO(response.content)
        
    with gzip.open(bytes_io, 'rt') as infile:
        df = pd.read_csv(infile, low_memory=False)
    
    return df

def write_urdb_rate_data(urdb_rate_data, urdb_filepath = './', overwrite_identical=True):
    """
    Takes Pandas DataFrame containing URDB rate data and stores as .csv at
    urdb_filepath. The 'overwrite_identical' variable indicates whether 
    'urdb_rate_data' should be compared to previous URDB download and replace it
    if they are found to be identical. This avoids data duplication when it is
    unnecessary. Function returns True if data previously exists, False if it is
    unique, and None if the comparison was never performed (overwrite_identical 
    == False).
    """

    todays_date = helpers.todays_date()
    new_urdb_file = urdb_filepath+'usurdb_{}.csv'.format(todays_date)
    urdb_rate_data.to_csv(new_urdb_file, index=False)
        
    # Check if db has changed since last download
    if overwrite_identical:
        prev_urdb_files = glob.glob(urdb_filepath+'*.csv')
        if len(prev_urdb_files)>1:
            prev_urdb_dates = [fp.split('usurdb_')[1].split('.csv')[0] for fp in prev_urdb_files]
            prev_urdb_dates.remove(todays_date)
            most_recent_date = pd.Series(prev_urdb_dates).map(int).max()
            most_recent_urdb_file = urdb_filepath+'usurdb_{}.csv'.format(most_recent_date)
            
            if filecmp.cmp(new_urdb_file, most_recent_urdb_file, shallow=True):
                subprocess.run('rm {}'.format(most_recent_urdb_file), shell=True)
                prev_exists = True
            else:
                prev_exists = False
        else:
            prev_exists = False
    else:
        prev_exists = None

    return prev_exists  

def read_afdc_data():
    """
    Reads Public DCFC data from the AFDC for active, public DCFC stations 
    and returns data as Pandas DataFrame
    """

    api_key = 'ddN1odaWTizqFDyULDSl1dhpV7bfJYw88nqnljSU'
    url = 'https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?'
    PARAMS = {'api_key': api_key,
              'status': 'E', #stations currently open
              'access': 'public', 
              'fuel_type': 'ELEC',
              'ev_charging_level': 'dc_fast'}
    r = requests.get(url, PARAMS, verify=False)
    s = str(r.content, 'utf-8')
    data = StringIO(s)
    df = pd.read_csv(data)

    return df

def write_afdc_data(afdc_df, afdc_filepath = './', overwrite_identical=True):
    """
    Takes Pandas DataFrame containing AFDC DCFC data and stores as .csv at
    afdc_filepath. The 'overwrite_identical' variable indicates whether 
    'afdc_df' should be compared to previous URDB download and replace it
    if they are found to be identical. This avoids data duplication when it is
    unnecessary. Function returns True if data previously exists, False if it is
    unique, and None if the comparison was never performed (overwrite_identical 
    == False).
    """
    
    path = os.getcwd()
    todays_date = helpers.todays_date()
    new_afdc_file = afdc_filepath+'afdc_{}.csv'.format(todays_date)
    afdc_df.to_csv(new_afdc_file, index=False)
        
    # Check if db has changed since last download
    if overwrite_identical:
        prev_afdc_files = glob.glob(afdc_filepath+'*.csv')
        if len(prev_afdc_files)>1:
            prev_afdc_dates = [fp.split('afdc_')[1].split('.csv')[0] for fp in prev_afdc_files]
            prev_afdc_dates.remove(todays_date)
            most_recent_date = pd.Series(prev_afdc_dates).map(int).max()
            most_recent_afdc_file = afdc_filepath+'afdc_{}.csv'.format(most_recent_date)
            
            if filecmp.cmp(new_afdc_file, most_recent_afdc_file):
                subprocess.run('rm {}'.format(most_recent_afdc_file), shell=True)
                prev_exists = True
            else:
                prev_exists = False
        else:
            prev_exists = False
    else:
        prev_exists = None

    return prev_exists  

    


    