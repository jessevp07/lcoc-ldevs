"""
DCFastChargingLocator object created from data downloaded from the AFDC, 
https://afdc.energy.gov/.
"""
#public
import pandas as pd
import geopandas as gpd
import os
import config
from shapely.geometry import Point

#private
import lcoc.readwrite as readwrite

class DCFastChargingLocator(object):
    """
    Object for working with DCFC station location data downloaded from the
    the Alternative Fuels Data Center (AFDC). The AFDC is a comprehensive
    clearinghouse of information about advanced transportation technologies. 
    It is supported by the U.S. Department of Energy's DOE Vehicle Technologies 
    Office. 

    Attributes
    -----------
    station_data:
        pandas.DataFrame of public, active DCFC stations from the AFDC 
    prev_exists:
        Boolean indicating whether version of dataset has been previously ran
    """

    def __init__(self, afdc_file=None):
        # Download AFDC data
        if afdc_file is not None:
            self.station_data = pd.read_csv(afdc_file, low_memory=False)
        else:
            self.station_data = readwrite.read_afdc_data()
            # Save copy of AFDC data (if unique) to data/afdc
            self.prev_exists = readwrite.write_afdc_data(self.station_data)

        # Preprocessing for unique DCFC stations only
        self.station_data = self.station_data[self.station_data['EV DC Fast Count'] > 0]
        self.station_data = self.station_data.groupby(['Latitude', 'Longitude'])['EV DC Fast Count'].sum().reset_index()

    def join_county_geoid(self,us_counties_gdf_file = os.path.join(config.DATA_PATH,'gis','2017_counties','cb_2017_us_county_500k','cb_2017_us_county_500k.shp')):
        """
        Function adds 'county_geoid' field to self.station_data by joining 
        station latitude/longitude to county geojson file.
        """
        
        # Add Point geometries
        pts = []
        for lon, lat in zip(self.station_data['Longitude'], self.station_data['Latitude']):
            pt = Point(lon, lat)
            pts.append(pt)

        self.station_data['geom'] = pts
        self.station_data = gpd.GeoDataFrame(self.station_data, geometry='geom')

        # Spatial join
        us_counties_gdf = gpd.read_file(us_counties_gdf_file)
        self.station_data.crs = us_counties_gdf.crs
        county_join_gdf = us_counties_gdf[['NAME', 'GEOID', 'STATEFP', 'COUNTYFP', 'geometry']]
        self.station_data = gpd.sjoin(self.station_data, us_counties_gdf, how='left', op='intersects')

        # Clean up
        self.station_data.rename(columns = {'NAME': 'county_name',
                               'GEOID': 'geoid',
                               'STATEFP': 'state_fips',
                               'COUNTYFP': 'county_fips'}, inplace=True)
        self.station_data.drop(columns='index_right', inplace=True)
        
    def aggregate_counties_to_csv(self, outfile=os.path.join(config.OUTPUT_PATH,'county-dcfc-counts','afdc_county_station_counts.csv')):
        """
        Function counts stations in county. Outputs station counts as outfile.
        """
        
        county_stations = self.station_data.groupby(['county_name',
                                                     'geoid',
                                                     'state_fips',
                                                     'county_fips'])['geom'].agg('count').reset_index()
        county_stations.rename(columns={'geom': 'n_dcfc_stations'}, inplace=True)
        
        # Add state abbrev
        state_fips_cw = {1: 'AL',
                         2: 'AK',
                         4: 'AZ',
                         5: 'AR',
                         6: 'CA',
                         8: 'CO',
                         9: 'CT',
                         11: 'DC',
                         10: 'DE',
                         12: 'FL',
                         13: 'GA',
                         15: 'HI',
                         19: 'IA',
                         16: 'ID',
                         17: 'IL',
                         18: 'IN',
                         20: 'KS',
                         21: 'KY',
                         22: 'LA',
                         25: 'MA',
                         24: 'MD',
                         23: 'ME',
                         26: 'MI',
                         27: 'MN',
                         29: 'MO',
                         28: 'MS',
                         30: 'MT',
                         37: 'NC',
                         38: 'ND',
                         31: 'NE',
                         33: 'NH',
                         34: 'NJ',
                         35: 'NM',
                         32: 'NV',
                         36: 'NY',
                         39: 'OH',
                         40: 'OK',
                         41: 'OR',
                         42: 'PA',
                         44: 'RI',
                         45: 'SC',
                         46: 'SD',
                         47: 'TN',
                         48: 'TX',
                         49: 'UT',
                         51: 'VA',
                         50: 'VT',
                         53: 'WA',
                         55: 'WI',
                         54: 'WV',
                         56: 'WY'}
        
        county_stations['State'] = county_stations['state_fips'].apply(lambda x: state_fips_cw[int(x)])
        county_stations.to_csv(outfile, index=False)
        print("Complete, {0} stations in {1} counties.".format(county_stations['n_dcfc_stations'].sum(), len(county_stations)))

    def categorize_by_plugcnt(self):
        """
        Categorize stations by plug count where: small = 1-plug, medium = 
        2-6 plugs, large = 7+ plugs.
        """
        
        def categorize(plugs):
            if plugs <= 3:
                category = 's'
            elif 3 < plugs < 15:
                category = 'm'
            elif plugs >= 15:
                category = 'l'
            else:
                category = None   
            return category
        
        self.station_data['category'] = self.station_data['EV DC Fast Count'].apply(categorize)
        








    