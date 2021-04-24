import sys
import geopandas as gpd 

def shp_to_geojson(shp_file, geojson_file=''):
    gdf = gpd.read_file(shp_file)
    shp_name = shp_file.split('/')[-1].split('.shp')[0]
    if geojson_file == '':
        geojson_file = geojson_file+shp_name+'.geojson'
    
    gdf.to_file(geojson_file, driver='GeoJSON')
    return(1)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        shp_file = sys.argv[1]
        shp_to_geojson(shp_file)
    elif len(sys.argv) == 3:
        shp_file = sys.argv[1]
        geojson_file = sys.argv[2]
        shp_to_geojson(shp_file, geojson_file)

    else:
        raise ValueError('Should at most specify .shp file and .geojson file (optional)')
    

    
