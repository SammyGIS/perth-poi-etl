# import all required libary
import argparse
import geopandas as gpd
import time
import pandas as pd
from sqlalchemy import create_engine
import requests
import tempfile
import zipfile
import urllib.request
from shapely.geometry import Point
from datetime import datetime


import warnings
warnings.filterwarnings("ignore")

# API services 
SLIP_SERVICES  = "https://services.slip.wa.gov.au/public/rest/services/SLIP_Public_Services/"
ARCGIS_SERVICES = "https://services7.arcgis.com/v8XBa2naYNQGOjlG/arcgis/rest/services/"

#links to data sources 
school_url = "https://apps.det.wa.edu.au/publicreports/SchoolsListExcel0880.xlsx"
police_url = 'https://catalogue.data.wa.gov.au/dataset/a0a4aee4-9197-4783-a7b9-588defb1fd30/resource/a7f118b6-e215-4de0-b083-458e2e8f63ad/download/wapoliceforcefacilities_shp.zip'

hospital_url = f"{SLIP_SERVICES}Health/MapServer/1/query"
toilet_url = f"{ARCGIS_SERVICES}INF_LOC_TOILETACCESSIBILITY_PV/FeatureServer/0/query"
pTransport_url = f"{SLIP_SERVICES}Transport/MapServer/14/query"
carPark_url = f"{ARCGIS_SERVICES}PS_OTH_CARPARKCENTROID_PV/FeatureServer/0/query"
parks_url = f"{ARCGIS_SERVICES}PKS_AST_PARKS_PV/FeatureServer/0/query"
wirelessPoint_url = f"{ARCGIS_SERVICES}INF_AST_WAPLOCATIONS_PV/FeatureServer/0/query"
securityCamera_url = f"{ARCGIS_SERVICES}INF_AST_SECURITYCAMERAS_PV/FeatureServer/0/query"
stAppurtenance_url = f"{ARCGIS_SERVICES}INF_AST_APPURTENANCES_PT_PV/FeatureServer/0/query"
streetLight_url = f"{ARCGIS_SERVICES}INF_PWR_LIGHTING_TVW_PV/FeatureServer/0/query" 
lgaBoundaries_url = f"{SLIP_SERVICES}Boundaries/MapServer/14/query"
localityBoundaries_url = f"{SLIP_SERVICES}Boundaries/MapServer/16/query"




# API parameters 
ARCGIS_POINTS_PARAMS = {
    "where": "OBJECTID > 0",
    "geometryType": "esriGeometryPoint",
    "spatialRel": "esriSpatialRelIntersects",
    "units": "esriSRUnit_Meter",
    "relationParam": "",
    "outFields": "*",
    "returnGeometry": "true",
    "featureEncoding": "esriDefault",
    "f": "geojson"
}

ARCGIS_POLY_PARAMS = {
    "where": "OBJECTID > 0",
    "geometryType": "esriGeometryPolygon",
    "spatialRel": "esriSpatialRelIntersects",
    "units": "esriSRUnit_Meter",
    "relationParam": "",
    "outFields": "*",
    "returnGeometry": "true",
    "featureEncoding": "esriDefault",
    "f": "geojson"
}


# CREATE TABLES
TABLE_SCHOOL = "schools"
TABLE_POLICE = "police force facilities"
TABLE_HOSPITAL = "health facilities"
TABLE_TOILET = "toilet"
TABLE_PUBLIC_TRANS = "public transport"
TABLE_CAR_PARK = "car parks"
TABLE_PARK = "parks"
TABLE_WIRELESS_POINTS = "wireless points"
TABLE_SECURITY_CAMERA = "security camera"
TABLE_STEET_APPURTENANCE = "street appurtenance"
TABLE_STREET_LIGHT = "street light"
TABLE_LGA_BOUNDARIES = "lga boundaries"
TABLE_LOCALITY_BOUNDARIES = "localities boundaries"


def main(params):
    """
    This function will extract data from the sources, transformed then and load them into the database
    params: database parameter 
    """
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    
    # create connection to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    

    def ingest_school(data, table):
      school_data = pd.read_excel(data, skiprows=27, engine='openpyxl')
      geometry = [Point(xy) for xy in zip(school_data['Longitude'], school_data['Latitude'])]
      gdf_school = gpd.GeoDataFrame(school_data, geometry=geometry, crs='EPSG:4326')
      gdf_school.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf_school.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_police(data, table):
      with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
      # Download the data and write it to the temporary file
        urllib.request.urlretrieve(data, temp_file.name)

      # Extract the contents of the zip file to a temporary directory
        with zipfile.ZipFile(temp_file.name, 'r') as zip_file:
          with tempfile.TemporaryDirectory() as temp_dir:
              zip_file.extractall(temp_dir)

              # Load the shapefile into a GeoDataFrame
              gdf = gpd.read_file(temp_dir + '/WAPoliceForceFacilities_SHP/Police_Facilities.shp')
      
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')


    def ingest_hospital(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_toilet(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_publicTrans(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_carParks(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_parks(data,table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_wirelessPoint(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_securityCamera(data,table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_stAppurtenance(data, table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_streetLight(data,table):
      response = requests.get(data, params=ARCGIS_POINTS_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_lgaBoundaries(data, table):
      response = requests.get(data, params=ARCGIS_POLY_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')

    def ingest_localityBoundaries(data, table):
      response = requests.get(data, params=ARCGIS_POLY_PARAMS)
      gdf = gpd.read_file(response.content.decode('utf-8'), driver="GeoJSON")
      gdf.head(n=0).to_postgis(name=table, con=engine, if_exists='replace')
      gdf.to_postgis(name=table, con=engine, if_exists='replace')



    # call all the functions
    print(f'Starting operation at excatly {datetime.now()}')
    t_start = time.time()
    ingest_school(school_url,TABLE_SCHOOL)
    time.sleep(10)

    ingest_police(police_url,TABLE_POLICE)
    time.sleep(10)
    
    ingest_hospital(hospital_url,TABLE_HOSPITAL)
    time.sleep(10)
    
    ingest_toilet(toilet_url,TABLE_TOILET)
    time.sleep(10)

    ingest_publicTrans(pTransport_url,TABLE_PUBLIC_TRANS)
    time.sleep(10) 

    ingest_carParks(carPark_url,TABLE_CAR_PARK)
    time.sleep(10) 

    ingest_parks(parks_url,TABLE_PARK)
    time.sleep(10) 

    ingest_wirelessPoint(wirelessPoint_url,TABLE_WIRELESS_POINTS)
    time.sleep(10) 

    ingest_securityCamera(securityCamera_url,TABLE_SECURITY_CAMERA)
    time.sleep(10) 

    ingest_stAppurtenance(stAppurtenance_url,TABLE_STEET_APPURTENANCE)
    time.sleep(10) 

    ingest_streetLight(streetLight_url,TABLE_STREET_LIGHT)
    time.sleep(10) 

    ingest_lgaBoundaries(lgaBoundaries_url,TABLE_LGA_BOUNDARIES)
    time.sleep(10) 

    ingest_localityBoundaries(localityBoundaries_url,TABLE_LOCALITY_BOUNDARIES)

    t_end = time.time()

    print('all data ingested into the database, took %.3f second' % (t_end - t_start))

 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Geospatial data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')

    args = parser.parse_args()

    main(args)