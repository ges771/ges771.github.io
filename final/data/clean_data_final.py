import numpy as np
import datetime
import pandas as pd
import functools as fc
from pandas.api.types import is_string_dtype
import urllib
from urllib.request import urlopen
import json
import geopandas as gpd
from shapely.geometry import Point
import os
import os
import fiona
from fiona import supported_drivers
#need to set env vars in anaconda: set GDAL_DATA=%CONDA_PREFIX%\Library\share\gdal set _CONDA_SET_GDAL_DATA=%GDAL_DATA%
if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'
from osgeo import gdal,ogr,osr
'GDAL_DATA' in os.environ


# This is a function to clean up the headers. It's predicated on the assumption that future excel files will have the same format.
sheets = []
def cleanup(xls_workbook):
    for sheet_name in xls2010to16.sheet_names:
        sheet = xls2010to16.parse(sheet_name,  header = 0, skiprows=3)
        sheet= sheet.dropna(axis=1, how = 'all')
        sheet = sheet.dropna(thresh=2)
        #Determine how many rows in header based on presence of first recipient number, then modify header. 
        if sheet.iat[0,0]=='Recipient Number':
            sheet.columns = sheet.iloc[0]
            sheet= sheet[1:]
        else:
            sheet.columns = sheet.iloc[1]
        #This is placeholder code for evenutally merging two rows into one. I did it manually in excel because it was taking too long to do with python.
            sheet= sheet[2:]
        sheets.append(sheet)
    return sheets   


def dat_tostr(df):
    for row in df.columns:
        #if row.str.contains("Date", regex=False):
        if "Date" in row:
            if "to" not in row:
                if row in df:
                    df["%s"%row]=df["%s"%row].astype('str')
                else:
                    pass
    return df   


#Input
xls2010to16 = pd.ExcelFile(r"C:\Users\brian\OneDrive - UMBC\GES_771\final\FIPSXYLocation for all 2010 CMF projects-4-16-2019-Batch 4_redacted_cleanedup.xlsx")
#cities = gpd.read_file(r"C:\Users\altebri\Documents\Gitlab\cmf-mapping-tool\data\cities\2015_CDP.shp")
cleanup(xls2010to16)
#print (sheets)

#Merged the sheets into a dataframe so I could work with a single dataframe.
df = fc.reduce(lambda left, right: pd.merge(left, right, how = 'left', left_on=['Recipient Number','Project Number'], right_on=['Recipient Number','Project Number'] ),sheets)

df= df.dropna(axis=1, how='all')
df= df.dropna(axis=0, how='all')
df['new_index']= df['Recipient Number'].astype(str)+"."+df['Project Number'].astype(str)
df = df.drop_duplicates(subset='new_index', inplace=False)



#splitting out the dataframe by presence or absence of addresses. This will save some processing time later. 
df=df[pd.notnull(df['Mask X_x'])]
#This was the only way I could get the new version to geocode. 
df['Coordinates'] = list(zip(df['Mask X_x'], df['Mask Y_x']))
df['Coordinates'] = df['Coordinates'].apply(Point)
gdf = gpd.GeoDataFrame(df, geometry='Coordinates')

#Clean up gpd
gdf = gdf.fillna('')
gdf = gdf.drop(['Project Zip Code+4','ProjectX','ProjectY', 'Mask X_x', 'Mask Y_x', 'Mask X_y', 'Mask Y_y','Mask Geocoded Census Tract FIPS_y','Mask Geocoded Census Tract FIPS Location_y','Mask Y_y'], axis= 1)

#prepping for export to geojson. Date/Time format not recognized
gdf = dat_tostr(gdf)

print (set(gdf['Project Type']))



#Export to geojson
filename = r"C:\Users\brian\OneDrive - UMBC\GES_771\final\data\CMFdata2019.geojson"
CSVfilename = r"C:\Users\brian\OneDrive - UMBC\GES_771\final\data\CMFdata2019.csv"

try:
    os.remove(filename)
except OSError:
    pass
try:
    os.remove(CSVfilename)
except OSError:
    pass

gdf.to_file(filename, driver="GeoJSON")
gdf.to_csv(CSVfilename, sep = ',')