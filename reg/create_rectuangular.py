"""Create rectangular shapefile as scope for GLAES priors."""
import pandas as pd
import geopandas as gpd
from functools import partial
import shapely.geometry as sp
from shapely.ops import unary_union, transform
import pyproj
import fiona
import fiona.crs
from rtree import index


SCHEMA = {'geometry': 'Polygon',
          'properties': {'id': 'str'}}

#%%
# =============================================================================
# Europe
# =============================================================================
europe = sp.box(2069200, 1187800, 7419700, 5585800)  # xl, yl, xu, yu

CRS = fiona.crs.from_epsg(3035)  # projetion: ETRS89 (Europe)

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/europe_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(europe),
             'properties': {'id': 'scope'}})
    
#%%
# =============================================================================
# Test: North sea
# =============================================================================
north_sea = sp.box(4E6, 3.3E6, 4.3E6, 3.7E6)  # xl, yl, xu, yu

CRS = fiona.crs.from_epsg(3035)

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/north_sea_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(north_sea),
             'properties': {'id': 'scope'}})


#%%
# =============================================================================
# Test: Mediterrainean sea
# =============================================================================
med_sea = sp.box(4E6, 3.3E6, 4.3E6, 3.6E6)  # xl, yl, xu, yu

CRS = fiona.crs.from_epsg(3035)

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/north_sea_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(north_sea),
             'properties': {'id': 'scope'}})

#%%
# =============================================================================
# Europe EEZ
# =============================================================================

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/europe_eez_rectangular.shp'

# read shp files
eez = gpd.read_file('C:/Users/Robin/Documents/Inatech/Model/input_raw/Marineregions/eez_v11.shp')
europe_rectangular = gpd.read_file('C:/users/Robin/Git_Projects/glaes/reg/europe_rectangular.shp')
europe = pd.read_csv('C:/Users/Robin/Documents/Inatech/Model/input_raw/ISO/country_codes.csv',
                      encoding='ISO-8859-1')

# filter EEZ of european countries
europe = europe[europe['region'] == 'Europe']['alpha-3']
europe = europe[europe != 'RUS']
europe_eez = eez[eez['ISO_SOV1'].isin(europe)].cx[-44:75, 30:75]

# unify european EEZs
europe_eez = europe_eez['geometry'].unary_union
europe_eez = gpd.GeoDataFrame({'geometry': europe_eez}, crs=eez.crs)

# common CRS
europe_eez = europe_eez.to_crs(europe_rectangular.crs)

europe_eez_rectangular = gpd.GeoDataFrame(
    {'geometry': gpd.overlay(europe_rectangular,europe_eez, how='intersection').unary_union}, 
    crs=europe_rectangular.crs
    )
europe_eez_rectangular.to_file(OUT_DIR)

# with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=europe_rectangular.crs) as c:
#     c.write({'geometry': sp.mapping(europe_eez_rectangular2),
#              'properties': {'id': 'scope'}})
    
    
#%%
# =============================================================================
# European scope EEZ
# =============================================================================

def intersection(poly1, poly2):
    """Calculate intersection of two polygons repairing invalid polygon 1."""
    try:
        return poly.intersection(poly2)
    except:
        return poly1.buffer(0).intersection(poly2)

#paths
OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/europe_eez_rectangular2.shp'
eez_dir = 'C:/Users/Robin/Documents/Inatech/Model/input_raw/Marineregions/eez_v11.shp'
# europe_rectangular_dir ='C:/users/Robin/Git_Projects/glaes/reg/europe_rectangular.shp'
europe_rectangular_dir ='C:/users/Robin/Git_Projects/glaes/reg/north_sea_rectangular.shp'


# filter shapes in rectangular
polygons = []
with fiona.open(eez_dir) as eez:
    meta_source = eez.meta
    
    with fiona.open(europe_rectangular_dir) as europe_rectangular:
        meta = europe_rectangular.meta
        
        for rectangular in europe_rectangular:
            rect = sp.shape(rectangular['geometry'])
            
    # reprojection to EPSG:3035
    project = partial(pyproj.transform,
                      pyproj.Proj(**meta_source['crs']),  # from
                      pyproj.Proj(**meta['crs']))  # to                
    
    for feature in eez.filter(bbox=(8,53,9,54)):
        if feature["geometry"] is None:
            print('continue 1')
            continue
        
        feat = sp.shape(feature['geometry'])
        feat = transform(project, feat)
        
        if isinstance(feat, sp.multipolygon.MultiPolygon):
            feat = list(feat)
        else:
            feat = [feat]
                
        polys = [intersection(f, rect) for f in feat]
        polys = [p for p in polys if not p.is_empty]
        
        polygons.extend(polys)

# debundle multipolygons
polygons_debundled = []
for p in polygons:
    if isinstance(p, sp.polygon.Polygon):
        polygons_debundled.append(p)
    elif isinstance(p, sp.multipolygon.MultiPolygon):
        polygons_debundled.extend(list(p))
    else:
        print('Unsupported type in polygons')

out_shape = unary_union(polygons_debundled)

# write file
with fiona.open(OUT_DIR, 'w', driver='ESRI Shapefile', schema=SCHEMA, crs=meta['crs']) as europe_eez_rectangular:
    europe_eez_rectangular.write({'geometry': sp.mapping(out_shape),
                                  'properties': {'id': 'scope'}})
