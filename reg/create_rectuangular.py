"""Create rectangular shapefile as scope for GLAES priors."""
import shapely.geometry as sp
import fiona
import fiona.crs


SCHEMA = {'geometry': 'Polygon',
          'properties': {'id': 'str'}}

#%%
# =============================================================================
# Europe
# =============================================================================
europe = sp.box(2068700, 1187300, 7420200, 5586300)

CRS = fiona.crs.from_epsg(3035)  # projetion: ETRS89 (Europe)

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/europe_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(europe),
             'properties': {'id': 'scope'}})
    
#%%
# =============================================================================
# Test: North sea
# =============================================================================
north_sea = sp.box(4E6, 3.3E6, 4.3E6, 3.6E6)  # xl, yl, xu, yu

CRS = fiona.crs.from_epsg(3035)

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/north_sea_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(north_sea),
             'properties': {'id': 'scope'}})


