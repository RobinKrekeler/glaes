"""Create rectangular shapefile as scope for GLAES priors."""
import shapely.geometry as sp
import fiona

europe = sp.box(2068700, 1187300, 7420200, 5586300)

CRS = fiona.crs.from_epsg(25832)  # projetion: ETRS89 (Europe)
SCHEMA = {'geometry': 'Polygon',
          'properties': {'id': 'str'}}

OUT_DIR = 'C:/users/Robin/Git_Projects/glaes/reg/europe_rectangular.shp'

with fiona.open(OUT_DIR, 'w', 'ESRI Shapefile', SCHEMA, crs=CRS) as c:
    c.write({'geometry': sp.mapping(europe),
             'properties': {'id': 'scope'}})
