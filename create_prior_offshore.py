import geokit as gk
import numpy as np
from os.path import join, isdir
from os import mkdir
import sys
from datetime import datetime as dt
from collections import OrderedDict
from json import dumps
from osgeo import ogr
from multiprocessing import Process                                                                     


# =============================================================================
# DEFINE SOURCES
# =============================================================================

INPUT_RAW_DIR = "../Master-Thesis-Robin-Krekeler/input_raw/"
OUTPUT_DIR = "../Master-Thesis-Robin-Krekeler/input_raw/GLAES/"

waterdepthSource = INPUT_RAW_DIR + 'GEBCO/gebco_2020_n75.0_s30.0_w-44.0_e75.0.tif'
wdpaMarineSource = (INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile0/WDPA_Jun2020_marine-shapefile-polygons.shp',
                    # INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile0/WDPA_Jun2020_marine-shapefile-points.shp',
                    INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile1/WDPA_Jun2020_marine-shapefile-polygons.shp',
                    # INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile1/WDPA_Jun2020_marine-shapefile-points.shp',
                    INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile2/WDPA_Jun2020_marine-shapefile-polygons.shp'#,
                    # INPUT_RAW_DIR + 'WDPA/WDPA_Jun2020_marine-shapefile2/WDPA_Jun2020_marine-shapefile-points.shp'
                    )
countriesSource = INPUT_RAW_DIR + 'NaturalEarth/ne_10m_admin_0_countries.shp'
seacablesSource = INPUT_RAW_DIR + 'SubmarineCableMap/cable-geo.json'
pipelinesSource = INPUT_RAW_DIR + 'WorldMap/natural_gas_pipelines_j96.shp'
shippingSource = INPUT_RAW_DIR + 'KNB/shipping_hand_drawn.shp'


#%%
# =============================================================================
# DEFINE EDGES
# =============================================================================

EVALUATION_VALUES = { 
    "waterdepth_threshold":
        # Indicates area with waterdepth less than X (m)
        [0, 5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 150, 200, 300, 
         500, 750, 1000, 1250, 1500, 2000],
    "shore_proximity":
        # Indicates distances too close to protected areas (m)
        [0, 500, 1000, 1500, 2000, 5000, 7000, 10000, 12000, 14000, 16000,
         18000, 20000, 22000, 25000, 30000],
    "protected_marine_area_proximity":
        # Indicates distances too close to protected areas (m)
        [0, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 
         4000, 5000],
    "protected_marine_bird_proximity":
        # Indicates distances too close to protected bird areas (m)
        [0, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 
         4000, 5000],
    "submarine_cable_proximity":
        # Indicates distances too close to submarine cables (m)
        [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    "pipeline_proximity":
        # Indicates distances too close to natural gas pipelines (m)
        [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    "shipping_proximity":
        # Indicates distances too close to center of shipping routes (m)
        [0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 
         6000]
    }


#%%
# =============================================================================
# EVALUATION FUNCTIONS
# =============================================================================
def evaluate_WATERDEPTH(regSource, tail):
    name = "waterdepth_threshold"
    unit = "meters"
    description = "Indicates pixels in which the water depth is less-than or equal-to X meters"
    source = "GEBCO 2020 Gridded Bathymetry"

    # Get distances
    thresholds = EVALUATION_VALUES[name]

    # Make Region Mask
    reg = gk.RegionMask.load(regSource, padExtent=500)

    # Create a geometry list from the osm files
    result = edgesByThreshold(reg, waterdepthSource, [-x for x in thresholds], True)

    # make result
    writeEdgeFile(result, reg, name, tail, unit, description, source, thresholds)


def evaluate_SHORE(regSource, tail):
    name = "shore_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from shore"
    source = "NaturalEarth"

    # Get distances
    distances = EVALUATION_VALUES[name]
    
    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the NaturalEarth files
    geom = geomExtractor(reg.extent, countriesSource, r"CONTINENT = 'Europe'", srs=reg.srs)

    # Get edge matrix
    result = edgesByProximity(reg, geom, distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


def evaluate_MARINERESERVES(regSource, tail):
    name = "protected_marine_area_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from a protected area"
    source = "WDPA"

    # Get distances
    distances = EVALUATION_VALUES[name]

    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the osm files
    geom = []
    for s in wdpaMarineSource:
        try: 
            geom.extend(geomExtractor(reg.extent, s, srs=reg.srs))
        except TypeError: 
            print('No feature extracted from ...' + str(s[-60:]))
            
    # Get edge matrix
    result = edgesByProximity(reg, dissolve(geom), distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


def evaluate_MARINEBIRDS(regSource, tail):
    name = "protected_marine_bird_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from a protected bird area"
    source = "WDPA"

    # Get distances
    distances = EVALUATION_VALUES[name]

    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the osm files
    geom = []
    for s in wdpaMarineSource:
        try: 
            geom.extend(geomExtractor(reg.extent, s, srs=reg.srs, where=r"DESIG_ENG LIKE '%bird%'"))
        except TypeError: 
            print('No feature extracted from ...' + str(s[-60:]))

    # Get edge matrix
    result = edgesByProximity(reg, dissolve(geom), distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


def evaluate_SEACABLES(regSource, tail):
    name = "submarine_cable_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from submarine cables"
    source = "SubmarineCableMap"

    # Get distances
    distances = EVALUATION_VALUES[name]
    
    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the NaturalEarth files
    geom = geomExtractor(reg.extent, seacablesSource, srs=reg.srs)

    # Get edge matrix
    result = edgesByProximity(reg, geom, distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


def evaluate_PIPELINES(regSource, tail):
    name = "pipeline_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from natural gas pipelines"
    source = "WorldMap"
    
    # Get distances
    distances = EVALUATION_VALUES[name]
    
    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the NaturalEarth files
    geom = geomExtractor(reg.extent, pipelinesSource, srs=reg.srs)

    # Get edge matrix
    result = edgesByProximity(reg, geom, distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


def evaluate_SHIPPING(regSource, tail):
    name = "shipping_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from center of shipping routes"
    source = "Knowledge Network for Biocomplexity"

    # Get distances
    distances = EVALUATION_VALUES[name]
    
    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=0, padExtent=max(distances))

    # Create a geometry list from the NaturalEarth files
    geom = geomExtractor(reg.extent, shippingSource, srs=reg.srs)

    # Get edge matrix
    result = edgesByProximity(reg, geom, distances)

    # make result
    writeEdgeFile( result, reg, name, tail, unit, description, source, distances)


#%%
# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def edgesByProximity(reg, geom, distances):
    
    # make initial matrix
    mat = np.ones(reg.mask.shape, dtype=np.uint8)*255 # Set all values to no data (255)
    mat[reg.mask] = 254 # Set all values in the region to untouched (254)
    
    # Only do growing if a geometry is available
    if not geom is None and len(geom)!=0:
        # make grow func
        def doGrow(geom, dist):
            if dist > 0:
                if isinstance(geom,list) or isinstance(geom, filter):
                    grown = [g.Buffer(dist) for g in geom] 
                else:
                    grown = geom.Buffer(dist) 
            else:
                grown = geom

            return grown
        
        # Do growing
        value = 0
        for dist in distances: 
            grown = doGrow(geom, dist)
            try:
                tmpSource = gk.vector.createVector(grown) # Make a temporary vector file
            except Exception as e:
                print(len(grown), [g.GetGeometryName() for g in grown])
                raise e
            
            indicated = reg.indicateFeatures(tmpSource) > 0.5 # Map onto the RegionMask
            
            # apply onto matrix
            sel = np.logical_and(mat==254, indicated) # write onto pixels which are indicated and available
            mat[sel] = value
            value += 1

    # Done!
    return mat


def edgesByThreshold(reg, source, thresholds, inverse=False):
    # make initial matrix
    mat = np.ones(reg.mask.shape, dtype=np.uint8)*255 # Set all values to no data (255)
    mat[reg.mask] = 254 # Set all values in the region to untouched (254)
    
    # Only do growing if a geometry is available
    value = 0
    for thresh in thresholds:
        if inverse:
            indicated = reg.indicateValues(source, value=(thresh,None)) > 0.5
        else:
            indicated = reg.indicateValues(source, value=(None,thresh)) > 0.5

        # apply onto matrix
        sel = np.logical_and(mat==254, indicated) # write onto pixels which are indicated and available
        mat[sel] = value
        value += 1

    # Done!
    return mat


def geomExtractor(extent, source, where=None, simplify=None, srs=None):
    searchGeom = extent.box
    if isinstance(source,str):
        searchFiles = [source,]
    else:
        searchFiles = list(extent.filterSources( join(source[0], source[1]) ))
    
    geoms = []
    for f in searchFiles:
        for geom in gk.vector.extractFeatures(f, geom=searchGeom, where=where, outputSRS=extent.srs, srs=srs)['geom']:
            geoms.append( geom.Clone() )

    if not simplify is None: 
        newGeoms = [g.SimplifyPreserveTopology(simplify) for g in geoms]
        for g, ng in zip(geoms, newGeoms):
            if "LINE" in ng.GetGeometryName():
                test = ng.Length()/g.Length()
            else: 
                test = ng.Area()/g.Area()

            if test<0.97:
                raise RuntimeError("ERROR: Simplified geometry is >3% different from the original")
            elif test<0.99:
                print("WARNING: simplified geometry is slightly different from the original")

    
    if len(geoms) == 0:
        return None
    else:
        return geoms


def dissolve(geom):
    multipolygon = ogr.Geometry(ogr.wkbMultiPolygon)
    first_step = True
    for g in geom:
        if not g.IsValid():
            continue
        if first_step:
            sr = g.GetSpatialReference()
            first_step = False
        multipolygon.AddGeometry(g)
        if g.GetSpatialReference().ExportToWkt() != sr.ExportToWkt():
            print('All elements in geom have to have the same CRS.')
            raise

    multipolygon = multipolygon.UnionCascaded()
    multipolygon.AssignSpatialReference(sr)
    
    return[multipolygon]


def writeEdgeFile( result, reg, name, tail, unit, description, source, values):
    # make output
    output = "%s.%s.tif"%(name,tail)
    if not isdir(OUTPUT_DIR): mkdir(OUTPUT_DIR)

    valueMap = OrderedDict()
    for i in range(len(values)): valueMap["%d"%i]="<=%.2f"%values[i]
    valueMap["254"]="untouched"
    valueMap["255"]="noData"

    meta = OrderedDict()
    meta["GLAES_PRIOR"] = "YES"
    meta["DISPLAY_NAME"] = name
    meta["ALTERNATE_NAME"] = "NONE"
    meta["DESCRIPTION"] = description
    meta["UNIT"] = unit
    meta["SOURCE"] = source
    meta["VALUE_MAP"] = dumps(valueMap)

    print(output)

    d = reg.createRaster(output=join(OUTPUT_DIR,output), data=result, overwrite=True, noDataValue=255, dtype=1, meta=meta)



#%%
# =============================================================================
# MAIN FUNCTIONALITY
# =============================================================================

if __name__== '__main__':
    START = dt.now()
    tail = str(int(dt.now().timestamp()))
    print( "RUN ID: ", tail)
    print( "TIME START: ", START)

    source = sys.argv[2]
    constraints = str(sys.argv[1]).split(',')
    
    # parallelize if multiple contraints are given
    if len(constraints) > 1:
        jobs = []
        for c in constraints:
            func = globals()["evaluate_" + c]
            j = Process(target=func, args=(source, tail))
            jobs.append(j)
            j.start()
        # wait for all jobs to finish
        for j in jobs:
            j.join()
    else:
        func = globals()["evaluate_" + str(constraints[0])]
        func(source, tail)

    END = dt.now()
    print( "TIME END: ", END)
    print( "CALC TIME: ", (END-START))
