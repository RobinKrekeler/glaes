import geokit as gk
import numpy as np
from os.path import join, isdir, isfile, basename, splitext
from os import mkdir
import sys
from multiprocessing import Pool
import time
from datetime import datetime as dt
from glob import glob
from collections import namedtuple, OrderedDict
from json import dumps

#################################################################
## DEFINE SOURCES

INPUT_RAW_DIR = "../Master-Thesis-Robin-Krekeler/input_raw/"

waterdepthSource = INPUT_RAW_DIR + 'GEBCO/gebco_2020_n75.0_s30.0_w-18.0_e48.0.tif'
##################################################################
## DEFINE EDGES
EVALUATION_VALUES = { 
    "waterdepth_threshold":
        # Indicates area with waterdepth less than X (m)
        [0, 5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 150, 200, 300, 500, 1000, 1500]
      }

#######################################################
## EVALUATION FUNCTIONS
def evaluate_WATERDEPTH(regSource, ftrID, tail):
    name = "waterdepth_threshold"
    unit = "meters"
    description = "Indicates pixels in which the water depth is less-than or equal-to X meters"
    source = "GEBCO 2020 Gridded Bathymetry"

    output_dir = join("../Master-Thesis-Robin-Krekeler/input_raw/GLAES/", name)

    # Get distances
    thresholds = EVALUATION_VALUES[name]

    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=ftrID, padExtent=500)

    # Create a geometry list from the osm files
    result = edgesByThreshold(reg, waterdepthSource, [-x for x in thresholds], True)

    # make result
    writeEdgeFile( result, reg, ftrID, output_dir, name, tail, unit, description, source, thresholds)


def evaluate_PARK(regSource, ftrID, tail):
    name = "protected_park_proximity"
    unit = "meters"
    description = "Indicates pixels which are less-than or equal-to X meters from a protected park"
    source = "WDPA"

    output_dir = join("outputs", name)

    # Get distances
    distances = EVALUATION_VALUES[name]

    # Make Region Mask
    reg = gk.RegionMask.load(regSource, select=ftrID, padExtent=max(distances))

    # Create a geometry list from the osm files
    geom = geomExtractor( reg.extent, wdpaSource, where=r"DESIG_ENG LIKE '%park%' OR IUCN_CAT = 'II'")

    # Get edge matrix
    result = edgesByProximity(reg, geom, distances)

    # make result
    writeEdgeFile( result, reg, ftrID, output_dir, name, tail, unit, description, source, distances)

##################################################################
## UTILITY FUNCTIONS
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

def writeEdgeFile( result, reg, ftrID, output_dir, name, tail, unit, description, source, values):
    # make output
    output = "%s.%s_%05d.tif"%(name,tail,ftrID)
    if not isdir(output_dir): mkdir(output_dir)

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

    d = reg.createRaster(output=join(output_dir,output), data=result, overwrite=True, noDataValue=255, dtype=1, meta=meta)

def geomExtractor( extent, source, where=None, simplify=None ): 
    searchGeom = extent.box
    if isinstance(source,str):
        searchFiles = [source,]
    else:
        searchFiles = list(extent.filterSources( join(source[0], source[1]) ))
    
    geoms = []
    for f in searchFiles:
        for geom, attr in gk.vector.extractFeatures(f, searchGeom, where=where, outputSRS=extent.srs):
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

###################################################################
## MAIN FUNCTIONALITY
if __name__== '__main__':
    START= dt.now()
    tail = str(int(dt.now().timestamp()))
    print( "RUN ID: ", tail)
    print( "TIME START: ", START)

    # Choose the function
    func = globals()["evaluate_"+sys.argv[1]]

    # Choose the source
    if len(sys.argv)<3:
        source = join("reg","aachenShapefile.shp")
    else:
        source = sys.argv[2]

    # Arange workers
    if len(sys.argv)<4:
        doMulti = False
    else:
        doMulti = True
        pool = Pool(int(sys.argv[3]))
    
    # submit jobs
    res = []
    count = -1
    # for g,a in gk.vector.extractFeatures(source):
    for g in gk.vector.extractFeatures(source):
        count += 1
        #if count<1 : continue
        #if count == 2:break

        # Do the analysis
        if doMulti:
            res.append(pool.apply_async(func, (source, count, tail)))
        else:
            func(source, count, tail)
    
    if doMulti:
        # Check for errors
        for r,i in zip(res,range(len(res))):
            try:
                r.get()
            except Exception as e:
                print("EXCEPTION AT ID: "+str(i))
                raise e

        # Wait for jobs to finish
        pool.close()
        pool.join()

    # finished!
    END= dt.now()
    print( "TIME END: ", END)
    print( "CALC TIME: ", (END-START))

