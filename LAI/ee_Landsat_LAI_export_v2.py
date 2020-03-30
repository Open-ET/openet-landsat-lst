#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 12:33:56 2020

Generate LAI images over the years for a Landsat scene

Export for all Landsat images, cloud cover 70% or less, No mosaic

Export as image collection



@author: kangyanghui
"""
import ee; ee.Initialize()
import sys

def renameLandsat(image):
    """
    Function that renames Landsat bands
    """
    sensor = ee.String(image.get('SATELLITE'))
    from_list = ee.Algorithms.If(sensor.compareTo('LANDSAT_8'),
                             ['B1','B2','B3','B4','B5','B7','pixel_qa'],
                             ['B2','B3','B4','B5','B6','B7','pixel_qa'])
    to_list = ['blue','green','red','nir','swir1','swir2','pixel_qa']

    return image.select(from_list, to_list)


def getQABits(image, start, end, newName):
    """
     Function that returns an image containing just the specified QA bits.
    """
    # Compute the bits we need to extract.
    pattern = 0
    for i in range(start, end + 1):
       pattern = pattern + 2**i

    # Return a single band image of the extracted QA bits, giving the band
    # a new name.
    return image.select([0],[newName]).bitwiseAnd(pattern).rightShift(start)


def maskLST(image):
    """
    Function that masks a Landsat image based on the QA band
    """
    pixelQA = image.select('pixel_qa')
    cloud = getQABits(pixelQA, 1, 1, 'clear')
    return image.updateMask(cloud.eq(1))


def setDate(image):
    """
    Function that adds a "date" property to an image in format "YYYYmmdd"
    """

    eeDate = ee.Date(image.get('system:time_start'))
    date = eeDate.format('YYYYMMdd')
    return image.set('date',date)

def getVIs(img):
    """
    Compute VIs for an Landsat image
    """    
    SR = img.expression('float(b("nir")) / b("red")')
    NDVI = img.expression('float((b("nir") - b("red"))) / (b("nir") + b("red"))')
    EVI = img.expression('2.5 * float((b("nir") - b("red"))) / (b("nir") + 6*b("red") - 7.5*float(b("blue")) + 10000)')
    # GCI = img.expression('float(b("nir")) / b("green") - 1');
    # EVI2 = img.expression('2.5 * float((b("nir") - b("red"))) / (b("nir") + 2.4*float(b("red")) + 10000)');
    # OSAVI = img.expression('1.16 * float(b("nir") - b("red")) / (b("nir") + b("red") + 1600)');
    NDWI = img.expression('float((b("nir") - b("swir1"))) / (b("nir") + b("swir1"))')
    # NDWI2 = img.expression('float((b("nir") - b("swir2"))) / (b("nir") + b("swir2"))');
    # MSR = img.expression('float(b("nir")) / b("swir1")');
    # MTVI2 = img.expression('1.5*(1.2*float(b("nir") - b("green")) - 2.5*float(b("red") - b("green")))/sqrt((2*b("nir")+10000)*(2*b("nir")+10000) - (6*b("nir") - 5*sqrt(float(b("nir"))))-5000)');
    
    return img.addBands(SR.select([0], ['SR'])) \
              .addBands(NDVI.select([0],['NDVI'])) \
              .addBands(EVI.select([0],['EVI'])) \
              .addBands(NDWI.select([0],['NDWI']))
              # .addBands(GCI.select([0],['GCI']))
              # .addBands(EVI2.select([0],['EVI2']))
              # .addBands(OSAVI.select([0],['OSAVI']))
              # .addBands(NDWI2.select([0],['NDWI2']))
              # .addBands(MSR.select([0],['MSR']))
              # .addBands(MTVI2.select([0],['MTVI2']));

def trainRF(samples, features, classProperty):
    """
    Function that trains a Random Forest regressor
    """
    rfRegressor = ee.Classifier.randomForest(numberOfTrees=100,
                                             minLeafPopulation=20,
                                             variablesPerSplit=8) \
                                .setOutputMode('REGRESSION') \
                                .train(features=samples,classProperty='MCD_LAI',
                                       inputProperties=features)
    
    return rfRegressor


def getRFModel(sensor, biome, threshold):
    """
    Wrapper function to train RF model given biome and sensor
    "sensor" needs to be a "String"; it cannot be an EE computed object
    """
  
    assetDir = 'users/yanghui/OpenET/LAI_US/train_samples/'
    filename = 'LAI_train_samples_'+sensor+'_v10_1_labeled'   # change training sample
    train = ee.FeatureCollection(assetDir+filename)

    # filter sat_flag
    train = train.filterMetadata('sat_flag','equals','correct')
  
    # get train sample by biome
    train = ee.Algorithms.If(ee.Number(biome).eq(0), train,
                             train.filterMetadata('biome2','equals',biome))
  
    # percentage of saturated samples to use
    train = ee.FeatureCollection(train)

    """
    train = train.filterMetadata('mcd_qa','equals',1) \
                 .filterMetadata('random','less_than',threshold) \
                 .merge(train.filterMetadata('mcd_qa','equals',0))
  	"""

    # train
    features = ['red','green','nir','swir1','lat','lon','NDVI','NDWI','sun_zenith','sun_azimuth']
    rf = trainRF(train, features, 'MCD_LAI')
    return rf


def getTrainImg(image):
    """
    Function that takes an Landsat image and prepare feature bands
    """

    nlcd2011 = ee.Image('USGS/NLCD/NLCD2011')

    # NLCD processing
    year = ee.Date(image.get('system:time_start')).get('year')
    nlcd_all = ee.Image('USGS/NLCD/NLCD2001').select(['landcover'],['landcover_2001']) \
        .addBands(ee.Image('USGS/NLCD/NLCD2004').select(['landcover'],['landcover_2004'])) \
        .addBands(ee.Image('USGS/NLCD/NLCD2006').select(['landcover'],['landcover_2006'])) \
        .addBands(ee.Image('USGS/NLCD/NLCD2008').select(['landcover'],['landcover_2008'])) \
        .addBands(ee.Image('USGS/NLCD/NLCD2011').select(['landcover'],['landcover_2011'])) \
        .addBands(ee.Image('USGS/NLCD/NLCD2013').select(['landcover'],['landcover_2013'])) \
        .addBands(ee.Image('USGS/NLCD/NLCD2016').select(['landcover'],['landcover_2016']))

    nlcd_dict = {'1997':0,'1998':0,'1999':0,'2000':0,'2001':0,'2002':0,'2003':1,'2004':1,'2005':1,
        '2006':2,'2007':2,'2008':3,'2009':3,'2010':4,'2011':4,'2012':4,'2013':5,'2014':5,
        '2015':6,'2016':6,'2017':6,'2018':6,'2019':6}
    nlcd_dict = ee.Dictionary(nlcd_dict)
        
    nlcd = nlcd_all.select([nlcd_dict.get(ee.Number(year))]); # get NLCD for corresponding year
        
    # add bands
    image = maskLST(image);
    image = getVIs(image);
    mask_prev = image.select([0]).mask();

    # add other bands
    image = image.addBands(nlcd2011.select([0],['nlcd']).clip(image.geometry()))
    image = image.addBands(ee.Image.pixelLonLat().select(['longitude','latitude'],['lon','lat']).clip(image.geometry()))
    # image = image.addBands(ee.Image.constant(ft.get('year')).select([0],['year']).clip(image.geometry()));
    image = image.addBands(ee.Image.constant(ee.Number(image.get('WRS_PATH'))).select([0],['path'])).clip(image.geometry())
    image = image.addBands(ee.Image.constant(ee.Number(image.get('WRS_ROW'))).select([0],['row'])).clip(image.geometry())
    image = image.addBands(ee.Image.constant(ee.Number(image.get('SOLAR_ZENITH_ANGLE'))).select([0],['sun_zenith'])).clip(image.geometry())
    image = image.addBands(ee.Image.constant(ee.Number(image.get('SOLAR_AZIMUTH_ANGLE'))).select([0],['sun_azimuth'])).clip(image.geometry())
    # image = image.addBands(ee.Image.constant(ee.Number(ft.get('doy'))).select([0],['DOY'])).clip(image.geometry());

    # add biome band
    fromList = [21,22,23,24,31,41,42,43,52,71,81,82,90,95]
    toList = [0,0,0,0,0,1,2,3,4,5,5,6,7,8]
    image = image.addBands(image.select('nlcd').remap(fromList, toList).rename('biome2')).updateMask(mask_prev)

    return image

def getLAIforBiome(image, biome, rf_models):
    biome_band = image.select('biome2')
    model = rf_models.get(ee.String(biome))
    lai = image.updateMask(biome_band.select('biome2').eq(ee.Image.constant(ee.Number.parse(biome)))) \
               .classify(model,'LAI')
    return lai

def getLAI(image, rf_models):
    """
    Function that computes LAI for an input Landsat image and Random Forest models
    """
  
    # Add necessary bands to image
    image = getTrainImg(image)
    mask_prev = image.select([0]).mask()
  
    # Apply regressor for each biome
    biomes = rf_models.keys()
    lai_coll = ee.List(biomes).map(lambda b: getLAIforBiome(image, b, rf_models))
    lai_coll = ee.ImageCollection(lai_coll)
  
    # combine lai of all biomes
    lai_img = ee.Image(lai_coll.mean().copyProperties(image)).updateMask(mask_prev) \
        .set('system:time_start',image.get('system:time_start'))
    
    return lai_img


def trainModels(sensor, nonveg):
    """
    Function that trains biome-specific RF models for a specific sensor
    Args:
        sensor: String {'LT05','LE07','LC08'}
        nonveg: whether LAI is computed for non-vegetation pixels
    """
  
    biomes = [1,2,3,4,5,6,7,8]
    biomes_str = ['1','2','3','4','5','6','7','8']
  
    if(nonveg):
        biomes = [0,1,2,3,4,5,6,7,8]
        biomes_str = ['0','1','2','3','4','5','6','7','8']
  
    # Get models for each biome
    rf_models = ee.List(biomes).map(lambda biome: getRFModel(sensor, biome, 1))
    rf_models = ee.Dictionary.fromLists(biomes_str, rf_models)
 
    return rf_models


def getLAIImage(image, sensor, nonveg):
    """
    Main Algorithm to computer LAI for a Landsat image
    Args:
        sensor: needs to be specified as a String ('LT05','LE07','LC08')
        nonveg: True if want to compute LAI for non-vegetation pixels
    """
    # train random forest models
    # image = renameLandsat(image)
    rf_models = trainModels(sensor, nonveg)
    laiImg = getLAI(image, rf_models)
    return ee.Image(laiImg).clip(image.geometry())

def getLandsat(start, end, path, row):
    """
    Get Landsat image collection
    """
    # Landsat 8
    Landsat8_sr = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR') \
        .filterDate(start, end) \
        .filterMetadata('WRS_PATH','equals',path) \
        .filterMetadata('WRS_ROW','equals',row) \
        .filterMetadata('CLOUD_COVER','less_than',70) \
        .select(['B2','B3','B4','B5','B6','B7','pixel_qa'],
                ['blue','green','red','nir','swir1','swir2','pixel_qa']) \
        .map(maskLST)
    
    # Landsat 7
    Landsat7_sr = ee.ImageCollection('LANDSAT/LE07/C01/T1_SR')  \
        .filterDate(start, end) \
        .filterMetadata('WRS_PATH','equals',path) \
        .filterMetadata('WRS_ROW','equals',row) \
        .filterMetadata('CLOUD_COVER','less_than',70) \
        .select(['B1','B2','B3','B4','B5','B7','pixel_qa'],
                ['blue','green','red','nir','swir1','swir2','pixel_qa']) \
        .map(maskLST)

    # Landsat 5  
    Landsat5_sr = ee.ImageCollection('LANDSAT/LT05/C01/T1_SR') \
        .filterDate(start, end) \
        .filterMetadata('WRS_PATH','equals',path) \
        .filterMetadata('WRS_ROW','equals',row) \
        .filterMetadata('CLOUD_COVER','less_than',70) \
        .select(['B1','B2','B3','B4','B5','B7','pixel_qa'],
                ['blue','green','red','nir','swir1','swir2','pixel_qa']) \
        .map(maskLST)
  
    Landsat_sr_coll = Landsat8_sr.merge(Landsat5_sr).merge(Landsat7_sr).map(setDate)

    return Landsat_sr_coll


def getLAIColl(image_coll, sensor, nonveg):
    """
    Function that get LAI image collection for a specified sensor
    """
    out = image_coll.map(lambda image: getLAIImage(image, sensor, nonveg))
    return out


def getLAIColl_all(start, end, path, row):
    """
    Get Landsat LAI for an Landsat image collection
    including all possible images from any sensors
    """
    # Get Landsat collection
    landsat_coll = getLandsat(start, end, path, row)
    # print('landsat size', landsat_coll.size().getInfo())
    # print('landsat:',landsat_coll.limit(2).getInfo())
 
    l5_coll = landsat_coll.filterMetadata('SATELLITE','equals','LANDSAT_5')
    l7_coll = landsat_coll.filterMetadata('SATELLITE','equals','LANDSAT_7')
    l8_coll = landsat_coll.filterMetadata('SATELLITE','equals','LANDSAT_8')
  
    # Get RF model
    nonveg = 1
    l5_lai_coll = ee.Algorithms.If(l5_coll.size(),getLAIColl(l5_coll,'LT05',nonveg),ee.ImageCollection([]))
    l7_lai_coll = ee.Algorithms.If(l7_coll.size(),getLAIColl(l7_coll,'LE07',nonveg),ee.ImageCollection([]))
    l8_lai_coll = ee.Algorithms.If(l8_coll.size(),getLAIColl(l8_coll,'LC08',nonveg),ee.ImageCollection([]))
    l5_lai_coll = ee.ImageCollection(l5_lai_coll)
    l7_lai_coll = ee.ImageCollection(l7_lai_coll)
    l8_lai_coll = ee.ImageCollection(l8_lai_coll)
    # print('l5 LAI collection',l5_lai_coll.limit(10))
    # print('l8 LAI collection',l8_lai_coll.size().getInfo())
    # print(l8_lai_coll.limit(2).getInfo())
    
    lai_coll = l5_lai_coll.merge(l7_lai_coll).merge(l8_lai_coll) \
        .map(lambda img: img.set())
    # print(lai_coll.limit(10))
  
    return lai_coll


def main(argv):

    # Set path row
    path = int(argv[1])
    row = int(argv[2])
    start_year = int(argv[3])
    end_year = int(argv[4])
    version = int(argv[5])

    pathrow = str(path).zfill(3)+str(row).zfill(3)
    assetDir = 'projects/disalexi/example_data/LAI/LAI_'+pathrow+'_v'+str(version)+'/'

    start = str(start_year)+'-01-01'
    end = str(end_year+1)+'-01-01'

    # Get Landsat collection
    landsat_coll = getLandsat(start, end, path, row)
    landsat_coll = landsat_coll.sort('system:time_start')

    n = landsat_coll.size().getInfo()
    print(n)
    # print(laiColl.limit(10).getInfo())
    sys.stdout.flush()

    # print(laiColl.limit(10).getInfo())

    for i in range(n):

        landsat_image = ee.Image(landsat_coll.toList(5000).get(i))
        # print(landsat_image.getInfo())

        eedate = ee.Date(landsat_image.get('system:time_start'))
        date = eedate.format('YYYYMMdd').getInfo()

        sensor_dict = {'LANDSAT_5':'LT05','LANDSAT_7':'LE07','LANDSAT_8':'LC08'}
        sensor = landsat_image.get('SATELLITE').getInfo()
        sensor = sensor_dict[sensor]

        proj = landsat_image.select([0]).projection().getInfo()
        crs = proj['crs']
        # transform = ee.Projection(landsat_image.select([0]).projection()).transform().getInfo()
        transform = proj['transform']
        scale = ee.Projection(landsat_image.select([0]).projection()).nominalScale().getInfo()
        print(date, crs, scale, str(transform))

        nonveg = 1
        laiImage = getLAIImage(landsat_image, sensor, nonveg)
        
        # date = laiImage.get('date')
        # outname = 'LAI_' + date.getInfo()
        # print(outname)
        outname = 'LAI_' + pathrow + '_' + date + '_' + sensor + '_v' + str(version) 


        task = ee.batch.Export.image.toAsset(image = laiImage,
                                             description = outname,
                                             assetId = assetDir+outname,
                                             # region = subset.getInfo()['corrdinates'],
                                             crs = crs,
                                             crsTransform = str(transform))
      
        
        task.start()  # submit task
        task_id = task.id
        print(task_id,outname)
        
        sys.stdout.flush()


if __name__ == "__main__":
    main(sys.argv)