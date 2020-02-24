#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 16:33:56 2020

Genearl sharpened thermal band for a Landsat image
Export thermal sharpening results to a asset image collection

Method overview:
1. Build a global RF regressor for scene-wise sharpening
2. Build a moving-window local sharpener based on linear regression
3. Residual at aggregated resolution is redistributed to combine local and globla results

Update:
  - Fix the pixel mis-alignment in reprojecting the local coefficients to original projection
  - Reproject the final results to original projection with transform
  - Need ot use crsTransform in export function as well

@author: kangyanghui
"""

import ee; ee.Initialize()
import sys

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


def qaLandsat(image):
  """
  Apply cloud masking to a Lndsat image
  """
  pixelQA = image.select('pixel_qa')
  clear = getQABits(pixelQA, 1, 1, 'clear')
  shadow = getQABits(pixelQA, 3, 3, 'shadow')
  cloud = getQABits(pixelQA, 5, 5, 'cloud')
  cirrus = getQABits(pixelQA, 8, 9, 'cirrus') # low confidence or none

  return image.updateMask(clear.eq(1))
    # .updateMask(shadow.eq(0)) \
    # .updateMask(cloud.eq(0)) \
    # .updateMask(cirrus.lt(2))


def getAffineTransform(image):
  projection = image.projection()
  json = ee.Dictionary(ee.Algorithms.Describe(projection))
  return ee.List(json.get('transform'))

def getLandsat(start, end, path, row):
  """
  Get Landsat image collection
  """

  # Set up bands and corresponding band names
  sensorBandDict = {
    'L8': ee.List([1,2,3,4,5,7,6,'pixel_qa']),
    'L7': ee.List([0,1,2,3,4,5,6,'pixel_qa']),
    'L5': ee.List([0,1,2,3,4,5,6,'pixel_qa']),
  };

  # Set up collections
  collectionDict = {
    'L8': 'LANDSAT/LC08/C01/T1_SR',
    'L7': 'LANDSAT/LE07/C01/T1_SR',
    'L5': 'LANDSAT/LT05/C01/T1_SR',
  };

  sensorBandName = ['blue','green','red','nir','swir1','tir', 'swir2','pixel_qa'];

  # Landsat 8
  Landsat8_sr = ee.ImageCollection(collectionDict['L8']) \
    .filterDate(start, end) \
    .filterMetadata('WRS_PATH','equals',path) \
    .filterMetadata('WRS_ROW','equals',row) \
    .filterMetadata('CLOUD_COVER','less_than',70) \
    .select(sensorBandDict['L8'], sensorBandName) \
    .map(qaLandsat)
  
  # Landsat 7
  Landsat7_sr = ee.ImageCollection(collectionDict['L7'])  \
    .filterDate(start, end) \
    .filterMetadata('WRS_PATH','equals',path) \
    .filterMetadata('WRS_ROW','equals',row) \
    .filterMetadata('CLOUD_COVER','less_than',70) \
    .select(sensorBandDict['L7'], sensorBandName) \
    .map(qaLandsat)

  # Landsat 5  
  Landsat5_sr = ee.ImageCollection(collectionDict['L5']) \
      .filterDate(start, end) \
      .filterMetadata('WRS_PATH','equals',path) \
      .filterMetadata('WRS_ROW','equals',row) \
      .filterMetadata('CLOUD_COVER','less_than',70) \
      .select(sensorBandDict['L5'], sensorBandName) \
      .map(qaLandsat)

  Landsat_sr_coll = Landsat8_sr.merge(Landsat5_sr).merge(Landsat7_sr)

  return Landsat_sr_coll


def thermalSharpening(image):
  """
  Thermal sharpening algorithm with global-RF and local-SLR and a residual redistribution process

  Args:
    image: a Landsat image, need to be pre-quality-masked; bands need to be renamed
  """

  # settings
  tir_res_dict = ee.Dictionary({'LANDSAT_5':120,'LANDSAT_7':60,'LANDSAT_8':100})
  tir_res = tir_res_dict.get(image.get('SATELLITE'))
  # high_res = 30

  kernel_size = 20  # kernel radius for local linear regression, 
                    # lower values for more heterogenous areas
  cv_threshold = 0.15  # threshold to select homogenous pixels
  bands = ['blue','green','red','nir','swir1','swir2']  # predictor bands

  bound = image.geometry()
  crs = image.projection().crs()
  transform = getAffineTransform(image)

  # aggregate the TIR band (result of the cubic convulution interpolation)
  tir = image.select(['tir']) \
    .reduceResolution(reducer=ee.Reducer.mean(),bestEffort=True) \
    .reproject(crs=crs,scale=tir_res) \
    .divide(10).pow(4)  # convert to brightness temperature or radiance 

  # aggregating predictor bands for mean value
  other = image.select(bands)
  other_mean = other \
      .reduceResolution(reducer=ee.Reducer.mean(),bestEffort=True) \
      .reproject(crs=crs,scale=tir_res)

  # aggregating predictor bands for std value
  other_std = other \
    .reduceResolution(reducer=ee.Reducer.stdDev(),bestEffort=True) \
    .reproject(crs=crs,scale=tir_res)

  # compute the coefficient of variation of sub-pixel reflectance
  other_cv = other_std.divide(other_mean).reduce(ee.Reducer.mean()).rename(['mean_cv'])

  # add a bias band (=1) to the image for linear regression reducer
  image_agg = other_mean \
    .addBands(ee.Image(1).clip(bound).rename(['bias'])) \
    .addBands(tir) \
    .clip(bound)

  # Fit moving-window linear regressions at coarse resolution 
  # Y: tir (power 4)
  # X: SR Bands
  kernel = ee.Kernel.square(kernel_size)
  local_fit = image_agg.reduceNeighborhood(ee.Reducer.linearRegression(len(bands)+1,1), kernel, None, False)

  # extract coefficients
  band_names = bands.copy()
  band_names.extend(['bias'])
  coefficients = local_fit.select('coefficients').arrayProject([0]) \
    .arrayFlatten([band_names]) \
    .reproject(crs,transform) # use crsTransform instead of scale to avoid misalignment

  rmse = local_fit.select('residuals').arrayFlatten([['residuals']]).pow(0.25)

  # Apply linear fit at high resolution for shaprened TIR
  inputs = image.select(bands).addBands(ee.Image(1).clip(bound).rename(['bias']))
  tir_sp_local = inputs.multiply(coefficients).reduce(ee.Reducer.sum()).pow(0.25)

  # Fit a scene-wise random forest model
  # select homogeneous samples defined by a threshold in c.v.
  samples = image_agg \
    .updateMask(other_cv.lt(cv_threshold))  \
    .sample(region=bound, scale=tir_res, factor=5e-3)

  rf = ee.Classifier.randomForest(100, 4, 50) \
    .setOutputMode('REGRESSION') \
    .train(samples,'tir',bands)

  # Apply RF to local resolution (SR bands)
  tir_sp_global = image.classify(rf, 'tir_pred').pow(0.25)

  """ Residual analysis """
  # aggregate local model results to tir resolution
  local_agg = tir_sp_local.pow(4) \
    .reduceResolution(reducer=ee.Reducer.mean(),bestEffort=True) \
    .reproject(crs=crs,scale=tir_res).pow(0.25)

  # aggregate global model results to tir resolution
  global_agg = tir_sp_global.pow(4) \
    .reduceResolution(reducer=ee.Reducer.mean(),bestEffort=True) \
    .reproject(crs=crs,scale=tir_res).pow(0.25)

  # compute weights based on residuals at coarese resolution
  res_local = local_agg.pow(4).subtract(tir).abs()
  res_global = global_agg.pow(4).subtract(tir).abs()
  res_local_part = ee.Image(1).divide(res_local.pow(2))
  res_global_part = ee.Image(1).divide(res_global.pow(2))
  weight_local = res_local_part.divide(res_local_part.add(res_global_part))
  weight_global = res_global_part.divide(res_local_part.add(res_global_part))

  valid_mask = res_local.gt(0).multiply(res_global.gt(0)) # no residual equals to 0

  # compute weighted average of local and global model results
  tir_sp_final = tir_sp_local.pow(4).multiply(weight_local) \
      .add(tir_sp_global.pow(4).multiply(weight_global)).multiply(valid_mask).pow(0.25)  # weighted sum
  tir_sp_final = tir_sp_final.add(tir_sp_local.multiply(res_local.eq(0)))  # local residual is 0
  tir_sp_final = tir_sp_final.add(tir_sp_global.multiply(res_global.eq(0)))  # global residual is 0

  # prepare output
  out = tir_sp_final.rename(['tir_sharpend'])
  
  out = out.addBands(image.select('tir').divide(10).rename(['tir_original'])) \
    .addBands(tir.pow(0.25).rename(['tir_agg'])) \
    .addBands(tir_sp_local.rename(['tir_sp_local'])) \
    .addBands(tir_sp_global.rename(['tir_sp_global'])) \
    .addBands(local_agg.rename(['tir_local_agg'])) \
    .addBands(global_agg.rename(['tir_global_agg'])) \
    .addBands(weight_local.rename(['local_weights'])) \
    .addBands(rmse.rename(['slr_rmse']))

  out = out.reproject(crs,transform)  # need to reproject to original crs with transform to avoid misalignment
  
  out = out.clip(bound).multiply(10).int16()

  return ee.Image(out.copyProperties(image).set('system:time_start',image.get('system:time_start')))


def getSharpenedTir(start, end, path, row):

  landsat_coll = getLandsat(start, end, path, row)
  sharpened_tir = landsat_coll.map(thermalSharpening)
  sharpened_tir = sharpened_tir.sort('system:time_start')

  return sharpened_tir


def main(argv):

  # argv = [0,'42','34','2016','2018']

  # Set path row
  path = int(argv[1])
  row = int(argv[2])
  start_year = int(argv[3])
  end_year = int(argv[4])
  pathrow = str(path).zfill(3)+str(row).zfill(3)
  
  assetDir = 'projects/disalexi/example_data/LST/LST_'+pathrow'_v1/'

  start = str(start_year)+'-01-01'
  end = str(end_year+1)+'-01-01'

  landsat_coll = getLandsat(start, end, path, row)
  landsat_coll = landsat_coll.sort('system:time_start')

  # sharpened tir collection
  # sharpened = getSharpenedTir(start, end, path, row)
  n = landsat_coll.size().getInfo()
  print(n)
  # print(laiColl.limit(10).getInfo())
  sys.stdout.flush()

  for i in range(n):

      landsat_image = ee.Image(landsat_coll.toList(5000).get(i))

      tir_sp_image = thermalSharpening(landsat_image)

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
      
      # date = laiImage.get('date')
      # outname = 'LAI_' + date.getInfo()
      # print(outname)
      outname = 'LST_' + pathrow + '_' + date + '_' + sensor + '_v1'
      print(outname)

      # export window (small)
      """
      subset = ee.Geometry.Polygon([[[-122.00927602072072, 38.890298881752535],
            [-122.00927602072072, 38.855684804880134],
            [-121.95777760763478, 38.855684804880134],
            [-121.95777760763478, 38.890298881752535]]], None, False)
      """

      task = ee.batch.Export.image.toAsset(image = tir_sp_image,
                                           description = outname,
                                           assetId = assetDir+outname,
                                           # region = subset.getInfo()['coordinates'],
                                           crs = crs,
                                           # scale = scale)
                                           crsTransform = str(transform)) # the input has to be a string '[a,b,c]'
        
      task.start()  # submit task
      task_id = task.id
      print(task_id,outname)
      
      sys.stdout.flush()

if __name__ == "__main__":
  main(sys.argv)