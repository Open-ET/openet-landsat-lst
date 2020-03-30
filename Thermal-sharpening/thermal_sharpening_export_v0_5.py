#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 16:33:56 2020

General sharpened thermal band for a Landsat image
Export thermal sharpening results to a asset image collection

Method overview:
1. Build a global RF regressor for scene-wise sharpening
2. Build a moving-window local sharpener based on linear regression
3. Residual at aggregated resolution is redistributed to combine local and global results

Update:
  - Fix the pixel mis-alignment in reprojecting the local coefficients to original projection
  - Reproject the final results to original projection with transform
  - Need ot use crsTransform in export function as well

@author: kangyanghui
"""
import sys

import ee

import openet.sharpen

ee.Initialize()


def getQABits(image, start, end, newName):
    
  """
   Function that returns an image containing just the specified QA bits.
  """
  # Compute the bits we need to extract.
  pattern = 0
  for i in range(start, end + 1):
    pattern = pattern + 2 ** i

  # Return a single band image of the extracted QA bits, giving the band
  # a new name.
  return image.select([0],[newName]).bitwiseAnd(pattern).rightShift(start)


def qaLandsat(image):
  """
  Apply cloud masking to a Landsat image
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
    'L8': ee.List([1, 2, 3, 4, 5, 7, 6, 'pixel_qa']),
    'L7': ee.List([0, 1, 2, 3, 4, 5, 6, 'pixel_qa']),
    'L5': ee.List([0, 1, 2, 3, 4, 5, 6, 'pixel_qa']),
  }

  # Set up collections
  collectionDict = {
    'L8': 'LANDSAT/LC08/C01/T1_SR',
    'L7': 'LANDSAT/LE07/C01/T1_SR',
    'L5': 'LANDSAT/LT05/C01/T1_SR',
  }

  sensorBandName = [
    'blue', 'green', 'red', 'nir', 'swir1', 'tir', 'swir2', 'pixel_qa']

  # Landsat 8
  Landsat8_sr = ee.ImageCollection(collectionDict['L8']) \
    .filterDate(start, end) \
    .filterMetadata('WRS_PATH', 'equals', path) \
    .filterMetadata('WRS_ROW', 'equals', row) \
    .filterMetadata('CLOUD_COVER','less_than',70) \
    .select(sensorBandDict['L8'], sensorBandName) \
    .map(qaLandsat)
  
  # Landsat 7
  Landsat7_sr = ee.ImageCollection(collectionDict['L7'])  \
    .filterDate(start, end) \
    .filterMetadata('WRS_PATH', 'equals', path) \
    .filterMetadata('WRS_ROW', 'equals', row) \
    .filterMetadata('CLOUD_COVER', 'less_than', 70) \
    .select(sensorBandDict['L7'], sensorBandName) \
    .map(qaLandsat)

  # Landsat 5  
  Landsat5_sr = ee.ImageCollection(collectionDict['L5']) \
    .filterDate(start, end) \
    .filterMetadata('WRS_PATH', 'equals', path) \
    .filterMetadata('WRS_ROW', 'equals', row) \
    .filterMetadata('CLOUD_COVER', 'less_than', 70) \
    .select(sensorBandDict['L5'], sensorBandName) \
    .map(qaLandsat)

  Landsat_sr_coll = Landsat8_sr.merge(Landsat5_sr).merge(Landsat7_sr)

  return Landsat_sr_coll


def getSharpenedTir(start, end, path, row):

  landsat_coll = getLandsat(start, end, path, row)
  sharpened_tir = landsat_coll.map(openet.sharpen.thermal.thermalSharpening)
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
  
  assetDir = 'users/cgmorton/example_data/LST/LST_' + pathrow + '_v1/'
  # assetDir = 'projects/disalexi/example_data/LST/LST_' + pathrow + '_v1/'

  start = str(start_year) + '-01-01'
  end = str(end_year+1) + '-01-01'

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

    tir_sp_image = openet.sharpen.thermal.landsat(landsat_image)

    # TODO: Cast output image to an integer to match input

    eedate = ee.Date(landsat_image.get('system:time_start'))
    date = eedate.format('YYYYMMdd').getInfo()

    sensor_dict = {'LANDSAT_5': 'LT05', 'LANDSAT_7': 'LE07', 'LANDSAT_8': 'LC08'}
    sensor = landsat_image.get('SATELLITE').getInfo()
    sensor = sensor_dict[sensor]

    proj = landsat_image.select([0]).projection().getInfo()
    crs = proj['crs']
    # transform = ee.Projection(landsat_image.select([0]).projection()).transform().getInfo()
    transform = proj['transform']
    scale = ee.Projection(landsat_image.select([0]).projection())\
      .nominalScale().getInfo()
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

    task = ee.batch.Export.image.toAsset(image=tir_sp_image,
                                         description=outname,
                                         assetId=assetDir+outname,
                                         # region=subset.getInfo()['coordinates'],
                                         crs=crs,
                                         # scale=scale)
                                         crsTransform=str(transform)) # the input has to be a string '[a,b,c]'

    task.start()  # submit task
    task_id = task.id
    print(task_id, outname)

    sys.stdout.flush()

if __name__ == "__main__":
  main(sys.argv)
