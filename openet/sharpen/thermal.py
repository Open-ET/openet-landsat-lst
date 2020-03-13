import ee

from . import utils


def thermalSharpening(image):
    """
    Thermal sharpening algorithm with global-RF and local-SLR and a residual redistribution process

    Args:
        image: a Landsat image, need to be pre-quality-masked; bands need to be renamed
    """

    # settings
    tir_res_dict = ee.Dictionary({
        'LANDSAT_5': 120, 'LANDSAT_7': 60, 'LANDSAT_8': 100})
    tir_res = ee.Number(tir_res_dict.get(image.get('SATELLITE')))
    # high_res = 30

    kernel_size = 20  # kernel radius for local linear regression,
    # lower values for more heterogeneous areas
    cv_threshold = 0.15  # threshold to select homogeneous pixels
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']  # predictor bands

    bound = image.geometry()
    crs = image.projection().crs()
    transform = utils.getAffineTransform(image)

    tir_transform = transform.set(0, tir_res).set(4, tir_res.multiply(-1))

    # aggregate the TIR band (result of the cubic convolution interpolation)
    # convert to brightness temperature or radiance
    tir = image.select(['tir']) \
        .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True) \
        .reproject(crs=crs, scale=tir_res) \
        .divide(10).pow(4)

    # aggregating predictor bands for mean value
    other = image.select(bands)
    other_mean = other \
        .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True) \
        .reproject(crs=crs, scale=tir_res)

    # aggregating predictor bands for std value
    other_std = other \
        .reduceResolution(reducer=ee.Reducer.stdDev(), bestEffort=True) \
        .reproject(crs=crs, scale=tir_res)

    # compute the coefficient of variation of sub-pixel reflectance
    other_cv = other_std.divide(other_mean).reduce(ee.Reducer.mean()) \
        .rename(['mean_cv'])

    # add a bias band (=1) to the image for linear regression reducer
    image_agg = other_mean \
        .addBands(ee.Image(1).clip(bound).rename(['bias'])) \
        .addBands(tir) \
        .clip(bound)

    # Fit moving-window linear regressions at coarse resolution
    # Y: tir (power 4)
    # X: SR Bands
    kernel = ee.Kernel.square(kernel_size)
    local_fit = image_agg.reduceNeighborhood(
        ee.Reducer.linearRegression(len(bands) + 1, 1), kernel, None, False)

    # extract coefficients
    # use crsTransform instead of scale to avoid misalignment
    band_names = bands.copy()
    band_names.extend(['bias'])
    coefficients = local_fit.select('coefficients').arrayProject([0]) \
        .arrayFlatten([band_names]) \
        .reproject(crs, transform)

    rmse = local_fit.select('residuals').arrayFlatten([['residuals']]).pow(0.25)

    # Apply linear fit at high resolution for sharpened TIR
    inputs = image.select(bands).addBands(
        ee.Image(1).clip(bound).rename(['bias']))
    tir_sp_local = inputs.multiply(coefficients).reduce(ee.Reducer.sum())\
        .pow(0.25)

    # Fit a scene-wise random forest model
    # select homogeneous samples defined by a threshold in c.v.
    samples = image_agg \
        .updateMask(other_cv.lt(cv_threshold)) \
        .sample(region=bound, scale=tir_res, factor=5e-3)

    rf = ee.Classifier.randomForest(100, 4, 50) \
        .setOutputMode('REGRESSION') \
        .train(samples, 'tir', bands)

    # Apply RF to local resolution (SR bands)
    tir_sp_global = image.classify(rf, 'tir_pred').pow(0.25)

    """ Residual analysis """
    # aggregate local model results to tir resolution
    local_agg = tir_sp_local.pow(4) \
        .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True) \
        .reproject(crs=crs, scale=tir_res).pow(0.25)

    # aggregate global model results to tir resolution
    global_agg = tir_sp_global.pow(4) \
        .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True) \
        .reproject(crs=crs, scale=tir_res).pow(0.25)

    # compute weights based on residuals at coarese resolution
    res_local = local_agg.pow(4).subtract(tir).abs()
    res_global = global_agg.pow(4).subtract(tir).abs()
    res_local_part = ee.Image(1).divide(res_local.pow(2))
    res_global_part = ee.Image(1).divide(res_global.pow(2))
    weight_local = res_local_part.divide(res_local_part.add(res_global_part))
    weight_global = res_global_part.divide(res_local_part.add(res_global_part))

    # no residual equals to 0
    valid_mask = res_local.gt(0).multiply(res_global.gt(0))

    # compute weighted average of local and global model results
    # weighted sum
    tir_sp_final = tir_sp_local.pow(4).multiply(weight_local) \
        .add(tir_sp_global.pow(4).multiply(weight_global)) \
        .multiply(valid_mask).pow(0.25)
    # local residual is 0
    tir_sp_final = tir_sp_final.add(tir_sp_local.multiply(res_local.eq(0)))
    # global residual is 0
    tir_sp_final = tir_sp_final.add(tir_sp_global.multiply(res_global.eq(0)))

    # prepare output
    out = tir_sp_final.rename(['tir_sharpened'])

    out = out.addBands(image.select('tir').divide(10).rename(['tir_original'])) \
        .addBands(tir.pow(0.25).rename(['tir_agg'])) \
        .addBands(tir_sp_local.rename(['tir_sp_local'])) \
        .addBands(tir_sp_global.rename(['tir_sp_global'])) \
        .addBands(local_agg.rename(['tir_local_agg'])) \
        .addBands(global_agg.rename(['tir_global_agg'])) \
        .addBands(weight_local.rename(['local_weights'])) \
        .addBands(rmse.rename(['slr_rmse']))

    # need to reproject to original crs with transform to avoid misalignment
    out = out.reproject(crs, transform)

    out = out.clip(bound).multiply(10).int16()

    out = out.copyProperties(image) \
        .set('system:time_start', image.get('system:time_start'))

    return ee.Image(out)
