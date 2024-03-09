import ee


# def lazy_property(fn):
#     """Decorator that makes a property lazy-evaluated
#
#     https://stevenloria.com/lazy-properties/
#     """
#     attr_name = '_lazy_' + fn.__name__
#
#     @property
#     def _lazy_property(self):
#         if not hasattr(self, attr_name):
#             setattr(self, attr_name, fn(self))
#         return getattr(self, attr_name)
#
#     return _lazy_property


class Model:
    def __init__(self, image):
        """

        Parameters
        ----------
        image : ee.Image
            "Prepped" Landsat image with standardized bands names
                (i.e. blue, green, red, nir, swir1, swir2, lst).
            Thermal band must be named 'lst' and be in units of Kelvin.
            Must have property 'SPACECRAFT_ID' with a value of 'LANDSAT_4',
                'LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8', or 'LANDSAT_9'.

        """
        self.image = image

    # TODO: Decide if this should be a lazy property since it doesn't have inputs
    # @lazy_property
    def sharpen(self):
        """Thermal sharpening algorithm

        Global-RF and local-SLR and a residual redistribution process

        Returns
        -------
        ee.Image

        References
        ----------
        https://www.mdpi.com/2072-4292/4/11/3287/htm

        """
        tir_res_dict = ee.Dictionary({
            'LANDSAT_4': 120, 'LANDSAT_5': 120, 'LANDSAT_7': 60,
            'LANDSAT_8': 100, 'LANDSAT_9': 100,
        })
        tir_res = ee.Number(tir_res_dict.get(self.image.get('SPACECRAFT_ID')))

        # Apply energy conservation step with a slighter large window to reduce blurry effect
        ec_window_dict = ee.Dictionary({
            'LANDSAT_4': 120, 'LANDSAT_5': 120, 'LANDSAT_7': 90,
            'LANDSAT_8': 120, 'LANDSAT_9': 120,
        })
        ec_window = ee.Number(ec_window_dict.get(self.image.get('SPACECRAFT_ID')))

        kernel_size = 20  # kernel radius for local linear regression,
        # lower values for more heterogeneous areas
        cv_threshold = 0.15  # threshold to select homogeneous pixels
        bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']  # predictor bands

        # TODO: Call from __init__?
        bound = self.image.geometry()
        crs = self.image.projection().crs()
        transform = self.getAffineTransform()

        tir_transform = transform.set(0, tir_res).set(4, tir_res.multiply(-1))
        ec_transform = transform.set(0, ec_window).set(4, ec_window.multiply(-1))

        # Aggregate the TIR band (result of the cubic convolution interpolation)
        # Convert to brightness temperature or radiance
        tir = (
            self.image.select(['lst'])
            .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True)
            .reproject(crs=crs, crsTransform=tir_transform)
            .pow(4)
        )

        # Aggregating predictor bands for mean value
        other = self.image.select(bands)
        other_mean = (
            other.reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True)
            .reproject(crs=crs, crsTransform=tir_transform)
        )

        # Aggregating predictor bands for std value
        other_std = (
            other.reduceResolution(reducer=ee.Reducer.stdDev(), bestEffort=True)
            .reproject(crs=crs, crsTransform=tir_transform)
        )

        # Compute the coefficient of variation of sub-pixel reflectance
        other_cv = other_std.divide(other_mean).reduce(ee.Reducer.mean())

        # Add a bias band (=1) to the image for linear regression reducer
        # Add the LST image back in
        image_agg = other_mean.addBands([
            other_mean.select([0]).multiply(0).add(1).rename(['bias']), tir
        ])

        # Fit moving-window linear regressions at coarse resolution
        # Y: tir (power 4)
        # X: SR Bands
        kernel = ee.Kernel.square(kernel_size)
        local_fit = image_agg.reduceNeighborhood(
            ee.Reducer.linearRegression(len(bands) + 1, 1), kernel, None, False
        )

        # Extract coefficients
        # Use crsTransform instead of scale to avoid misalignment
        band_names = bands.copy()
        band_names.extend(['bias'])
        coefficients = (
            local_fit.select('coefficients')
            .arrayProject([0])
            .arrayFlatten([band_names])
            .reproject(crs, transform)
        )

        # CGM - Not used below, commenting out
        # rmse = local_fit.select('residuals').arrayFlatten([['residuals']]).pow(0.25)

        # Apply linear fit at high resolution for sharpened TIR
        inputs = self.image.select(bands).addBands([
            self.image.select([0]).multiply(0).add(1).rename(['bias'])
        ])
        tir_sp_local = inputs.multiply(coefficients).reduce(ee.Reducer.sum()).pow(0.25)

        # Fit a scene-wise random forest model
        # Select homogeneous samples defined by a threshold in c.v.
        samples = (
            image_agg.updateMask(other_cv.lt(cv_threshold))
            .sample(region=bound, scale=tir_res, factor=5e-3)
        )

        # YK - Update: use smileRandomForest
        rf = (
            ee.Classifier.smileRandomForest(100, 4, 50)
            .setOutputMode('REGRESSION')
            .train(samples, 'lst', bands)
        )

        # Apply RF to local resolution (SR bands)
        tir_sp_global = self.image.classify(rf, 'lst_pred').pow(0.25)

        """ Residual analysis """
        # Aggregate local model results to tir resolution
        local_agg = (
            tir_sp_local.pow(4)
            .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True)
            .reproject(crs=crs, crsTransform=tir_transform)
            .pow(0.25)
        )

        # Aggregate global model results to tir resolution
        global_agg = (
            tir_sp_global.pow(4)
            .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True)
            .reproject(crs=crs, crsTransform=tir_transform)
            .pow(0.25)
        )

        # Compute weights based on residuals at coarse resolution
        # YK - Update: use existing images instead of ee.Image(1)
        res_local = local_agg.pow(4).subtract(tir).abs()
        res_global = global_agg.pow(4).subtract(tir).abs()
        ## res_local_part = ee.Image(1).divide(res_local.pow(2))
        # res_global_part = ee.Image(1).divide(res_global.pow(2))
        res_local_part = res_local.multiply(0).add(1).divide(res_local.pow(2))
        res_global_part = res_global.multiply(0).add(1).divide(res_global.pow(2))
        weight_local = res_local_part.divide(res_local_part.add(res_global_part))
        weight_global = res_global_part.divide(res_local_part.add(res_global_part))

        # No residual equals to 0
        valid_mask = res_local.gt(0).multiply(res_global.gt(0))

        # Compute weighted average of local and global model results
        # Weighted sum
        tir_sp_final = (
            tir_sp_local.pow(4).multiply(weight_local)
            .add(tir_sp_global.pow(4).multiply(weight_global))
            .multiply(valid_mask).pow(0.25)
        )
        # Local residual is 0
        tir_sp_final = tir_sp_final.add(tir_sp_local.multiply(res_local.eq(0)))
        # Global residual is 0
        tir_sp_final = tir_sp_final.add(tir_sp_global.multiply(res_global.eq(0)))

        """ YK - UPDATE: Apply Energy Conservation to combined sharpened image """
        # Aggregated sharpened image
        tir_sp_agg = (
            tir_sp_final
            .pow(4)
            .reduceResolution(ee.Reducer.mean())
            .reproject(crs=crs, crsTransform=ec_transform)
            .pow(0.25)
        )
        # Aggregate original TIR image
        tir_org_agg = (
            self.image.select(['lst'])
            .pow(4)
            .reduceResolution(ee.Reducer.mean())
            .reproject(crs=crs, crsTransform=ec_transform)
            .pow(0.25)
        )

        # Interpolate residuals
        res_sp = tir_sp_agg.subtract(tir_org_agg)
        res_conv = res_sp.resample('bilinear').reproject(crs, transform)

        # Add interpolated residual back to sharpened image
        tir_sp_ec = tir_sp_final.subtract(res_conv)

        # Prepare output
        out = tir_sp_ec.rename(['lst_sharpened'])

        # # TODO: Add flag/parameter to control if all bands are exported
        # out = (
        #     out.addBands(tir_sp_final.rename(['tir_sharpened_non_ec']))
        #     .addBands(image.select('tir').rename(['tir_original']))
        #     .addBands(tir_sp_local.rename(['tir_sp_local']))
        #     .addBands(tir_sp_global.rename(['tir_sp_global']))
        #     .addBands(weight_local.rename(['local_weights']))
        #     .addBands(rmse.rename(['slr_rmse']))
        # )

        # CGM - Commenting out adding the other bands for now
        # out = (
        #     out.addBands(image.select('tir').rename(['tir_original']))
        #     .addBands(tir.pow(0.25).rename(['tir_agg']))
        #     .addBands(tir_sp_local.rename(['tir_sp_local']))
        #     .addBands(tir_sp_global.rename(['tir_sp_global']))
        #     .addBands(local_agg.rename(['tir_local_agg']))
        #     .addBands(global_agg.rename(['tir_global_agg']))
        #     .addBands(weight_local.rename(['local_weights']))
        #     .addBands(rmse.rename(['slr_rmse']))
        # )

        # CGM - copyProperties sometimes drops the type
        out = ee.Image(out.copyProperties(self.image)) \
            .set({'system:time_start': self.image.get('system:time_start'),
                  'energy_conservation': 'True',
                  })

        return out

    def getAffineTransform(self):
        projection = self.image.projection()
        json = ee.Dictionary(ee.Algorithms.Describe(projection))
        return ee.List(json.get('transform'))
