import re

import ee
import openet.core

from .model import Model


class Landsat(object):
    # CGM - Using the __new__ to return is discouraged and is probably not
    #   great Python but it was the only way I could find to make the general
    #   Landsat class directly callable like the collection specific ones
    # def __init__(self):
    #     """"""
    #     pass

    def __new__(cls, image_id, c2_lst_correct=True):
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif re.match('LANDSAT/L[TEC]0[45789]/C02/T1_L2', image_id):
            return Landsat_C02_L2(image_id, c2_lst_correct=c2_lst_correct)
        else:
            raise ValueError('unsupported image_id')


class Landsat_C02_L2(Model):
    def __init__(self, image_id, c2_lst_correct=True):
        """"""
        # TODO: Support input being an ee.Image
        # For now assume input is always an image ID
        if type(image_id) is not str:
            raise ValueError('unsupported input type')
        elif (image_id.startswith('LANDSAT/') and
                not re.match('LANDSAT/L[TEC]0[45789]/C02/T1_L2', image_id)):
            raise ValueError('unsupported collection ID')

        raw_image = ee.Image(image_id)

        # CGM - Testing out not setting any self. parameters and passing inputs
        #   to the super().__init__() call instead

        spacecraft_id = ee.String(raw_image.get('SPACECRAFT_ID'))

        # Select the ST/LST band separately from the SR bands
        # Not passing the QA band through since it is not used in the model
        input_bands = ee.Dictionary({
            'LANDSAT_4': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'LANDSAT_5': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'LANDSAT_7': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7'],
            'LANDSAT_8': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
            'LANDSAT_9': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7'],
        })
        output_bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']

        lst_band = ee.Dictionary({
            'LANDSAT_4': 'ST_B6', 'LANDSAT_5': 'ST_B6', 'LANDSAT_7': 'ST_B6',
            'LANDSAT_8': 'ST_B10', 'LANDSAT_9': 'ST_B10',
        })

        # # CGM - We are intentionally not masking for clouds in the LST
        # # Cloud mask function must be passed with raw/unnamed image
        # cloud_mask = openet.core.common.landsat_c2_sr_cloud_mask(
        #     raw_image, **cloudmask_args
        # )

        # Prep image to 0-1 surface reflectance values
        prep_img = (
            raw_image
            .select(input_bands.get(spacecraft_id), output_bands)
            .multiply([0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275])
            .add([-0.2, -0.2, -0.2, -0.2, -0.2, -0.2])
            .set({'system:time_start': raw_image.get('system:time_start'),
                  'system:index': raw_image.get('system:index'),
                  'SPACECRAFT_ID': spacecraft_id,
                  })
        )

        # Compute the emissivity corrected LST and add to the prepped image
        #   or add the raw LST to the prepped image
        if c2_lst_correct:
            lst_img = openet.core.common.landsat_c2_sr_lst_correct(
                raw_image, prep_img.normalizedDifference(['nir', 'red'])
            )
        else:
            lst_img = (
                raw_image.select([ee.String(lst_band.get(spacecraft_id))])
                .multiply(0.00341802).add(149.0)
            )

        init_img = prep_img.addBands(lst_img.rename(['lst']))

        # CGM - super could be called without the init if we set input_image and
        #   spacecraft_id as properties of self
        super().__init__(init_img)
        # super()
