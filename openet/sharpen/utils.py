import ee


def getAffineTransform(image):
    projection = image.projection()
    json = ee.Dictionary(ee.Algorithms.Describe(projection))
    return ee.List(json.get('transform'))


def point_image_value(image, xy, scale=1):
    """Extract the output value from a calculation at a point"""
    return ee.Image(image)\
        .reduceRegion(
            reducer=ee.Reducer.first(), geometry=ee.Geometry.Point(xy),
            scale=scale)\
        .getInfo()
