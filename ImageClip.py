from geopy import Point
from geopy.distance import VincentyDistance, vincenty

class ImageClip:
  """
  Object representing a boundary box. This can be either the overall bounding box to be carved
  into smaller images, or the smaller image bounding boxes.
  Bounding boxes have to have a northwest coordinate, but the southwest coordinate can be specified
  with either lat lngs or via width and height.
  """
  def __init__(self, nw_lat, nw_lng, se_lat=None, se_lng=None, width=None, height=None):
    self.nw_coordinates = Point(nw_lat, nw_lng)
    self.image_id = None
    if(se_lat and se_lng):
      self.se_coordinates = Point(se_lat, se_lng)
      self.width = vincenty(self.nw_coordinates, Point(nw_lat, se_lng)).kilometers
      self.height = vincenty(self.nw_coordinates, Point(se_lat, nw_lng)).kilometers
    elif(width and height):
      self.width = width
      self.height = height
      coordinate_east = VincentyDistance(kilometers=width/1000).destination(self.nw_coordinates, 90) 
      coordinate_south = VincentyDistance(kilometers=height/1000).destination(self.nw_coordinates, 180) 
      self.se_coordinates = Point(coordinate_south.latitude, coordinate_east.longitude)
    else:
      raise ValueError("Provide SE latlngs *or* width and height")

  def nw_coordinates():
    return self.nw_coordinates

  def nw_lat(self):
    return self.nw_coordinates.latitude

  def nw_lng(self):
    return self.nw_coordinates.longitude

  def se_coordinates(self):
    return self.se_coordinates

  def se_lat(self):
    return self.se_coordinates.latitude

  def se_lng(self):
    return self.se_coordinates.longitude

  def width(self):
    return self.width

  def height(self):
    return self.height

  def image_id(self):
    return self.image_id

  def set_image_id(self, image_id):
    self.image_id = image_id

  def prepare_geojson(self):
    geo_json_geometry = {
        "type": "Polygon",
        "coordinates": [
          [
            [self.nw_lng(), self.se_lat()],
            [self.nw_lng(), self.nw_lat()],
            [self.se_lng(), self.nw_lat()],
            [self.se_lng(), self.se_lat()],
            [self.nw_lng(), self.se_lat()]
          ]
        ]
      }
    return geo_json_geometry

  def __str__(self):
    return "Coordinates: NW ({}, {}), SE ({}, {}). Width: {}. Height: {}. Image_ID: {}".format(
        self.nw_lat(),
        self.nw_lng(),
        self.se_lat(),
        self.se_lng(),
        self.width,
        self.height,
        self.image_id
      )
