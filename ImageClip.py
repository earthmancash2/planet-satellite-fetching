from geopy import Point
from geopy.distance import VincentyDistance, vincenty
import uuid

class ImageClip:
  """
  Object representing a boundary box. This can be either the overall bounding box to be carved
  into smaller images, or the smaller image bounding boxes.
  Bounding boxes have to have a northwest coordinate, but the southwest coordinate can be specified
  with either lat lngs or via width and height.
  """
  def __init__(self, nw_lat, nw_lng, se_lat=None, se_lng=None, width=None, height=None, box_id=None):
    self.nw_coordinates = Point(nw_lat, nw_lng)
    self.image_id = None
    self.item_type = None
    self.asset_type = None
    self.box_id = box_id or uuid.uuid4().hex[:8]
    if(se_lat and se_lng):
      self.se_coordinates = Point(se_lat, se_lng)
      self.width = vincenty(self.nw_coordinates, Point(nw_lat, se_lng)).kilometers*1000
      self.height = vincenty(self.nw_coordinates, Point(se_lat, nw_lng)).kilometers*1000
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

  def set_image_info(self, image_id, item_type, asset_type):
    self.image_id = image_id
    self.item_type = item_type
    self.asset_type = asset_type

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

  def to_dict(self):
    clip_dict = {
      "box_id": self.box_id,
      "nw_lat": self.nw_lat(),
      "nw_lng": self.nw_lng(),
      "se_lat": self.se_lat(),
      "se_lng": self.se_lng(),
      "width": self.width,
      "height": self.height,
      "image_id": self.image_id,
      "item_type": self.item_type,
      "asset_type": self.asset_type
    }
    return clip_dict

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

def image_clip_df_decoder(df):
  """
  If loading a clip from a pandas dataframe, decode to an ImageClip object
  """
  clip = ImageClip(box_id=df['box_id'],
    nw_lat=df['nw_lat'],
    nw_lng=df['nw_lng'],
    se_lat=df['se_lat'],
    se_lng=df['se_lng'])
  clip.set_image_info(
    image_id=df['image_id'],
    item_type=df['item_type'],
    asset_type=df['asset_type'])
  return clip
