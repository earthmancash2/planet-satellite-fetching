"""
Set of filters for searches and download requests to planet API. This includes:
 - Time window for images
 - Tolerable level of cloud cover
 - Only full images or cropped images as well
"""

# filter images acquired in a certain date range
date_range_filter = {
  "type": "DateRangeFilter",
  "field_name": "acquired",
  "config": {
    "gte": "2016-07-01T00:00:00.000Z",
    "lte": "2018-08-01T00:00:00.000Z"
  }
}

# filter any images which are more than 50% clouds
cloud_cover_filter = {
  "type": "RangeFilter",
  "field_name": "cloud_cover",
  "config": {
    "lte": 0.05
  }
}

full_image_filter = {
  "type": "NumberInFilter",
  "field_name": "usable_data",
  "config": [1]
}
