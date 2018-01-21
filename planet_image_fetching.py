import os
import requests
from requests.auth import HTTPBasicAuth
from geopy import Point
from geopy.distance import VincentyDistance, vincenty
from ImageClip import ImageClip, image_clip_df_decoder
from multiprocessing.dummy import Pool as ThreadPool
from retrying import retry
from time import gmtime, strftime, sleep

def get_clipped_image_boundaries(overall_bounding_box, width, height):
  """
  Given an overall bounding box and desired width and height, create smaller bounding
  boxes of width and height such that all of the overall bounding box is covered.

  only_in_boundaries: If true, don't produce smaller bounding boxes that partially spill out of
                      overall bounding box
  """
  def get_east_boundary_boxes(origin_bounding_box):
    # get boundary box to the east by making new boundary box current's ne corner as origin
    destination_bounding_box = ImageClip(
      origin_bounding_box.nw_lat(), 
      origin_bounding_box.se_lng(), 
      width=width, height=height
    )

    # if boundary is outside overall boundary box, return. 
    # If still inside overall boundary, search again further east
    if destination_bounding_box.se_lng() > overall_bounding_box.se_lng():
      out_boundaries = list()
      #pdb.set_trace()
      return(out_boundaries)
    else:
      out_boundaries = get_east_boundary_boxes(destination_bounding_box)
      out_boundaries.append(destination_bounding_box)
      #pdb.set_trace()
      return out_boundaries

  def get_south_and_east_boundary_boxes(origin_bounding_box):
    """
    From the overall bounding box, get a set of smaller rectangular bounding boxes that cover
    the overall space. These smaller rectangular bounding boxes will be sent to the search API
    to get image IDs for image fetching

    This makes use of the `get_east_boundary_boxes()` function to fill out boxes to the east
    """

    # Get all boxes to the east before going one row down
    boxes_east = [origin_bounding_box] + get_east_boundary_boxes(origin_bounding_box)

    box_south = ImageClip(
      origin_bounding_box.se_lat(),
      origin_bounding_box.nw_lng(),
      width=width, height=height
    )

    # If southern boundary box is outside overall boundary box, return
    if box_south.se_lat() < overall_bounding_box.se_lat():
      out_boundaries = list()
      return(boxes_east)
    else:
      out_boundaries = get_south_and_east_boundary_boxes(box_south)
      out_boundaries += boxes_east
      return out_boundaries

  print("Getting smaller boundary boxes from {}".format(str(overall_bounding_box)))

  # Seed the recursion with the smaller bounding box in NW corner
  origin_bounding_box = ImageClip(
    overall_bounding_box.nw_lat(), 
    overall_bounding_box.nw_lng(), 
    width=width, 
    height=height
  )
  return get_south_and_east_boundary_boxes(origin_bounding_box)


def get_clipped_image_info(overall_bounding_box, width, height, item_type, asset_type):
  """
  From an overall bounding box, return an array of smaller cropped bounding boxes,
  where each smaller box has designated width and height in meters. Array entries 
  include nw and se latlngs and image IDs of corresponding image from planet API
  """
  clipped_image_boundaries = get_clipped_image_boundaries(overall_bounding_box, width, height)

  print("Split overall boundary into {} smaller boundaries with width {}m and height {}m".format(
    str(len(clipped_image_boundaries)), width, height))
 
  with requests.Session() as s:
    s.auth = (os.environ['PL_API_KEY'], '') 

    @retry(
      wait_exponential_multiplier=1000,
      wait_exponential_max=10000)
    def fetch_image_id(bounding_box):
      """
      From the bounding box coordinates, use the planet search API to find an image_ID that covers the box
      Filters for the search are specified in planet_api_params.py
      """
      from PlanetAPIParams import date_range_filter, cloud_cover_filter, full_image_filter
      geo_json_geometry = bounding_box.prepare_geojson()

      geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": geo_json_geometry
      }

      permission_filter = {
        "type": "PermissionFilter",
        "config": ["assets." + asset_type + ":download"]
      }

      satellite_image_search_params = {
        "type": "AndFilter",
        "config": [geometry_filter,
          date_range_filter,
          cloud_cover_filter,
          permission_filter
        ]
      }

      search_endpoint_request = {
        "item_types": [item_type],
        "filter": satellite_image_search_params
      }

      print("sending search request {}".format(str(search_endpoint_request)))
      result = s.post(
        'https://api.planet.com/data/v1/quick-search',
        auth=HTTPBasicAuth(os.environ['PL_API_KEY'], ''),
        json=search_endpoint_request)
      
      if result.status_code == 429:
        raise Exception("rate limit error")
      elif result.status_code != 200:
        print("ERROR: unsuccessful request, Got response code {}, reason is {}".format(str(result.status_code), result.reason))

      response = result.json()
      if(len(response['features'])):
        image_id = response['features'][0]['id']
        print("Got image ID: {}".format(image_id))
        bounding_box.set_image_info(image_id, item_type, asset_type)
      elif(result.status_code == 200):
        print("ERROR: successful request, but no image IDs returned. result is {}".format(str(response)))

      return

    thread_pool = ThreadPool(5)
    thread_pool.map(fetch_image_id, clipped_image_boundaries)
  
  return(clipped_image_boundaries)

def write_clip_summary_file(filename, clipped_images):
  """
  Write a summary csv file of clips, including their coordinates
  and planet image info
  """
  import csv
  print("Writing clip summary file {}".format(filename))
  with open(filename, 'w') as csvfile:
    # get headers from first clip
    summarywriter = csv.DictWriter(csvfile,clipped_images[0].to_dict().keys())
    summarywriter.writeheader()
    for clip in clipped_images:
      summarywriter.writerow(clip.to_dict())
  print("Summary file {} written!".format(filename))
  return

def load_clipped_image_info(filename):
  """
  Load a previously written summary csv file of clips.
  It uses pandas to read the csv to understand
  field types, eg. floats
  """
  import pandas as pd
  print("Loading clip summary file {}".format(filename))
  df = pd.read_csv(filename)
  clips = [image_clip_df_decoder(c) for index, c in df.iterrows()]
  return clips

def write_boundary_hits_file(download_status_codes):
  """
  Having an issue downloading clips that are on the boundary
  of satellite images. The search API is returning image IDs
  for images that don't completely overlap the clip, which then
  errors out when downloading.

  For now, going to write each clip to which this happens to a
  file for later downloading
  """
  import csv

  boundary_hits_filename = os.path.join(os.environ['PL_IMAGE_DIR'], "boundary_hits.csv")
  with open(boundary_hits_filename, 'w') as csvfile:
    # get headers from first clip
    headers = list(download_status_codes[0][0].to_dict().keys())
    headers.insert(0, 'status_code')
    boundary_hit_writer = csv.DictWriter(csvfile, headers)
    boundary_hit_writer.writeheader()
    for d in download_status_codes:
      outdict = d[0].to_dict()
      outdict['status_code'] = d[1]
      boundary_hit_writer.writerow(outdict)
  return

def download_clipped_images(clipped_images):
  """
  Given a complete clip image info object, download the clip using the fetched image ID.

  I originally had the fetch request as one of the ImageClip.py's functions, but it was
  easier to parallelize the requests if they didn't exist as a class function
  """
  with requests.Session() as s:
    s.auth = (os.environ['PL_API_KEY'], '') 

    @retry(
      wait_exponential_multiplier=1000,
      wait_exponential_max=10000)
    def download_clip(clip):
      """
      Given an image clip bounding box with a filled in image_id, ping planet API
      to get the clip download ready, and then download it
      """
      if not clip.image_id:
        raise ValueError("Can only download clip with valid image ID")

      download_filename = os.environ['PL_IMAGE_DIR'] + "Clip " + clip.item_type + \
            "-" + clip.asset_type + " " + clip.box_id + '.zip'

      # If clip already downloaded, skip and return custom code 888
      if os.path.isfile(download_filename):
        print("clip {} already downloaded! Skipping...".format(download_filename))
        return (clip, 888)

      # Prepare request to get clip download ready
      aoi = clip.prepare_geojson()
      targets = [
        {
          "item_id": clip.image_id, 
          "item_type": clip.item_type,
          "asset_type": clip.asset_type
        }
      ]
      clip_query_json = {"aoi": aoi, "targets": targets}

      print("Pinging Planet API for clip {} with item_id: {}".format(clip.box_id, clip.image_id))

      result = s.post(
        'https://api.planet.com/compute/ops/clips/v1',
        auth=HTTPBasicAuth(os.environ['PL_API_KEY'], ''),
        json=clip_query_json)

      if result.status_code == 429:
        raise Exception("rate limit error")
      elif result.status_code == 400:
        # Boundary of satellite image, skip and save for later
        print("AOI out of bounds for clip {}".format(str(clip.box_id)))
        return (clip, result.status_code)

      # Check status of clip image download
      download_id = result.json()['id']
      print("Readying download... id = {}".format(download_id))

      timer_count = 0
      download_done = False
      while timer_count < 60 and not download_done:
        sleep(1)
        timer_count += 1
        status_url = 'https://api.planet.com/compute/ops/clips/v1/' + download_id

        download_result = s.get(
          url=status_url,
          auth=HTTPBasicAuth(os.environ['PL_API_KEY'], '')
          )

        download_result_json = download_result.json()

        if(download_result.json()['state'] == 'succeeded'):
          download_done = True
          download_url = download_result.json()['_links']['results'][0]

          print("Downloading file to {}".format(download_filename))

          r = s.get(download_url, allow_redirects=True)
          open(download_filename, 'wb').write(r.content)

      return (clip, result.status_code)
        
    thread_pool = ThreadPool(5)
    download_status_codes = thread_pool.map(download_clip, clipped_images)

    # Write box_ids for images where planet API returned bad image IDs
    write_boundary_hits_file(download_status_codes)

  return

# Test run with coordinates in Marin. NOTE: Planet API only allows free fetching
# for images in California

nw_lng = -122.509337
nw_lat = 37.807952
se_lng = -122.378702
se_lat = 37.713905

#nw_lng = -122.46394153223865
#nw_lat = 37.76290386679462

item_type = 'PSScene3Band'
asset_type = 'visual'

# Determine bounding boxes and search for image IDs
# test_bounding_box = ImageClip(nw_lat, nw_lng, width=300, height=300)
#test_bounding_box = ImageClip(nw_lat, nw_lng, se_lat=se_lat, se_lng=se_lng)
#image_clips = get_clipped_image_info(test_bounding_box, 200, 200, item_type, asset_type)

# summary_filename = os.environ['PL_IMAGE_DIR'] + "Clips_Summary_" + \
#   strftime("%Y-%m-%d", gmtime()) + ".csv"
# write_clip_summary_file(summary_filename, image_clips)

# Load previously searched bounding boxes.
summary_filename = os.path.join(os.environ['PL_IMAGE_DIR'], "Clips_Summary_2018-01-09.csv")
image_clips = load_clipped_image_info(summary_filename)
download_clipped_images(image_clips)

