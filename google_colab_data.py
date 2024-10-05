#@title Imports (need to be run only once)

#sudo apt-get install swig
#pip install rasterio s2geometry pygeos geopandas tqdm

"""
This module imports various libraries and modules necessary for geospatial data processing and manipulation.
"""

# Standard library imports
import functools  # Higher-order functions and operations on callable objects
import glob  # Unix style pathname pattern expansion
import gzip  # Support for gzip files
import json  # JSON encoder and decoder
import multiprocessing  # Process-based parallelism
from multiprocessing.pool import ThreadPool  # Thread-based parallelism
import os  # Miscellaneous operating system interfaces
import shutil  # High-level file operations
import tempfile  # Generate temporary files and directories
from typing import Optional, Tuple, Iterable, Callable, Any  # Type hinting

# Third-party library imports
import geopandas as gpd  # Geospatial data in Python
from google.auth import credentials  # Google authentication
from google.cloud import storage  # Google Cloud Storage client library
from IPython import display  # IPython display utilities
import pandas as pd  # Data manipulation and analysis
import pyproj  # Python interface to PROJ (cartographic projections and transformations library)
import rasterio  # Access to geospatial raster data
from rasterio.transform import Affine  # Affine transformations
import s2geometry as s2  # S2 Geometry Library
import shapely  # Manipulation and analysis of geometric objects
from shapely.geometry import Polygon, box  # Geometric objects
import tqdm.notebook  # Progress bar for Jupyter notebooks
from shapely.ops import transform  # Geometric transformations
# @title Prepare urls of geotiffs of the given region

# @markdown First, select a region from either the [Natural Earth low res](https://www.naturalearthdata.com/downloads/110m-cultural-vectors/110m-admin-0-countries/) (fastest), [Natural Earth high res](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/) or [World Bank high res](https://datacatalog.worldbank.org/dataset/world-bank-official-boundaries) shapefiles:
region_border_source = "Natural Earth (High Res 10m)"  # @param ["Natural Earth (Low Res 110m)", "Natural Earth (High Res 10m)", "World Bank (High Res 10m)"]
region = "SGP (Singapore)"  # @param ["", "ABW (Aruba)", "AGO (Angola)", "AIA (Anguilla)", "ARG (Argentina)", "ATG (Antigua and Barbuda)", "BDI (Burundi)", "BEN (Benin)", "BFA (Burkina Faso)", "BGD (Bangladesh)", "BHS (The Bahamas)", "BLM (Saint Barthelemy)", "BLZ (Belize)", "BOL (Bolivia)", "BRA (Brazil)", "BRB (Barbados)", "BRN (Brunei)", "BTN (Bhutan)", "BWA (Botswana)", "CAF (Central African Republic)", "CHL (Chile)", "CIV (Ivory Coast)", "CMR (Cameroon)", "COD (Democratic Republic of the Congo)", "COG (Republic of Congo)", "COL (Colombia)", "COM (Comoros)", "CPV (Cape Verde)", "CRI (Costa Rica)", "CUB (Cuba)", "CUW (Cura\u00e7ao)", "CYM (Cayman Islands)", "DJI (Djibouti)", "DMA (Dominica)", "DOM (Dominican Republic)", "DZA (Algeria)", "ECU (Ecuador)", "EGY (Egypt)", "ERI (Eritrea)", "ETH (Ethiopia)", "FLK (Falkland Islands)", "GAB (Gabon)", "GHA (Ghana)", "GIN (Guinea)", "GMB (Gambia)", "GNB (Guinea Bissau)", "GNQ (Equatorial Guinea)", "GRD (Grenada)", "GTM (Guatemala)", "GUY (Guyana)", "HND (Honduras)", "HTI (Haiti)", "IDN (Indonesia)", "IND (India)", "IOT (British Indian Ocean Territory)", "JAM (Jamaica)", "KEN (Kenya)", "KHM (Cambodia)", "KNA (Saint Kitts and Nevis)", "LAO (Laos)", "LBR (Liberia)", "LCA (Saint Lucia)", "LKA (Sri Lanka)", "LSO (Lesotho)", "MAF (Saint Martin)", "MDG (Madagascar)", "MDV (Maldives)", "MEX (Mexico)", "MOZ (Mozambique)", "MRT (Mauritania)", "MSR (Montserrat)", "MUS (Mauritius)", "MWI (Malawi)", "MYS (Malaysia)", "MYT (Mayotte)", "NAM (Namibia)", "NER (Niger)", "NGA (Nigeria)", "NIC (Nicaragua)", "NPL (Nepal)", "PAN (Panama)", "PER (Peru)", "PHL (Philippines)", "PRI (Puerto Rico)", "PRY (Paraguay)", "RWA (Rwanda)", "SDN (Sudan)", "SEN (Senegal)", "SGP (Singapore)", "SHN (Saint Helena)", "SLE (Sierra Leone)", "SLV (El Salvador)", "SOM (Somalia)", "STP (Sao Tome and Principe)", "SUR (Suriname)", "SWZ (Eswatini)", "SXM (Sint Maarten)", "SYC (Seychelles)", "TCA (Turks and Caicos Islands)", "TGO (Togo)", "THA (Thailand)", "TLS (East Timor)", "TTO (Trinidad and Tobago)", "TUN (Tunisia)", "TZA (United Republic of Tanzania)", "UGA (Uganda)", "URY (Uruguay)", "VCT (Saint Vincent and the Grenadines)", "VEN (Venezuela)", "VGB (British Virgin Islands)", "VIR (United States Virgin Islands)", "VNM (Vietnam)", "ZAF (South Africa)", "ZMB (Zambia)", "ZWE (Zimbabwe)"]
# @markdown **or** specify an area of interest in [WKT format](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) (assumes crs='EPSG:4326'); this [tool](https://arthur-e.github.io/Wicket/sandbox-gmaps3.html) might be useful.
your_own_wkt_polygon = "crs='EPSG:4326'"  # @param {type:"string"}

# @markdown Second, specify which years to download
download_2016 = True  # @param { type: "boolean" }
download_2017 = True  # @param { type: "boolean" }
download_2018 = True  # @param { type: "boolean" }
download_2019 = True  # @param { type: "boolean" }
download_2020 = True  # @param { type: "boolean" }
download_2021 = True  # @param { type: "boolean" }
download_2022 = True  # @param { type: "boolean" }
download_2023 = True  # @param { type: "boolean" }

_GCS_BUCKET = "open-buildings-temporal-data"

_DATASET_VERSION = 'v1'

_GCS_MANIFESTS_FOLDER = "manifests_merged_s2_level_2_float"

_LOCAL_DOWNLOAD_URL_FILE_PATH = "/tmp/downloadable_urls.txt"

_MAX_NUM_THREADS = 8

_MANIFEST_S2_LEVEL = 2


def get_years_as_list() -> list[int]:
  years_to_download = []
  for year in range(2016, 2024):
    should_download = globals()[f"download_{year}"]
    if should_download:
      years_to_download.append(year)
  return years_to_download


def get_region_geometry(
    region_border_source: str, region: str, your_own_wkt_polygon: str
) -> shapely.geometry.base.BaseGeometry:
  """Returns the shapely geometry of the requested region."""

  if your_own_wkt_polygon:
    region_df = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries.from_wkt([your_own_wkt_polygon]), crs="EPSG:4326"
    )
    if not isinstance(
        region_df.iloc[0].geometry, shapely.geometry.polygon.Polygon
    ) and not isinstance(
        region_df.iloc[0].geometry, shapely.geometry.multipolygon.MultiPolygon
    ):
      raise ValueError(
          "`your_own_wkt_polygon` must be a POLYGON or MULTIPOLYGON."
      )
    print(f"Preparing your_own_wkt_polygon.")
    return region_df.iloc[0].geometry

  if not region:
    raise ValueError("Please select a region or set your_own_wkt_polygon.")

  if region_border_source == "Natural Earth (Low Res 110m)":
    url = (
        "https://naciscdn.org/naturalearth/"
        "110m/cultural/ne_110m_admin_0_countries.zip"
    )
    !wget -N {url}
    display.clear_output()
    region_shapefile_path = os.path.basename(url)
  elif region_border_source == "Natural Earth (High Res 10m)":
    url = (
        "https://naciscdn.org/naturalearth/"
        "10m/cultural/ne_10m_admin_0_countries.zip"
    )
    !wget -N {url}
    display.clear_output()
    region_shapefile_path = os.path.basename(url)
  elif region_border_source == "World Bank (High Res 10m)":
    url = (
        "https://datacatalogfiles.worldbank.org/ddh-published/"
        "0038272/DR0046659/wb_countries_admin0_10m.zip"
    )
    !wget -N {url}
    !unzip -o {os.path.basename(url)}
    display.clear_output()
    region_shapefile_path = "WB_countries_Admin0_10m"

  region_iso_a3 = region.split(" ")[0]
  region_df = (
      gpd.read_file(region_shapefile_path)
      .query(f'ISO_A3 == "{region_iso_a3}"')
      .dissolve(by="ISO_A3")[["geometry"]]
  )
  print(f"Preparing {region} from {region_border_source}.")
  return region_df.iloc[0].geometry


def get_bounding_box_s2_covering_tokens(
    region_geometry: shapely.geometry.base.BaseGeometry,
) -> list[str]:
  """Returns the s2_tokens of the bounding box of the provided geometry."""
  region_bounds = region_geometry.bounds
  s2_lat_lng_rect = s2.S2LatLngRect_FromPointPair(
      s2.S2LatLng_FromDegrees(region_bounds[1], region_bounds[0]),
      s2.S2LatLng_FromDegrees(region_bounds[3], region_bounds[2]),
  )
  coverer = s2.S2RegionCoverer()
  # NOTE: Should be kept in-sync with manifest s2 cell level.
  coverer.set_fixed_level(_MANIFEST_S2_LEVEL)
  coverer.set_max_cells(1000000)
  return [cell.ToToken() for cell in coverer.GetCovering(s2_lat_lng_rect)]


def get_matching_manifest_blobs(s2_token: str) -> list[storage.Blob]:
  """Returns a list of manifest blobs for the given s2_token."""
  matching_manifest_blobs = []
  token_manifest_blobs = list(
      storage_client.list_blobs(
          _GCS_BUCKET,
          prefix=os.path.join(_DATASET_VERSION, _GCS_MANIFESTS_FOLDER, f'{s2_token}_'),
      )
  )
  for year in get_years_as_list():
    filtered_token_manifests = [
        blob for blob in token_manifest_blobs if f'_{str(year)}_' in blob.name
    ]
    matching_manifest_blobs.extend(filtered_token_manifests)
  return matching_manifest_blobs


def multithreaded_fn(progress_bar_desc: str, fn: Callable, items: Iterable[Any]):
  """Run `fn` on `items` using multithreading and display a progress bar."""
  total_num_items = len(items)
  fn_results = []
  with tqdm.notebook.tqdm(
      total=len(items), desc=progress_bar_desc
  ) as pbar:
    with ThreadPool(processes=_MAX_NUM_THREADS) as pool:
      for result in pool.map(fn, items):
        fn_results.extend(result)
        pbar.update(1)
  return fn_results



def multithreaded_get_matching_manifest_blobs(
    s2_tokens: list[str],
) -> list[storage.Blob]:
  """Returns a list of manifest blobs for the given s2_tokens."""
  return multithreaded_fn("Fetching matching manifests",
                          get_matching_manifest_blobs, s2_tokens)


def extract_tile_polygons(
    manifest_bytes: bytes,
) -> list[str]:
  """Extracts geotiff urls from a manifest."""
  tile_polys = []
  manifest = json.loads(manifest_bytes)
  crs = None
  for tileset in manifest["tilesets"]:
    for source in tileset["sources"]:
      # All tiles in a manifest should have the same projection
      if crs is None:
        crs = tileset["crs"]
      affine_transform = source["affineTransform"]
      transform = Affine.translation(
          affine_transform["translateX"], affine_transform["translateY"]
      ) * Affine.scale(affine_transform["scaleX"], affine_transform["scaleY"])
      dimensions = source["dimensions"]
      width = dimensions["width"]
      height = dimensions["height"]

      corners = [(0, 0), (width, 0), (width, height), (0, height)]
      corners = [transform * corner for corner in corners]

      uri = source["uris"][0]
      object_path = manifest["uriPrefix"] + uri
      tile_polys.append((object_path, Polygon(corners)))

  return tile_polys, crs


def extract_geotiff_urls(
    manifest_blob: storage.Blob,
    region_geometry: shapely.geometry.base.BaseGeometry,
) -> list[str]:
  """Extracts geotiff urls from a manifest intersecting `region_geometry`."""
  manifest_bytes = manifest_blob.download_as_bytes()
  tile_polys, crs = extract_tile_polygons(manifest_bytes)
  # EPSG:4326 is the standard WGS84 lat/lon coordinate system. We transform
  # region_geometry from EPSG:4326 to manifest's projection before doing
  # intersection check.
  transformer = pyproj.Transformer.from_crs("epsg:4326", crs, always_xy=True)
  region_geometry = transform(transformer.transform, region_geometry)
  geotiff_urls = []
  for (url, poly) in tile_polys:
    if poly.intersects(region_geometry):
      geotiff_urls.append(url)

  return geotiff_urls


def multithreaded_extract_geotiff_urls(
    manifest_blobs: list[storage.Blob],
    region_geometry: shapely.geometry.base.BaseGeometry,
) -> list[str]:
  """Extracts geotiff urls from manifests."""
  return multithreaded_fn(
      "Extracting urls",
      lambda manifest_blob: extract_geotiff_urls(
          manifest_blob, region_geometry
      ),
      manifest_blobs,
  )


def write_to_file(filename: str, urls: list[str]) -> None:
  """Writes urls to file."""
  with open(filename, "w") as f:
    for url in urls:
      f.write(f"{url}\n")


# Clear output after pip install.
display.clear_output()
storage_client = storage.Client(credentials=credentials.AnonymousCredentials())
geometry = get_region_geometry(
    region_border_source, region, your_own_wkt_polygon
)
s2_tokens = get_bounding_box_s2_covering_tokens(geometry)

region_manifest_blobs = multithreaded_get_matching_manifest_blobs(s2_tokens)

geotiff_urls = multithreaded_extract_geotiff_urls(
    region_manifest_blobs, geometry
)

write_to_file(_LOCAL_DOWNLOAD_URL_FILE_PATH, geotiff_urls)

print(f"Finished writing urls to file. File contains {len(geotiff_urls)} urls")