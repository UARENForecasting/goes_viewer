import numpy as np
from pyproj import CRS


G17_CORNERS = np.array(((-116, 38), (-102, 30)))
G16_CORNERS = np.array(((-116, 30), (-102, 38)))
WEB_MERCATOR = CRS.from_epsg("3857")
GEODETIC = CRS.from_epsg("4326")
DX = 2500
DY = DX
