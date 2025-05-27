import numpy as np
from pyproj import CRS


GOES_CORNERS = np.array(((-116, 30), (-102, 38)))
WEB_MERCATOR = CRS.from_epsg("3857")
GEODETIC = CRS.from_epsg("4326")
DX = 2000
DY = DX
