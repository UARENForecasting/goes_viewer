import json
import os
from pathlib import Path

RED = "#AB0520"
BLUE = "#0C234B"
S3_PREFIX = os.getenv("GV_S3_PREFIX", "ABI-L2-MCMIPF")
CONTRAST = int(os.getenv("GV_CONTRAST", 105))
TILE_SOURCE = os.getenv("GV_TILE_SOURCE",
                        "https://stamen-tiles.a.ssl.fastly.net/toner-lite")
LON_LIMITS = [
    float(s) for s in os.getenv("GV_LON_LIMITS", "-115,-103").split(",")
]
LAT_LIMITS = [float(s) for s in os.getenv("GV_LAT_LIMITS", "31,37").split(",")]
FILENAME = os.getenv("GV_FILE_NAME", "index.html")
API_URL = os.getenv("GV_API_URL")
API_USER = os.getenv("GV_API_USER")
API_PASS = os.getenv("GV_API_PASSWORD")
FILTERS = json.loads(os.getenv("GV_FILTERS", '{"Type": "ghi"}'))
BASE_DIR = Path(os.getenv("GV_BASE_DIR", "."))
FIG_DIR = os.getenv('GV_FIG_DIR', 'figs/')
PLAY_SPEED = os.getenv("GV_PLAY_SPEED", 500)
MAX_IMAGES = os.getenv("GV_MAX_IMAGES", 24)
DATA_PARAMS = json.loads(os.getenv("GV_DATA_PARAMS", '{}'))
SERVICE_AREA = os.getenv('GV_SERVICE_AREA', None)
