from datetime import datetime
from pathlib import Path
import psycopg2.extensions


# The root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# xml file url
URL = "https://devpub.moderntv.eu/epg.xml"

# ===============================
# Database configuration
# ===============================
DATABASES = {
    'OPTIONS': {
        'isolation_level': psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE,
    },
    'default': {
        'DATABASE': '',
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    },
}

# Media files config

BASE_PICTURES_PATH = BASE_DIR / 'media/md1/epgpictures/'

Path(BASE_PICTURES_PATH).mkdir(parents=True, exist_ok=True)

print(
    datetime.now(),
    '\nRoot: {}'.format(BASE_DIR),
    '\nPictures Directory: {}'.format(BASE_PICTURES_PATH)
)

PICTURES_DIR = 'md1/epgpictures/'

XMLFILE_DIR = BASE_DIR
XMLFILE = BASE_DIR / "epg.xml"

# Deletion date from the xml and the database
DELETION_DATE = 120 # days
