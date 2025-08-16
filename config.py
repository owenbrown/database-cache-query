"""Configuration settings for the database cache query system."""

import pathlib

# Cache directory configuration as specified in requirement 5.7
CACHED_DATA_FOLDER = pathlib.Path.home() / "data" / "cached_data"
