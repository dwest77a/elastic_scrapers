# Elastic Search Python Scraper Project

Part of the Flight Finder migration to flights
 - Scans elastic search records of arsf, faam and eufar files
 - Combines metadata and spatial coordinates into new json files
 - Json files can then be added to elastic search index using pystac_conversion bulk mode

Additional steps:
 - Include additional indexes (bas-masin, new arsf)
 - Fix existing projects (twinotter)
