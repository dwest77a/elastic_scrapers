# Elastic Search Python Scraper Project

Part of the Flight Finder migration to flights
 - Scans elastic search records of arsf files
 - Combines metadata and spatial coordinates into new json files
 - Json files can then be added to elastic search index in future

Additions to json content:
 - Flight name: (from archive)
 - Project: (pcode - check multiples)
 - Path: (done)
 - Variables: (from archive)
 - geoinfo: (from archive - envelope)
 - timeinfo: (done)
 - platform: (from archive)
 - facilities: (from archive)
 - instruments: (from archive)

Updated: 19/10/2022
