# Elastic Search Python Scraper Project

Part of the Flight Finder migration to flights
 - Scans elastic search records of arsf, faam and eufar files
 - Combines metadata and spatial coordinates into new json files
 - Json files can then be added to elastic search index in future

Metadata to combine (misc):
 - flight num
 - pcode (data)
 - aircraft
 - variables
 - location
 - platform
 - instruments
 - pi/crew

Initial: 19/10/2022
 - adding FAAM support 07/11/2022
