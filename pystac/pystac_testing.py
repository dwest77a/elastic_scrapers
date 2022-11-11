import json
import shutil
import tempfile
from datetime import date
from pathlib import Path

from pystac import Catalog, Collection, Item, get_stac_version
from pystac.extensions.eo import EOExtension
from pystac.extensions.label import LabelExtension

root_catalog = Catalog.from_file('catalog_test.json')
print(f"ID: {root_catalog.id}")
print(f"Title: {root_catalog.title or 'N/A'}")
print(f"Description: {root_catalog.description or 'N/A'}")

stac_vsn = get_stac_version()
print('STAC Version:',stac_vsn)

# Retrieve All Collection Objects
collections = list(root_catalog.get_collections())

print(f"Number of collections: {len(collections)}")
print("Collections IDs:")
for collection in collections:
    print(f"- {collection.id}")
    
    items = list(collection.get_items())
    for item in items:
        print(f" - {item.id}")
