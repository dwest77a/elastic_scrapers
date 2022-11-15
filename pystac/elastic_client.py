import json
import os, sys
import numpy as np

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

body = {
        "_source": {
            "include": [
                "es_id",
                "description_path",
                "collection",
                "geometry.display",
                "properties"
            ]
        },
        "query": {
            "bool": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "exists": {
                                    "field": "geometry.display.type"
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "term": {
                                    "geometry.display.type": "point"
                                }
                            }
                        ],
                        "should": []
                    }
                }
            }
        },
        "aggs": {
            "variables": {
                "nested": {
                    "path": "parameters"
                },
                "aggs": {
                    "std_name": {
                        "filter": {
                            "term": {
                                "parameters.name": "standard_name"
                            }
                        },
                        "aggs": {
                            "values": {
                                "terms": {
                                    "field": "parameters.value.raw"
                                }
                            }
                        }
                    }
                }
            }
        },
        "size": 400
    }

class ElasticClient:
    """
    Connects to elasticsearch and submits a request
    """

    connection_kwargs = {
        "hosts": ["https://es9.ceda.ac.uk:9200"],
        "headers": {
            "x-api-key":  "b0cc021feec53216cb470b36bec8786b10da4aa02d60edb91ade5aae43c07ee6",
        },
#        "use_ssl": True,
        "verify_certs": False,
        "ssl_show_warn": False,
    }
    def __init__(self):
        self.index = "stac-flightfinder-items"
        self.es = Elasticsearch(**self.connection_kwargs)
    
    def request(self):
        result = self.es.search(index=self.index, body=body)
        print(len(result["hits"]["hits"]))

ElasticClient().request()
    
    