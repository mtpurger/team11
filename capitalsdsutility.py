from datetime import datetime
from google.cloud import datastore
import utility


class Capitals:

    def __init__(self):
        self.ds = datastore.Client(project=utility.project_id())
        self.kind = "capitals"

    def store_capital(self, capitalId, country, name, longitude, latitude, countryCode, continent):
        key = self.ds.key(self.kind)
        entity = datastore.Entity(key)

        entity['id'] = capitalId
        entity['country'] = country
        entity['name'] = name
        entity['longitude'] = longitude
        entity['latitude'] = latitude
        entity['countryCode'] = countryCode
        entity['continent'] = continent

        return self.ds.put(entity)

def parse_note_time(note):
    """converts a greeting to an object"""
    return {
        'text': note['text'],
        'timestamp': note['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    }
