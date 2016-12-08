from datetime import datetime
from google.cloud import datastore
import utility


class Capitals:

    def __init__(self):
        self.ds = datastore.Client(project='hackathon-team-011')
        self.kind = "capitals"

    def store_capital(self, idnum, capitalId, country, name, longitude, latitude, countryCode, continent):
        key = self.ds.key(self.kind, idnum)
        entity = datastore.Entity(key)

        entity['id'] = capitalId
        entity['country'] = country
        entity['name'] = name
        entity['longitude'] = longitude
        entity['latitude'] = latitude
        entity['countryCode'] = countryCode
        entity['continent'] = continent

        return self.ds.put(entity)

    def fetch_capital(self, idnum, capitalId, country, name, longitude, latitude, countryCode, continent):
        key = self.ds.key(self.kind)
        entity = datastore.Entity(idnum)

        results = list()
        query = self.ds.query(kind=self.kind)
        for entity in list(query):
            if (entity['id']==idnum):
                country = entity['country']
                country = entity['country']
                name = entity['name']
                longitude = entity['longitude']
                latitude = entity['latitude']
                countryCode = entity['countryCode']
                continent = entity['continent']

        return

def parse_note_time(note):
    """converts a greeting to an object"""
    return {
        'text': note['text'],
        'timestamp': note['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
    }
