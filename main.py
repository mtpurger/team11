"""Main Entrypoint for the Application"""

import logging
import json
import base64
import googlemaps

import urllib, urllib2

from flask import Flask, request
from flask import jsonify
from google.cloud import datastore
from google.cloud import pubsub

import capitalsdsutility
import Map
import utility
import notebook
import sys
from google.cloud import storage, exceptions
from google.cloud.storage import Blob

app = Flask(__name__)

def get_query_results(query, limitNum):
    results = list()
    for entity in list(query.fetch(limit=limitNum)):
        results.append(dict(entity))
    return results

def parse_capital(capital):
    """converts a greeting to an object"""
    return {
        'id': capital['id'],
        'country': capital['country'],
        'name': capital['name'],
        'location': {
            'latitude': capital['latitude'],
            'longitude': capital['longitude']
        },
        'countryCode': capital['countryCode'],
        'continent': capital['continent']
    }

@app.route('/api/status', methods=['GET'])
def status():
    response = json.dumps({'insert': True, 'fetch': True, 'delete': True, 'list': True, 'query': True, 'search': True, 'pubsub' : True, 'storage' : True}) 
    return response, 200

@app.route('/api/capitals/<id>', methods=['DELETE'])
def deletecapital(id):
    try:
        ds = datastore.Client(project='hackathon-team-011')
        key = ds.key('capitals', int(id))
        entity = ds.get(ds.key('capitals', int(id)))
        if entity is None:
            response = {'code': 404, 'message': 'Capital not found'}
            return jsonify(response), 404

        ds.delete(key)
        response = {'code': 200, 'message': 'Capital successfully deleted'}
        return jsonify(response), 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error'}
        return jsonify(response)

@app.route('/api/capitals/<id>', methods=['GET'])
def fetchcapital(id):
    try:
        ds = datastore.Client(project='hackathon-team-011')
        key = ds.key('capitals', int(id))
        entity = ds.get(ds.key('capitals', int(id)))
        if entity is None:
            response = {'code': 404, 'message': 'Capital record not found'}
            return jsonify(response), 404

        return jsonify(parse_capital(entity)), 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error'}
        return jsonify(response)

@app.route('/api/capitals/<int:id>', methods=['PUT'])
def insertcapital(id):
    try:
        """deletes, fetchs and inserts capitals from/to datastore"""
        capitalsds = capitalsdsutility.Capitals()

        inputobj = request.get_json()
            
        capitalid = id
        country = inputobj['country']
        name = inputobj['name']
        longitude = inputobj['location']['longitude']
        latitude = inputobj['location']['latitude']
        countrycode = inputobj['countryCode']
        continent = inputobj['continent']

        capitalsds.store_capital(
            id, capitalid,
            country,
            name,
            longitude,
            latitude,
            countrycode,
            continent)

        return 'Successfully stored the capital', 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error: ' + e.message}
        return jsonify(response)      

@app.route('/api/capitals', methods=['GET'])
def listcapitals():
    try:
        limit = 20
        ds = datastore.Client(project='hackathon-team-011')
        query = ds.query(kind="capitals")

        opt_param = request.args.get("query")
        if opt_param != None:
            queryParms = opt_param.split(":")
            query.add_filter(queryParms[0], '=', queryParms[1])
            limit = 99999999
        
        opt_param = request.args.get("search")

        if opt_param != None:
            limit = 99999999 

        results = get_query_results(query, limit)
        
        count = 0
        result = []
        for obj in results:
            if count < limit:
                count += 1
                res1 = parse_capital(obj)
                if opt_param is None:
                    result.append(res1)
                else:
                    if opt_param in str(res1):
                        result.append(res1)

        if len(result) == 0:
            response = {'code': 404, 'message': 'Search did not return anything' }
            return jsonify(response), 404

        return jsonify(result), 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error'}
        return jsonify(response)

@app.route('/api/capitals/<id>/publish', methods=['POST'])
def publishtotopic(id):
    try:
        # Fetch the capital
        ds = datastore.Client(project='hackathon-team-011')
        key = ds.key('capitals', int(id))
        entity = ds.get(key)

        if entity is None:
            response = {'code': 404, 'message': 'Capital record not found'}
            return jsonify(response), 404

        # Fetch the topic
        obj = request.get_json()
        bigtopicname = obj['topic']
        tokens = bigtopicname.split('/')
        projectname = tokens[1]
        topicname = tokens[3]
        pubsubclient = pubsub.Client(project=projectname)
        topic = pubsubclient.topic(topicname)

        if not topic.exists():
            response = {'code': 404, 'message': 'Topic record not found'}
            return jsonify(response), 404

        # Publish the capital to the topic
        message = json.dumps(parse_capital(entity))
        publishedid = topic.publish(message)
        
        response = {'messageId': int(publishedid)}
        return jsonify(response), 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error: ' + e.message}
        return jsonify(response)

@app.route('/api/capitals/<id>/store', methods=['POST'])
def sendBucket(id):
    try:
        ds = datastore.Client(project='hackathon-team-011')
        key = ds.key('capitals', int(id))
        entity = ds.get(ds.key('capitals', int(id)))
        if entity is None:
            response = {'code': 404, 'message': 'Capital not found'}
            return jsonify(response), 404

        gcs = storage.Client(project='hackathon-team-011')

        obj = request.get_json();
        bucketname = obj['bucket']

        bucket = gcs.get_bucket(bucketname)
        filename = str(id)
        
        blob = Blob(filename, bucket)
        
        #fs = open(filename, 'w')
        #fs.write(json.dumps(parse_capital(entity)))
        #fs.close()
        #fs = open(filename, 'r')
        blob.upload_from_string(json.dumps(parse_capital(entity)))
        #fs.close()

        response = {'code': 200, 'message': 'Capital successfully stored in GCS in file: ' + filename}
        return jsonify(response), 200

    except exceptions.NotFound:
        response = {'code': 404, 'message': 'Error: Bucket {} does not exists.'.format(bucketname)}
        return jsonify(response), 404
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error' + e.message}
        return jsonify(response)
  

@app.route('/pubsub/receive', methods=['POST'])
def pubsub_receive():
    """dumps a received pubsub message to the log"""

    data = {}
    try:
        obj = request.get_json()
        utility.log_info(json.dumps(obj))

        data = base64.b64decode(obj['message']['data'])
        utility.log_info(data)

    except Exception as e:
        # swallow up exceptions
        logging.exception('Oops!')

    return jsonify(data), 200


@app.route('/notes', methods=['POST', 'GET'])
def access_notes():
    """inserts and retrieves notes from datastore"""

    book = notebook.NoteBook()
    if request.method == 'GET':
        results = book.fetch_notes()
        result = [notebook.parse_note_time(obj) for obj in results]
        return jsonify(result)
    elif request.method == 'POST':
        print json.dumps(request.get_json())
        text = request.get_json()['text']
        book.store_note(text)
        return "done"

@app.route('/api/capitals/map', methods=['GET'])
def test_reverse_geocode():
    gmaps = googlemaps.Client(key='AIzaSyDmfwKdVtJgOvTByXYXKyKsTAKlGjHEYDE')    
	
    #responses.add(responses.GET, 'https://maps.googleapis.com/maps/api/geocode/json',  body='{"status":"OK","results":[]}',  status=200,  content_type='application/json')

	# Look up an address with reverse geocoding
    reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))

    print reverse_geocode_result
    
    mymap = Map()
    # Add Beijing, you'll want to use your geocoded points here:
    mymap.add_point((40.714224, -73.96145))
    #with open("output.html", "w") as out:
    #    print(map)

    #address = reverse_geocode_result[0][0]

    #url = 'http://maps.googleapis.com/maps/api/geocode/json?' + urllib.urlencode(params)
    #rawreply = urllib2.urlopen(url).read()                                          
    #reply = json.loads(rawreply)  

    #if reverse_geocode_result is None:
    #    return "none", 200

    #return address, 200

@app.errorhandler(500)
def server_error(err):
    """Error handler"""
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(err), 500


if __name__ == '__main__':
    # Used for running locally
    app.run(host='127.0.0.1', port=8080, debug=True)
