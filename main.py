"""Main Entrypoint for the Application"""

import logging
import json
import base64

from flask import Flask, request
from flask import jsonify
from google.cloud import datastore

import capitalsdsutility
import utility
import notebook
import sys

app = Flask(__name__)

def get_query_results(query):
    results = list()
    for entity in list(query.fetch()):
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
    response = json.dumps({'insert': True, 'fetch': True, 'delete': True, 'list': True, 'query': True, 'search': False, 'pudsub' : False, 'storage' : False}) 
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

@app.route('/api/capitals/<id>', methods=['PUT'])
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
            idnum, capitalid,
            country,
            name,
            longitude,
            latitude,
            countrycode,
            continent)

        return 'Successfully stored the capital', 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error'}
        return jsonify(response)      

@app.route('/api/capitals', methods=['GET'])
def listcapitals():
    try:
        ds = datastore.Client(project='hackathon-team-011')
        query = ds.query(kind="capitals")

        opt_param = request.args.get("query")
        if opt_param != None:
            queryParms = opt_param.split(":")
            query.add_filter(queryParms[0], '=', queryParms[1])

        opt_param = request.args.get("search")
        if opt_param != None:
            query.add_filter('*', '=', opt_param)

        results = get_query_results(query)
        result = [parse_capital(obj) for obj in results]
        return jsonify(result), 200
    except Exception as e:
        response = {'code': 0, 'message': 'Unexpected error'}
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

        #file_put_contents("gs://hackathon-team-011.appspot.com/" + id + ".txt", base64.b64decode(jsonify(response)));
        bPath = "gs://hackathon-team-011.appspot.com/" + id + ".txt"        
        fp = open(bPath, 'w')
        return 'step 1', 200
        
        #fwrite(fp, jsonify(response));
        #fclose(fp);
        #response = {'code': 200, 'message': 'Capital sent to the Bucket successfully'}
        #return "gs://hackathon-team-011.appspot.com/" + id + ".txt", 200
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
