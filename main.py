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

app = Flask(__name__)

def get_query_results(query):
    results = list()
    for entity in list(query.fetch()):
        results.append(dict(entity))
    return results

def parse_capital(capital):
    """converts a greeting to an object"""
    return {
        'countryCode': capital['countryCode'],
        'country': capital['country'],
        'name': capital['name'],
        'latitude': capital['latitude'],
        'longitude': capital['longitude']
    }

@app.route('/api/capitals/<id>', methods=['DELETE'])
def deletecapital(id):
    ds = datastore.Client(project='hackathon-team-011')
    key = ds.key('capitals', int(id))
    ds.delete(key)
    return "Deleted", 200

@app.route('/api/status', methods=['GET'])
def status():
    """misc api/status"""
    response = json.dumps({'insert': True, 'fetch': True, 'delete': True, 'list': True})

    return response, 200

@app.route('/api/capitals/<id>', methods=['GET'])
def fetchcapital(id):
        ds = datastore.Client(project='hackathon-team-011')
        query = ds.query(kind="capitals")
        query.add_filter('id', '=', int(id))
        results = get_query_results(query)
        result = [parse_capital(obj) for obj in results]
        return jsonify(result)  

@app.route('/api/capitals', methods=['DELETE', 'GET', 'PUT'])
def capitals():
    """deletes, fetchs and inserts capitals from/to datastore"""
    capitalsds = capitalsdsutility.Capitals()

    if request.method == "GET":
        ds = datastore.Client(project='hackathon-team-011')
        query = ds.query(kind="capitals")
        results = get_query_results(query)

        result = [parse_capital(obj) for obj in results]
        return jsonify(result)
    elif request.method == "PUT":
        inputobj = request.get_json()

        capitalid = inputobj['id']
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

    return "Request method not implemented yet: " + request.method

        

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
