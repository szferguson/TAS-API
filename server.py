from flask import Flask, url_for, send_from_directory, request, jsonify
import logging, os
import json
import time
import traceback
import signal
import sql

app = Flask(__name__)
file_handler = logging.FileHandler('server.log')
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
client = sql.Client()

VERSION = "1.0.0"

def signal_handler(sig, frame):
    print('Stopping API')
    client.close()
    exit(0)

@app.route('/', methods=['GET', 'POST'])
def index():
    auth = False
    data = request.get_json()
    if data and data.get("key") in keys:
        auth = True
    return jsonify(
        {
        'name': 'Travel Advisory System',
        'version': VERSION,
        'timestamp': time.time(),
        'authenticated': auth
        }
    )

@app.route('/country', methods = ['GET', 'POST', 'DELETE'])
def country():
    if request.method == 'GET':
        countries = client.get_all_countries()
        return jsonify(countries)
    data = request.get_json()
    key = request.headers.get('api-key')
    country = data.get("country")
    if client.validate_key(key, country):
        if request.method == 'POST':
            return client.add_new_country(country)
        elif request.method == 'DELETE':
            return client.delete_country(country)
    else:
        return jsonify({"error": "Unauthorized request"}), 401

@app.route('/country/<string:country>', methods = ['GET', 'PUT'])
def country_info(country):
    if request.method == 'GET':
        return client.get_country_info(country)
    data = request.get_json()
    key = request.headers.get('api-key')
    if client.validate_key(key, country):
        if request.method == 'PUT':
            return client.update_country_info(country, data.get("description"), data.get("riskLevel"))
    else:
        return jsonify({"error": "Unauthorized request"}), 401

@app.route('/country/<string:country>/advisory', methods = ['GET'])
def get_all_advisories(country):
    if request.method == 'GET':
        return client.get_all_advisories(country)

@app.route('/country/<string:country>/advisory/<string:category>', methods = ['GET', 'PUT'])
def get_advisory(country, category):
    if request.method == 'GET':
        return client.get_advisory(country, category)
    data = request.get_json()
    key = request.headers.get('api-key')
    if client.validate_key(key, country):
        if request.method == 'PUT':
            return client.update_advisory(country, category, data)
    else:
        return jsonify({"error": "Unauthorized request"}), 401

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    app.run(host='0.0.0.0', port=5000, debug=True)
    # print(client.validate_key('canada124', 'Canada'))
