from app import app
import redis
import json
import os
import sys
from flask import render_template, flash, redirect, request, make_response, Response, jsonify, send_file
import platform
import io
from minio import Minio
import hashlib
import base64
from playsound import playsound

redisHost = os.getenv("REDIS_HOST") or "localhost"
redisPort = os.getenv("REDIS_PORT") or 6379
minioHost = os.getenv("MINIO_HOST") or "localhost"
minioPort = os.getenv("MINIO_PORT") or 9000
minioUser = "rootuser"
minioPasswd = "rootpass123"
minioFinalAddress = minioHost + ":" + minioPort
minioClient = Minio(minioFinalAddress,
               secure=False,
               access_key=minioUser,
               secret_key=minioPasswd)
redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
bucketName = "queue"
############# LOCAL CONFIGURATION
# redisHost='localhost'
# redisPort=6379
# minioUser = "rootuser"
# minioPasswd = "rootpass123"
# # minioFinalAddress = minioHost + ":" + minioPort
# minioClient = Minio('localhost:9000',
#                secure=False,
#                access_key=minioUser,
#                secret_key=minioPasswd)
# bucketName = "queue"
# redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)

def log_debug(message):
    print("DEBUG:", message, file=sys.stdout)
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    redisClient.lpush('logging', f"{message}")

def log_info(message):
    print("INFO:", message, file=sys.stdout)
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    redisClient.lpush('logging', f"{message}")

@app.route('/', methods=['GET'])
def hello():
    return '<h1> Music Separation Server</h1><p> Use a valid endpoint </p>'


@app.route('/apiv1')
def root():
        log_info("Request to root endpoint.")
        return app.send_static_file('index.html')

@app.route('/apiv1/separate', methods=['POST'])
def separate():
        log_debug("Received a POST request to separate endpoint.")
        data = request.get_json()
        mp3_base64_encoded = data.get("mp3", "Default_MP3")
        # Creating Hash for request
        hash_object = hashlib.sha256(mp3_base64_encoded.encode())
        hash_hex = hash_object.hexdigest()
        log_debug("hash_hex is: " + hash_hex)

        
        # Decoding and pasting in mp3 file which will be deleted later
        mp3_base64_decoded = base64.b64decode(mp3_base64_encoded)
        with open("temp.mp3", "wb") as mp3_file:
                mp3_file.write(mp3_base64_decoded)
        
        # Pushing to redis queue
        redisClient.lpush('toWorkers', f" Key of Song to be changed is: {hash_hex}")
        log_debug("Pushed to redis queue")
        # Checking for minio bucket
        if minioClient.bucket_exists(bucketName):
                log_debug("Queue Bucket exists")
        else:
                minioClient.make_bucket(bucketName)
                log_debug("Queue Bucket did not exist. Bucket has been created")
        log_debug("Placing song hash in Queue Bucket")
        result = minioClient.fput_object(bucketName, hash_hex, 'temp.mp3')
        print(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )
        log_debug("placed file in mp3 bucket: " + hash_hex)
        response_data = {
        "hash": hash_hex, 
        "reason": "Song enqueued for separation"}
        return jsonify(response_data)

@app.route('/apiv1/queue', methods=['GET'])
def queue():
        log_info("Received a GET request to queue endpoint.")
        elements = redisClient.lrange("toWorkers", 0, -1)
        # 'elements' will now contain all elements from the list
        elements = [element.decode('utf-8') for element in elements]
        response_data = {
                "queue": elements}
        return jsonify(response_data)
@app.route('/apiv1/<songhash>/<type>', methods=['GET'])
def get_track(songhash, type):
        log_info("Received a GET request to play the track")
        try:  
                # data = request.get_json()
                # song_hash = data.get("hash")
                # file_type=data.get("type", "bass")
                song_hash = songhash
                file_type = type
                log_debug("Song Hash: " + song_hash)
                log_debug("File Type: " + file_type)
                localFileName = '/app/output.mp3'
                bucketFileName = song_hash + '/' + file_type + ".mp3"
                log_debug("Name of file I am extracting is: " + bucketFileName)
                minioClient.fget_object("output", bucketFileName, localFileName)
                return send_file(localFileName, as_attachment=True, download_name='seperated_track.mp3')
                # playsound(localFileName)
                # response_data = {
                #         "status": "SUCCESS.PLAYED THE SONG"}
                # return jsonify(response_data)
        except Exception as e:
                response_data = {
                        "status": "FAILED",
                        "Exception: ": e}
                return jsonify(response_data)

@app.route('/apiv1/<songhash>/<type>', methods=['DELETE'])
def remove_track(songhash, type):
        log_info("Received a GET request to delete the track")
        try:
                song_hash = songhash
                file_type = type
                log_debug("Song Hash: " + song_hash)
                log_debug("File Type: " + file_type)
                bucketFileName = song_hash + '/' + file_type + ".mp3"
                # minioClient.remove_object("output", song_hash + "/bass.mp3")
                # minioClient.remove_object("output", song_hash + "/vocals.mp3")
                # minioClient.remove_object("output", song_hash + "/drums.mp3")
                # minioClient.remove_object("output", song_hash + "/other.mp3")
                minioClient.remove_object("output", bucketFileName)
                response_data = {
                        "status": "Removed the Songs"}
                return jsonify(response_data)
        except Exception as e:
                response_data = {
                        "status": "FAILED",
                        "Exception: ": e}
                return jsonify(response_data)

       

