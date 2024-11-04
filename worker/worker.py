import sys
import os
import redis
from minio import Minio
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

def log_debug(message):
    print("DEBUG:", message, file=sys.stdout)
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    redisClient.lpush('logging', f"{message}")

def log_info(message):
    print("INFO:", message, file=sys.stdout)
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
    redisClient.lpush('logging', f"{message}")

# redisHost = "localhost"
# redisPort = 6379
# # minioHost = os.getenv("MINIO_HOST") or "localhost"
# # minioPort = os.getenv("MINIO_PORT") or 9000
# minioUser = "rootuser"
# minioPasswd = "rootpass123"
# minioFinalAddress = 'localhost:9000'
# minioClient = Minio(minioFinalAddress,
#                secure=False,
#                access_key=minioUser,
#                secret_key=minioPasswd)
# redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0)
bucketName = "output"
while True:
    try:
        work = redisClient.blpop("toWorkers", timeout=0)
        ##
        ## Work will be a tuple. work[0] is the name of the key from which the data is retrieved
        ## and work[1] will be the text log message. The message content is in raw bytes format
        ## e.g. b'foo' and the decoding it into UTF-* makes it print in a nice manner.
        ##
        hash_hex = work[1].decode('utf-8').split(':')[1].strip()
        bucketName = "queue"
        response=None
        # Get data of an object.
        try:
            response = minioClient.fget_object(bucketName, hash_hex, './docker-facebook-demucs/input/temp.mp3')
            print("Recieved Object with key: ", hash_hex)
            # os.system(command)
            # print("Seperation is Done!!!!!!!!!!!")
            command = f"python3 -m demucs.separate --out ./docker-facebook-demucs/output ./docker-facebook-demucs/input/temp.mp3 --mp3"
            os.system(command)
            print("Seperation is Done!!!!!!!!!!!")
    # Read data from response.
        finally:
            # if response != None:
            #     response.close()
            #     response.release_conn()
            base_output_file_location = './docker-facebook-demucs/' + 'output/htdemucs/temp/'
            bass_file_location = base_output_file_location + 'bass.mp3'
            drums_file_location = base_output_file_location + 'drums.mp3'
            vocals_file_location = base_output_file_location + 'vocals.mp3'
            other_file_location = base_output_file_location + 'other.mp3'
            bucketName = "output"
            if minioClient.bucket_exists(bucketName):
                log_debug("Output Bucket exists")
            else:
                minioClient.make_bucket(bucketName)
                log_debug("Queue Bucket did not exist. Bucket has been created")
            log_debug("Placing song hash in Queue Bucket")
            folderName = hash_hex + "/"
            result = minioClient.fput_object(bucketName, folderName + 'bass.mp3', bass_file_location)
            print(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )
            result = minioClient.fput_object(bucketName, folderName + 'drums.mp3', drums_file_location)
            print(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )
            result = minioClient.fput_object(bucketName, folderName + 'vocals.mp3', vocals_file_location)
            print(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )
            result = minioClient.fput_object(bucketName, folderName + 'other.mp3', other_file_location)
            print(
                "created {0} object; etag: {1}, version-id: {2}".format(
                    result.object_name, result.etag, result.version_id,
                )
            )
            print("Hellooo. I am doneeeee")

        
    except Exception as exp:
        print(f"Exception raised in log loop: {str(exp)}")
    sys.stdout.flush()
    sys.stderr.flush()