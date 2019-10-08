import argparse
import json
import datetime
import operator
import googleapiclient.discovery
from google.cloud import storage


def create_service():
   """Creates the service object for calling the Cloud Storage API."""
   # Construct the service object for interacting with the Cloud Storage API -
   # the 'storage' service, at version 'v1'.
   # You can browse other available api services and versions here:
   #     https://developers.google.com/api-client-library/python/apis/
   return googleapiclient.discovery.build('storage', 'v1')


def list_bucket(bucket):
   """Returns a list of metadata of the objects within the given bucket.
   [
     {
   "kind": "storage#object",
   "contentType": "image/png",
   "name": "Screenshot 2019-10-01 at 12.16.12 PM.png",
   "etag": "COW99LuIiuUCEAE=",
   "generation": "1570448474971877",
   "md5Hash": "nHgkwrcnf3WEZoDn4ueqeQ==",
   "bucket": "dadada-for-zuckerberg",
   "updated": "2019-10-07T11:41:14.971Z",
   "timeStorageClassUpdated": "2019-10-07T11:41:14.971Z",
   "crc32c": "t94wTg==",
   "metageneration": "1",
   "storageClass": "COLDLINE",
   "mediaLink": "https://www.googleapis.com/download/storage/v1/b/dadada-for-zuckerberg/o/Screenshot%202019-10-01%20at%2012.16.12%20PM.png?generation=1570448474971877&alt=media",
   "timeCreated": "2019-10-07T11:41:14.971Z",
   "id": "dadada-for-zuckerberg/Screenshot 2019-10-01 at 12.16.12 PM.png/1570448474971877",
   "selfLink": "https://www.googleapis.com/storage/v1/b/dadada-for-zuckerberg/o/Screenshot%202019-10-01%20at%2012.16.12%20PM.png",
   "size": "469067"
 }
   ]

   """

   service = create_service()

   # Create a request to objects.list to retrieve a list of objects.
   fields_to_return = \
       'nextPageToken,items(timeCreated,name,size,contentType,metadata(my-key))'
   req = service.objects().list(bucket=bucket, fields=fields_to_return)

   all_objects = []
   # If you have too many items to list in one request, list_next() will
   # automatically handle paging with the pageToken.
   while req:
       resp = req.execute()
       all_objects.extend(resp.get('items', []))
       req = service.objects().list_next(req, resp)
   return all_objects

def custom_getter(obj):
   t = obj['timeCreated']
   return datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')


def delete_obj(bucket_name,blob_name):
   storage_client = storage.Client()
   bucket = storage_client.get_bucket(bucket_name)
   blob = bucket.blob(blob_name)
   blob.delete()

def get_info(bucket):
   obj_in_bucket =  list_bucket(bucket)
   sorted_x = sorted(obj_in_bucket, key=lambda i: custom_getter(i))
   tmpobj = sorted_x.pop(0)
   print(tmpobj)
   delete_obj(bucket,tmpobj['name'])
   return (obj_in_bucket)


def response_func2(data,context):
   return main('BUCKET_NAME')
           
def main(bucket):
   return str(get_info(bucket))
