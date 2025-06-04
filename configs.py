from google.cloud import datastore
from google.cloud import pubsub_v1
import os
import logging

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "spartan-vine-456818-e9-c9000351f0f2.json"
#datastore_name = 'lead-files1'
#url = "http://lead.flexdirectpath.com/post/"
# batch_size =  210
# subscription_path="projects/spartan-vine-456818-e9/subscriptions/lead-pub-sub-sub"

url = os.getenv("CLIENT_URL")
datastore_name = os.getenv("DATASTORE_NAME")
batch_size =  int(os.getenv("BATCH_SIZE"))
subscription_path = os.getenv("SUBSCRIPTION_PATH")


subscriber = pubsub_v1.SubscriberClient()
datastore_client = datastore.Client(database=datastore_name)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)


