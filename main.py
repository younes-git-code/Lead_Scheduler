from configs import logging,subscription_path,subscriber
from data_store import process_leads


def callback(message):
    logging.info("Message Triggered")
    process_leads()
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

