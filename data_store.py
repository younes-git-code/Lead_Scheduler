from configs import datastore_client, batch_size,logging,url
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import datastore
import requests

def get_new_leads():
    logging.info("Fetching new leads from Datastore.")
    query = datastore_client.query(kind="LeadData")
    query.add_filter(filter = datastore.query.PropertyFilter("status", "=", "new"))
    results = list(query.fetch(limit=batch_size))
    logging.info(f"Fetched {len(results)} new lead(s).")
    return results

def process_duplicate_record(record):
    now = datetime.now(ZoneInfo("America/New_York")).isoformat()
    logging.debug(f"Checking for duplicates for lead with phone number: {record.get('phone_number')}")
    query = datastore_client.query(kind="LeadData")
    query.add_filter(filter = datastore.query.PropertyFilter("phone_number", "=", record["phone_number"]))
    query.add_filter(filter=  datastore.query.PropertyFilter("status", "=", "success"))
    results = list(query.fetch())
    is_duplicate = len(results) != 0
    record["status"] = "duplicate" if is_duplicate else "picked"
    record["updated_on"] = now
    datastore_client.put(record)
    return is_duplicate

def leads_push_api(record,status,error):
    now = datetime.now(ZoneInfo("America/New_York")).isoformat()
    record["status"] = status
    record["error"] = error
    record["updated_on"] = now
    datastore_client.put(record)

def process_leads():
    logging.info("Starting lead processing job.")
    new_leads = get_new_leads()
    if not new_leads:
        logging.info("No new leads to process.")
        return

    for index, lead in enumerate(new_leads):
        try:
            phone = lead["phone_number"]
            is_duplicate = process_duplicate_record(lead)
            if is_duplicate:
                logging.info(f"[{index}] Duplicate lead detected - Phone: {phone}")
            else:
                logging.info(f"[{index}] New lead processed - Phone: {phone}")
                lead_push_api(lead)

        except Exception as e:
            logging.error(f"Error processing lead {lead.key.id_or_name}: {e}", exc_info=True)
            continue

    logging.info("Lead processing job completed.")


def lead_push_api(record):
    #"phone": record["phone_number"],

    params = {
        "campaign_id": "892819",
        "site_id": "290783",
        "pub_id": "724048",
        "leadid": record["id"],
        "dob": record["date_of_birth"],
        "address": record["address1"],
        "city": record["city"],
        "fname": record["first_name"],
        "lname": record["last_name"],
        "phone": "2129558900",
        "state": record["state"],
        "zip": record["postal_code"],
        "gender": record["gender"],
        "email": "null",
        "referrer": "null",
        "xxTrustedFormCertUrl": "null",
        "ip": "null",
        "sub_id": "null",
        "show_errors": "y",
        "test": "1"
    }

    logging.info(f"Pushing lead: {record['phone_number']}")

    try:
        response = requests.get(url, params=params)
        logging.info(f"API response status: {response.status_code}")

        if response.status_code == 200:
            if "Success" in response.text:
                logging.info(f"Lead push successful for {record['phone_number']}")
                leads_push_api(record, "success", "")
            else:
                logging.warning(f"Lead push failed (API responded with error): {response.text}")
                leads_push_api(record, "failed", response.text)
        else:
            logging.error(f"Non-200 response: {response.status_code}, body: {response.text}")
            leads_push_api(record, "failed", response.text)
    except Exception as e:
        logging.exception(f"Exception occurred while pushing lead for {record['phone_number']}")
        leads_push_api(record, "failed", str(e))
