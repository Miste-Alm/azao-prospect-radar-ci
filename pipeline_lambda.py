import os
import json
import csv
import gzip
import io
import datetime
import logging
import traceback
import boto3
import requests
import uuid

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constantes globales (hors variables d'environnement lues dans le handler)
DECP_BASE_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/decp-v3-marches-valides/exports/csv"
REGION_CODE = "11" # IDF
FILE_PATH_DECP_CSV_TMP = "/tmp/decp_data_30_days.csv"
FILE_PATH_TED_JSON_TMP = "/tmp/ted_results.json"

# Variable pour le chemin de recherche TED, avec une valeur par défaut
TED_SEARCH_PATH_DEFAULT = "/v3/notices/search" # Conforme à la doc PublicExpertSearchRequestV1

def get_date_range(days=30):
    """Retourne les dates de début et de fin pour les N derniers jours au format YYYY-MM-DD."""
    today = datetime.date.today()
    end_date = today
    start_date = today - datetime.timedelta(days=days -1)
    return start_date.isoformat(), end_date.isoformat()

def collect_decp_data(start_date, end_date, log_stream):
    """Collecte les données DECP pour la période et le lieu spécifiés."""
    logger.info("Début de la collecte DECP.")
    log_stream.write(f"DECP collection started for period {start_date} to {end_date}.\n")
    sirens_sirets = set()
    params = {
        "where": f"datenotification >= date'{start_date}' AND datenotification <= date'{end_date}' AND lieuexecution_code = '{REGION_CODE}'",
        "limit": -1, "offset": 0, "timezone": "UTC", "use_labels": "false", "epsg": 4326, "delimiter": ";", "compressed": "true"
    }
    try:
        logger.info(f"DECP API URL: {DECP_BASE_URL} with params: {params.get('where')}")
        response = requests.get(DECP_BASE_URL, params=params, timeout=180)
        response.raise_for_status()
        content = response.content
        if response.headers.get("Content-Encoding") == "gzip" or content.startswith(b'\x1f\x8b'):
            logger.info("Décompression des données DECP Gzip.")
            log_stream.write("DECP data is gzipped. Decompressing...\n")
            decompressed_content = gzip.decompress(content)
        else:
            decompressed_content = content
        with open(FILE_PATH_DECP_CSV_TMP, 'wb') as f_out:
            f_out.write(decompressed_content)
        file_size_kb = len(decompressed_content) / 1024
        logger.info(f"Données DECP sauvegardées dans {FILE_PATH_DECP_CSV_TMP} ({file_size_kb:.2f} KB).")
        log_stream.write(f"DECP data saved to {FILE_PATH_DECP_CSV_TMP} ({file_size_kb:.2f} KB).\n")
        with io.TextIOWrapper(io.BytesIO(decompressed_content), encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                for i in range(1, 4):
                    titulaire_id = row.get(f'titulaire_id_{i}')
                    if titulaire_id and titulaire_id.strip():
                        sirens_sirets.add(titulaire_id.strip())
            logger.info(f"{len(sirens_sirets)} identifiants uniques extraits de DECP.")
            log_stream.write(f"{len(sirens_sirets)} unique identifiers extracted from DECP.\n")
    except requests.exceptions.Timeout:
        logger.error("Timeout lors de la collecte DECP.")
        log_stream.write(f"Timeout during DECP collection.\n{traceback.format_exc()}\n")
        return set()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la collecte DECP: {e}")
        log_stream.write(f"Error during DECP collection: {e}\n{traceback.format_exc()}\n")
        return set()
    except Exception as e:
        logger.error(f"Erreur inattendue lors du traitement DECP: {e}")
        log_stream.write(f"Unexpected error during DECP processing: {e}\n{traceback.format_exc()}\n")
        return set()
    logger.info("Fin de la collecte DECP.")
    log_stream.write("DECP collection finished.\n")
    return sirens_sirets

def collect_ted_data(start_date, end_date, log_stream, ted_api_key, ted_api_base_url, ted_search_path):
    """Collecte les données TED pour la période et le lieu spécifiés en utilisant PublicExpertSearchRequestV1."""
    if not (ted_api_base_url and ted_api_key and ted_search_path):
        msg = "Variables TED (TED_API_ENDPOINT, AZAO_TED_API_KEY ou TED_SEARCH_PATH) non configurées. Collecte TED ignorée."
        logger.warning(msg)
        log_stream.write(f"{msg}\n")
        return False, 0
    
    full_ted_url = f"{ted_api_base_url.rstrip('/')}{ted_search_path}"
    msg_start = f"TED collection started for period {start_date} to {end_date} using URL: {full_ted_url}."
    logger.info("Début de la collecte TED.") # Generic start log
    logger.info(msg_start) # Specific start log for caplog
    log_stream.write(f"{msg_start}\n")
    
    all_notices_data = {"results": [], "total": 0}
    current_page = 0
    page_size = 100
    max_pages_to_fetch = 10
    total_results_processed_successfully = 0

    headers = {"X-Api-Key": ted_api_key, "Content-Type": "application/json"}

    while current_page < max_pages_to_fetch:
        payload = {
            "filter": {"publicationDate": {"from": start_date, "to": end_date}},
            "nutsCodes": ["FR10"], "page": current_page, "size": page_size
        }
        msg_api_call = f"Calling TED API page {current_page + 1}, pageSize {page_size}. URL: {full_ted_url}. Payload: {json.dumps(payload)}"
        logger.info(msg_api_call) # Log for caplog
        log_stream.write(f"{msg_api_call}\n")

        try:
            response = requests.post(full_ted_url, headers=headers, json=payload, timeout=60)
            
            if response.status_code >= 400:
                error_msg = f"Erreur API TED ({response.status_code}) for page {current_page + 1}: {response.text}"
                logger.error(error_msg) # Log for caplog
                log_stream.write(f"{error_msg}\n")
                return False, total_results_processed_successfully 
            
            data = response.json()
            notices_on_page = data.get("results", [])
            
            if not isinstance(notices_on_page, list):
                error_msg_type = f"Unexpected TED response for page {current_page + 1}: 'results' is not a list. Received: {type(notices_on_page)}"
                logger.error(error_msg_type)
                log_stream.write(f"{error_msg_type}\n")
                return False, total_results_processed_successfully

            all_notices_data["results"].extend(notices_on_page)
            total_results_processed_successfully += len(notices_on_page)
            
            msg_notices_received = f"{len(notices_on_page)} notices received from TED for page {current_page + 1}."
            logger.info(msg_notices_received) # Log for caplog
            log_stream.write(f"{msg_notices_received}\n")

            if len(notices_on_page) < page_size:
                msg_last_page = "Last page of TED results reached (items < page_size)."
                logger.info(msg_last_page) # Log for caplog
                log_stream.write(f"{msg_last_page}\n")
                break
            
            current_page += 1

        except requests.exceptions.RequestException as e:
            error_msg_req = f"TED Request Error for page {current_page + 1}: {e}"
            logger.error(error_msg_req)
            log_stream.write(f"{error_msg_req}\n{traceback.format_exc()}\n")
            return False, total_results_processed_successfully 
        except json.JSONDecodeError as e:
            error_msg_json = f"TED JSON Decode Error for page {current_page + 1}: {e}. Response: {response.text[:500]}..."
            logger.error(error_msg_json)
            log_stream.write(f"{error_msg_json}\n")
            return False, total_results_processed_successfully
        except Exception as e:
            error_msg_unexp = f"Unexpected error during TED collection for page {current_page + 1}: {e}"
            logger.error(error_msg_unexp)
            log_stream.write(f"{error_msg_unexp}\n{traceback.format_exc()}\n")
            return False, total_results_processed_successfully

    if all_notices_data["results"]:
        all_notices_data["total"] = total_results_processed_successfully
        with open(FILE_PATH_TED_JSON_TMP, 'w', encoding='utf-8') as f:
            json.dump(all_notices_data, f, ensure_ascii=False, indent=2)
        msg_ted_saved = f"TED data saved to {FILE_PATH_TED_JSON_TMP} ({total_results_processed_successfully} notices)."
        logger.info(msg_ted_saved)
        log_stream.write(f"{msg_ted_saved}\n")
        return True, total_results_processed_successfully
    else:
        msg_no_ted = "No TED notices collected or saved."
        logger.info(msg_no_ted)
        log_stream.write(f"{msg_no_ted}\n")
        return False, 0

def upload_to_s3(s3_client, local_path, bucket_name, s3_key_suffix, log_stream):
    if not os.path.exists(local_path):
        msg = f"Local file {local_path} not found for S3 upload."
        logger.warning(msg)
        log_stream.write(f"{msg}\n")
        return False
    s3_key = f"logs/{s3_key_suffix}"
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        msg_s3_ok = f"File {local_path} uploaded to s3://{bucket_name}/{s3_key}"
        logger.info(msg_s3_ok)
        log_stream.write(f"{msg_s3_ok}\n")
        return True
    except Exception as e:
        msg_s3_err = f"Error during S3 upload of {local_path} to {s3_key}: {e}"
        logger.error(msg_s3_err)
        log_stream.write(f"{msg_s3_err}\n{traceback.format_exc()}\n")
        return False

def lambda_handler(event, context):
    # Clean up potential leftover temp files from previous runs (especially for local testing)
    for temp_file_path in [FILE_PATH_DECP_CSV_TMP, FILE_PATH_TED_JSON_TMP]:
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.info(f"Removed pre-existing temp file: {temp_file_path}")
            except OSError as e:
                logger.warning(f"Could not remove pre-existing temp file {temp_file_path}: {e}")

    azao_s3_bucket_name = os.environ.get("AZAO_S3_BUCKET_NAME")
    azao_ted_api_key = os.environ.get("AZAO_TED_API_KEY")
    ted_api_base_url = os.environ.get("TED_API_ENDPOINT")
    ted_search_path = os.environ.get("TED_SEARCH_PATH", TED_SEARCH_PATH_DEFAULT)

    run_id = str(uuid.uuid4())
    timestamp_str = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_capture_string = io.StringIO()
    log_capture_string.write(f"Lambda run_id: {run_id} started at {timestamp_str}\n")
    logger.info(f"Lambda run_id: {run_id} démarrée. Event: {event}")

    if not azao_s3_bucket_name:
        err_msg = "Variable d'environnement AZAO_S3_BUCKET_NAME non définie."
        logger.error(err_msg)
        log_capture_string.write(f"FATAL ERROR: {err_msg}\n")
        return {"statusCode": 500, "body": json.dumps({"error": err_msg, "run_id": run_id})}

    s3_client = boto3.client('s3')
    leads_sirens_sirets = set()
    final_status_code = 200
    error_occurred_in_pipeline = False
    decp_file_uploaded = False
    ted_file_uploaded = False
    leads_csv_file_uploaded = False
    sirens_decp_count = 0
    num_ted_results_val = 0
    sorted_leads_count = 0

    try:
        start_date, end_date = get_date_range(30)
        log_capture_string.write(f"Date range for data collection: {start_date} to {end_date}\n")
        sirens_decp = collect_decp_data(start_date, end_date, log_capture_string)
        sirens_decp_count = len(sirens_decp)
        leads_sirens_sirets.update(sirens_decp)
        if os.path.exists(FILE_PATH_DECP_CSV_TMP):
            decp_file_uploaded = upload_to_s3(s3_client, FILE_PATH_DECP_CSV_TMP, azao_s3_bucket_name, f"decp_data_{timestamp_str}.csv", log_capture_string)
        
        ted_data_collected, num_ted_results = collect_ted_data(start_date, end_date, log_capture_string, azao_ted_api_key, ted_api_base_url, ted_search_path)
        num_ted_results_val = num_ted_results
        if ted_data_collected and os.path.exists(FILE_PATH_TED_JSON_TMP):
             ted_file_uploaded = upload_to_s3(s3_client, FILE_PATH_TED_JSON_TMP, azao_s3_bucket_name, f"ted_results_{timestamp_str}.json", log_capture_string)
        
        leads_csv_filename = f"azao_leads_{timestamp_str}.csv"
        leads_csv_path = f"/tmp/{leads_csv_filename}"
        sorted_leads = sorted(list(leads_sirens_sirets))
        sorted_leads_count = len(sorted_leads)
        with open(leads_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(["siren_or_siret"])
            for lead_id in sorted_leads:
                writer.writerow([lead_id])
        logger.info(f"Fichier des leads créé : {leads_csv_path} ({sorted_leads_count} leads).")
        log_capture_string.write(f"Leads CSV file created: {leads_csv_path} ({sorted_leads_count} leads).\n")
        leads_csv_file_uploaded = upload_to_s3(s3_client, leads_csv_path, azao_s3_bucket_name, leads_csv_filename, log_capture_string)
    except Exception as e:
        logger.error(f"Erreur fatale dans le lambda_handler: {e}")
        log_capture_string.write(f"FATAL ERROR in lambda_handler: {e}\n{traceback.format_exc()}\n")
        final_status_code = 500
        error_occurred_in_pipeline = True
    finally:
        log_json_filename = f"azao_log_{timestamp_str}.json"
        log_json_path = f"/tmp/{log_json_filename}"
        log_summary = {
            "run_id": run_id, "timestamp_utc": timestamp_str,
            "status_code_lambda_return": final_status_code,
            "error_occurred_in_pipeline": error_occurred_in_pipeline,
            "decp_data_collection": {"file_uploaded_to_s3": decp_file_uploaded, "sirens_extracted": sirens_decp_count},
            "ted_data_collection": {"file_uploaded_to_s3": ted_file_uploaded, "results_processed": num_ted_results_val},
            "leads_generated": {"count": sorted_leads_count, "file_uploaded_to_s3": leads_csv_file_uploaded},
            "log_trace_snippet": log_capture_string.getvalue().splitlines()[:50]
        }
        with open(log_json_path, 'w', encoding='utf-8') as f_log_json:
            json.dump(log_summary, f_log_json, indent=2, ensure_ascii=False)
        logger.info(f"Fichier log JSON créé : {log_json_path}")
        if azao_s3_bucket_name:
            upload_to_s3(s3_client, log_json_path, azao_s3_bucket_name, log_json_filename, log_capture_string)
        log_capture_string.close()
    logger.info(f"Exécution Lambda run_id: {run_id} terminée avec le code: {final_status_code}")
    return {
        "statusCode": final_status_code,
        "body": json.dumps({
            "message": "Pipeline execution finished.", "run_id": run_id,
            "leads_found": sorted_leads_count,
            "log_file_s3_key": f"logs/{log_json_filename}" if azao_s3_bucket_name else "S3 bucket not configured"
        })
    }

if __name__ == '__main__':
    os.environ['AZAO_S3_BUCKET_NAME'] = 'votre-bucket-s3-test-local'
    os.environ['AZAO_TED_API_KEY'] = 'votre-cle-api-ted-local'
    os.environ['TED_API_ENDPOINT'] = 'https://mock.ted.europa.eu'
    os.environ['TED_SEARCH_PATH'] = '/v3/notices/search'

    class MockS3ClientLocal:
        def upload_file(self, Filename, Bucket, Key):
            target_dir = f"./s3_mock_uploads/{Bucket}/{os.path.dirname(Key)}"
            if not os.path.exists(target_dir): os.makedirs(target_dir)
            target_path = os.path.join(target_dir, os.path.basename(Key))
            with open(target_path, 'wb') as mock_s3_file, open(Filename, 'rb') as local_file:
                mock_s3_file.write(local_file.read())
            print(f"[MOCK S3 LOCAL] Uploaded {Filename} to {target_path}")
            return True
    _boto3_client_orig = boto3.client
    def _mock_boto3_client_local(service_name, *args, **kwargs):
        if service_name == 's3': return MockS3ClientLocal()
        return _boto3_client_orig(service_name, *args, **kwargs)
    boto3.client = _mock_boto3_client_local
    logger.info("Début du test local du lambda_handler...")
    result = lambda_handler({}, None)
    logger.info(f"Résultat du test local: {json.dumps(result, indent=2)}")
    boto3.client = _boto3_client_orig

