import os
import json
import requests
import gspread
from datetime import datetime, UTC
from google.oauth2.service_account import Credentials


SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

INFOBIP_BASE_URL = os.getenv("INFOBIP_BASE_URL", "").rstrip("/")
INFOBIP_API_KEY = os.getenv("INFOBIP_API_KEY")
INFOBIP_SENDER = os.getenv("INFOBIP_SENDER")
INFOBIP_TEMPLATE_NAME = os.getenv("INFOBIP_TEMPLATE_NAME", "afma_cac")
INFOBIP_TEMPLATE_LANGUAGE = os.getenv("INFOBIP_TEMPLATE_LANGUAGE", "fr")

GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Rapport WhatsApp CAC")
GOOGLE_SHEET_WORKSHEET = os.getenv("GOOGLE_SHEET_WORKSHEET", "Logs")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

SEND_WHATSAPP = os.getenv("SEND_WHATSAPP", "false").lower() == "true"


def now_utc():
    return datetime.now(UTC).isoformat()


def clean_value(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "null":
        return None
    return text


def normalize_phone(phone):
    value = clean_value(phone)
    if not value:
        return None

    value = value.replace(" ", "").replace("-", "")

    if value.startswith("00"):
        value = value[2:]

    if value.startswith("0"):
        value = "212" + value[1:]

    if not value.isdigit():
        return None

    if not value.startswith("212"):
        return None

    if len(value) < 11 or len(value) > 15:
        return None

    return value


def get_salesforce_token():
    url = "https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
        "username": SF_USERNAME,
        "password": SF_PASSWORD + SF_SECURITY_TOKEN,
    }

    response = requests.post(url, data=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_cases(access_token: str, instance_url: str):
    soql = (
        "SELECT Id, CaseNumber, CreatedDate, Special__c, NomComplet__c, "
        "marqueVehicule__c, ModeleDeVehicule__c, ImmatriculeVehicule__c, "
        "Telephone__c, IDPolice__c "
        "FROM Case "
        "WHERE Origin = 'CAC' "
        "AND RecordTypeId = '01268000000kfEaAAI' "
        "AND CreatedDate >= 2026-03-19T00:00:00Z "
        "AND Special__c = false "
        "ORDER BY CreatedDate DESC"
    )

    url = f"{instance_url}/services/data/v59.0/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers, params={"q": soql}, timeout=30)
    response.raise_for_status()
    return response.json()


def update_case_special_true(access_token: str, instance_url: str, case_id: str):
    url = f"{instance_url}/services/data/v59.0/sobjects/Case/{case_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"Special__c": True}

    response = requests.patch(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()


def validate_record(record: dict):
    missing_fields = []

    required_fields = {
        "NomComplet__c": "Nom complet manquant",
        "marqueVehicule__c": "Marque véhicule manquante",
        "ModeleDeVehicule__c": "Modèle véhicule manquant",
        "ImmatriculeVehicule__c": "Immatriculation manquante",
        "Telephone__c": "Téléphone manquant",
        "IDPolice__c": "ID Police manquant",
    }

    for field, label in required_fields.items():
        if clean_value(record.get(field)) is None:
            missing_fields.append(label)

    normalized_phone = normalize_phone(record.get("Telephone__c"))
    if not normalized_phone:
        missing_fields.append("Téléphone invalide")

    if missing_fields:
        return False, " | ".join(missing_fields)

    return True, ""


def build_template_payload(record: dict):
    to_number = normalize_phone(record.get("Telephone__c"))

    return {
        "messages": [
            {
                "from": INFOBIP_SENDER,
                "to": to_number,
                "content": {
                    "templateName": INFOBIP_TEMPLATE_NAME,
                    "templateData": {
                        "body": {
                            "placeholders": [
                                clean_value(record.get("NomComplet__c")),
                                clean_value(record.get("marqueVehicule__c")),
                                clean_value(record.get("ModeleDeVehicule__c")),
                                clean_value(record.get("ImmatriculeVehicule__c")),
                                clean_value(record.get("IDPolice__c")),
                            ]
                        }
                    },
                    "language": INFOBIP_TEMPLATE_LANGUAGE
                }
            }
        ]
    }


def send_whatsapp_template(record: dict):
    if not SEND_WHATSAPP:
        return False, {
            "mode": "test",
            "message": "Envoi désactivé (SEND_WHATSAPP=false)"
        }

    url = f"{INFOBIP_BASE_URL}/whatsapp/1/message/template"
    headers = {
        "Authorization": f"App {INFOBIP_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = build_template_payload(record)

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text}

    if response.ok:
        return True, data

    return False, data


def init_google_sheet():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("La variable d'environnement GOOGLE_SERVICE_ACCOUNT_JSON est manquante.")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=scope
    )

    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_SHEET_WORKSHEET)
    return sheet


def save_report_to_sheets(report: dict):
    sheet = init_google_sheet()

    details_json = json.dumps({
        "sent": report["sent"],
        "failed": report["failed"]
    }, ensure_ascii=False)

    row = [
        report["date"],
        report["total_cases"],
        report["sent_count"],
        report["failed_count"],
        report["failed_missing_or_invalid_count"],
        report["failed_infobip_count"],
        report["failed_salesforce_update_count"],
        details_json
    ]

    sheet.append_row(row)


def main():
    print(f"[{now_utc()}] Cron job started")

    token_data = get_salesforce_token()
    access_token = token_data["access_token"]
    instance_url = token_data["instance_url"]

    result = fetch_cases(access_token, instance_url)
    records = result.get("records", [])

    report = {
        "date": datetime.now(UTC).strftime("%d/%m/%Y"),
        "total_cases": len(records),
        "sent_count": 0,
        "failed_count": 0,
        "failed_missing_or_invalid_count": 0,
        "failed_infobip_count": 0,
        "failed_salesforce_update_count": 0,
        "sent": [],
        "failed": [],
    }

    for record in records:
        case_id = record.get("Id")
        case_number = record.get("CaseNumber")
        raw_phone = record.get("Telephone__c")
        normalized_phone = normalize_phone(raw_phone)

        is_valid, reason = validate_record(record)
        if not is_valid:
            report["failed_count"] += 1
            report["failed_missing_or_invalid_count"] += 1
            report["failed"].append({
                "case_id": case_id,
                "case_number": case_number,
                "telephone_raw": raw_phone,
                "telephone_normalized": normalized_phone,
                "name": record.get("NomComplet__c"),
                "status": "not_sent",
                "reason_type": "missing_or_invalid_data",
                "reason": reason,
            })
            continue

        success, response_data = send_whatsapp_template(record)

        if not SEND_WHATSAPP:
            report["failed_count"] += 1
            report["failed_infobip_count"] += 1
            report["failed"].append({
                "case_id": case_id,
                "case_number": case_number,
                "telephone_raw": raw_phone,
                "telephone_normalized": normalized_phone,
                "name": record.get("NomComplet__c"),
                "status": "not_sent",
                "reason_type": "test_mode_no_send",
                "reason": "Mode test actif : aucun message envoyé",
                "infobip_response": response_data,
            })
            continue

        if success:
            try:
                update_case_special_true(access_token, instance_url, case_id)
                report["sent_count"] += 1
                report["sent"].append({
                    "case_id": case_id,
                    "case_number": case_number,
                    "telephone_raw": raw_phone,
                    "telephone_normalized": normalized_phone,
                    "name": record.get("NomComplet__c"),
                    "status": "sent",
                    "infobip_response": response_data,
                })
            except Exception as e:
                report["failed_count"] += 1
                report["failed_salesforce_update_count"] += 1
                report["failed"].append({
                    "case_id": case_id,
                    "case_number": case_number,
                    "telephone_raw": raw_phone,
                    "telephone_normalized": normalized_phone,
                    "name": record.get("NomComplet__c"),
                    "status": "not_sent",
                    "reason_type": "salesforce_update_error",
                    "reason": f"Message envoyé mais échec MAJ Salesforce: {str(e)}",
                    "infobip_response": response_data,
                })
        else:
            report["failed_count"] += 1
            report["failed_infobip_count"] += 1
            report["failed"].append({
                "case_id": case_id,
                "case_number": case_number,
                "telephone_raw": raw_phone,
                "telephone_normalized": normalized_phone,
                "name": record.get("NomComplet__c"),
                "status": "not_sent",
                "reason_type": "infobip_error",
                "reason": "Erreur Infobip",
                "infobip_response": response_data,
            })

    save_report_to_sheets(report)

    print("===== RAPPORT JOURNALIER =====")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"[{now_utc()}] Cron job finished")


if __name__ == "__main__":
    main()
