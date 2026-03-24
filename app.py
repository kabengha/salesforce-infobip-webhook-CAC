import os
import json
import requests
from datetime import datetime, UTC

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


def now_utc():
    return datetime.now(UTC).isoformat()


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
        "AND CreatedDate >= 2026-03-18T00:00:00Z "
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


def normalize_phone(phone: str | None) -> str | None:
    if not phone:
        return None

    value = str(phone).strip().replace(" ", "").replace("-", "")

    if value.lower() == "null":
        return None

    if value.startswith("00"):
        value = value[2:]

    if value.startswith("0"):
        value = "212" + value[1:]

    if not value.isdigit():
        return None

    if not value.startswith("212"):
        return None

    return value


def clean_value(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "null":
        return None
    return text


def validate_record(record: dict):
    required_fields = {
        "NomComplet__c": "Nom complet manquant",
        "marqueVehicule__c": "Marque véhicule manquante",
        "ModeleDeVehicule__c": "Modèle véhicule manquant",
        "ImmatriculeVehicule__c": "Immatriculation manquante",
        "Telephone__c": "Téléphone manquant",
        "IDPolice__c": "ID Police manquant",
    }

    for field, reason in required_fields.items():
        if clean_value(record.get(field)) is None:
            return False, reason

    normalized_phone = normalize_phone(record.get("Telephone__c"))
    if not normalized_phone:
        return False, "Téléphone invalide"

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


def update_case_special_true(access_token: str, instance_url: str, case_id: str):
    url = f"{instance_url}/services/data/v59.0/sobjects/Case/{case_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"Special__c": True}

    response = requests.patch(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()


def save_daily_report(report: dict):
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    filename = f"report_{date_str}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


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
        "sent": [],
        "failed": [],
    }

    for record in records:
        case_id = record.get("Id")
        case_number = record.get("CaseNumber")

        is_valid, reason = validate_record(record)
        if not is_valid:
            report["failed_count"] += 1
            report["failed"].append({
                "case_id": case_id,
                "case_number": case_number,
                "telephone": record.get("Telephone__c"),
                "reason": reason,
            })
            continue

        success, response_data = send_whatsapp_template(record)

        if success:
            try:
                update_case_special_true(access_token, instance_url, case_id)
                report["sent_count"] += 1
                report["sent"].append({
                    "case_id": case_id,
                    "case_number": case_number,
                    "telephone": normalize_phone(record.get("Telephone__c")),
                    "name": record.get("NomComplet__c"),
                    "status": "sent",
                    "infobip_response": response_data,
                })
            except Exception as e:
                report["failed_count"] += 1
                report["failed"].append({
                    "case_id": case_id,
                    "case_number": case_number,
                    "telephone": record.get("Telephone__c"),
                    "reason": f"Message envoyé mais échec MAJ Salesforce: {str(e)}",
                    "infobip_response": response_data,
                })
        else:
            report["failed_count"] += 1
            report["failed"].append({
                "case_id": case_id,
                "case_number": case_number,
                "telephone": record.get("Telephone__c"),
                "reason": "Erreur Infobip",
                "infobip_response": response_data,
            })

    save_daily_report(report)

    print(json.dumps({
        "date": report["date"],
        "total_cases": report["total_cases"],
        "sent_count": report["sent_count"],
        "failed_count": report["failed_count"],
    }, ensure_ascii=False))

    print(f"[{now_utc()}] Cron job finished")


if __name__ == "__main__":
    main()
