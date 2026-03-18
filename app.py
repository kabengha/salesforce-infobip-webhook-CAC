import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

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
        "marqueVehicule__c, ModeleDeVehicule__c, ImmatriculeVehicule__c, Telephone__c "
        "FROM Case "
        "WHERE Origin = 'CAC' "
        "AND RecordTypeId = '01268000000kfEaAAI' "
        "AND CreatedDate >= 2026-03-18T00:00:00Z "
        "ORDER BY CreatedDate DESC"
    )

    url = f"{instance_url}/services/data/v59.0/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    params = {"q": soql}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def normalize_record(record: dict) -> dict:
    created_date = record.get("CreatedDate", "")
    return {
        "id": record.get("Id"),
        "case_number": record.get("CaseNumber"),
        "created_date": created_date[:10] if created_date else None,
        "special": record.get("Special__c"),
        "nom_complet": record.get("NomComplet__c"),
        "marque": record.get("marqueVehicule__c"),
        "modele": record.get("ModeleDeVehicule__c"),
        "immatricule": record.get("ImmatriculeVehicule__c"),
        "telephone": record.get("Telephone__c"),
    }

def main():
    print(f"[{datetime.utcnow().isoformat()}] Cron job started")

    token_data = get_salesforce_token()
    access_token = token_data["access_token"]
    instance_url = token_data["instance_url"]

    result = fetch_cases(access_token, instance_url)
    records = result.get("records", [])

    print(f"Cases found: {len(records)}")

    filtered = [normalize_record(r) for r in records]

    for item in filtered:
        print(json.dumps(item, ensure_ascii=False))

    print(f"[{datetime.utcnow().isoformat()}] Cron job finished")

if __name__ == "__main__":
    main()
