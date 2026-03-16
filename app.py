from flask import Flask, request, jsonify
import json

app = Flask(__name__)

@app.route("/webhook/salesforce", methods=["POST"])
def salesforce_webhook():

    data = request.json

    print("Data received from Salesforce:", data)

    with open("salesforce_logs.txt", "a") as f:
        f.write(json.dumps(data) + "\n")

    return jsonify({"status": "received"}), 200


@app.route("/")
def home():
    return "Salesforce Webhook Running"
