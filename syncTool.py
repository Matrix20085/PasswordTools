import requests
import json
import os

# === CONFIGURATION ===
HASHTOPOLIS_URL = "http://<HASHTOPOLIS_SERVER_IP>/api/server.php"
HASHTOPOLIS_API_KEY = "<YOUR_HASHTOPOLIS_API_KEY>"
SUPERTASK_ID = <YOUR_SUPERTASK_ID>

HASHES_API_KEY = "<YOUR_HASHES_COM_API_KEY>"
HASHES_API_URL = "https://hashes.com/api/

# === HELPERS ===
def hashtopolis_request(data):
    data['accessKey'] = HASHTOPOLIS_API_KEY
    response = requests.post(HASHTOPOLIS_URL, json=data)
    return response.json()

def hashes_request(endpoint, payload):
    headers = {"Authorization": f"Bearer {HASHES_API_KEY}"}
    response = requests.post(HASHES_API_URL + endpoint, json=payload, headers=headers)
    return response.json()

# === STEP 1: Download new hash lists from hashes.com ===
def get_new_hashes():
    response = hashes_request("hashes/list", {"types": ["MD5", "NTLM", "SHA1"]})
    hashes = []
    for item in response.get("data", []):
        if not item.get("cracked"):
            hashes.append(item["hash"])
    return hashes

# === STEP 2: Upload new hashes to Hashtopolis ===
def upload_to_hashtopolis(hashes):
    if not hashes:
        return None
    content = "\n".join(hashes)
    with open("upload_hashes.txt", "w") as f:
        f.write(content)
    with open("upload_hashes.txt", "rb") as f:
        response = hashtopolis_request({
            "section": "hashlist",
            "request": "importHashlist",
            "name": "Hashes from hashes.com",
            "format": 0,
            "hashtypeId": 0,
            "separator": "\n",
            "hashlist": f.read().decode()
        })
    os.remove("upload_hashes.txt")
    return response.get("hashlistId")

# === STEP 3: Create a task if hashes were uploaded ===
def create_task(hashlist_id):
    if not hashlist_id:
        return
    response = hashtopolis_request({
        "section": "task",
        "request": "createTask",
        "hashlistId": hashlist_id,
        "priority": 1,
        "chunkTime": 600,
        "bench": False,
        "statusTimer": 30,
        "attackCmd": "-a 0 -w 3 -O wordlists/rockyou.txt",
        "taskName": "Auto Task from hashes.com",
        "superTaskId": SUPERTASK_ID
    })
    return response

# === STEP 4: Download cracked hashes from Hashtopolis ===
def get_cracked():
    response = hashtopolis_request({
        "section": "hashlist",
        "request": "getCracked"})
    cracked = response.get("crackedHashes", [])
    return {item["hash"]: item["plain"] for item in cracked}

# === STEP 5: Upload new cracked hashes to hashes.com ===
def upload_cracked_to_hashes_com(cracked_hashes):
    payload = [{"hash": h, "plain": p} for h, p in cracked_hashes.items()]
    response = hashes_request("hashes/submit", {"hashes": payload})
    return response

# === MAIN WORKFLOW ===
def main():
    print("Getting new hashes from hashes.com...")
    new_hashes = get_new_hashes()
    print(f"Found {len(new_hashes)} new hashes")

    print("Uploading to Hashtopolis...")
    hashlist_id = upload_to_hashtopolis(new_hashes)

    if hashlist_id:
        print(f"Creating task for hashlist ID {hashlist_id}...")
        create_task(hashlist_id)

    print("Checking for cracked hashes in Hashtopolis...")
    cracked = get_cracked()
    if cracked:
        print(f"Uploading {len(cracked)} cracked hashes to hashes.com...")
        upload_cracked_to_hashes_com(cracked)
    else:
        print("No cracked hashes found.")

if __name__ == "__main__":
    main()
