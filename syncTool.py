import requests
import base64
import json
import os
from pathlib import Path

# === CONFIGURATION ===
HASHTOPOLIS_URL = "http://<HASHTOPOLIS_SERVER_IP>/api/server.php"
HASHTOPOLIS_API_KEY = "<YOUR_HASHTOPOLIS_API_KEY>"
SUPERTASK_ID = "<YOUR_SUPERTASK_ID>"

HASHES_COM_API_KEY = "<YOUR_HASHES_COM_API_KEY>"
HASHES_COM_JOBS_URL = f"https://hashes.com/en/api/jobs?key={HASHES_COM_API_KEY}"
HASHES_COM_UPLOAD_URL = "https://hashes.com/en/api/founds"
HASHES_COM_BASE = "https://hashes.com"

HASH_TRACK_FILE = "hashlist_crack_status.json"
CHUNK_SIZE = 800000

# === HELPERS ===
def hashtopolis_request(data):
    data['accessKey'] = HASHTOPOLIS_API_KEY
    response = requests.post(HASHTOPOLIS_URL, json=data)
    return response.json()

def get_hashes_com_jobs():
    response = requests.get(HASHES_COM_JOBS_URL)
    return response.json().get("list", [])

def download_unfound_list(rel_path):
    url = HASHES_COM_BASE + rel_path
    resp = requests.get(url)
    return resp.text.splitlines()

def create_hashlist(name, hashtypeId, hashlist):
    data_encoded = base64.b64encode("\n".join(hashlist).encode()).decode()
    response = hashtopolis_request({
        "section": "hashlist",
        "request": "createHashlist",
        "name": name,
        "isSalted": False,
        "isSecret": False,
        "isHexSalt": False,
        "separator": ":",
        "format": 0,
        "hashtypeId": hashtypeId,
        "accessGroupId": 1,
        "data": data_encoded,
        "useBrain": False,
        "brainFeatures": 0
    })
    return response.get("hashlistId")

def create_task(hashlist_id):
    return hashtopolis_request({
        "section": "task",
        "request": "createTask",
        "hashlistId": hashlist_id,
        "priority": 1,
        "chunkTime": 600,
        "bench": False,
        "statusTimer": 30,
        "attackCmd": "-a 0 -w 3 -O wordlists/rockyou.txt",
        "taskName": "Auto Task",
        "superTaskId": SUPERTASK_ID
    })

def list_hashlists():
    result = hashtopolis_request({"section": "hashlist", "request": "listHashlists"})
    return result.get("hashlists", [])

def get_cracked(hashlist_id):
    result = hashtopolis_request({
        "section": "hashlist",
        "request": "getCracked",
        "hashlistId": hashlist_id
    })
    return result.get("cracked", [])

def save_tracker(data):
    with open(HASH_TRACK_FILE, 'w') as f:
        json.dump(data, f)

def load_tracker():
    if Path(HASH_TRACK_FILE).exists():
        with open(HASH_TRACK_FILE, 'r') as f:
            return json.load(f)
    return {}

def upload_cracked_to_hashes_com(hashtype_id, cracked):
    lines = [f"{item['hash']}:{item['plain']}" for item in cracked]
    algo_map = {0: 0, 1000: 1000, 100: 2811}  # MD5, NTLM, SHA1
    algo = algo_map.get(hashtype_id, None)
    if algo is None:
        return

    batch = []
    current_size = 0
    file_index = 0
    for line in lines:
        line_size = len(line.encode()) + 1  # newline
        if current_size + line_size > CHUNK_SIZE:
            write_and_upload_batch(batch, algo, file_index)
            batch = []
            current_size = 0
            file_index += 1
        batch.append(line)
        current_size += line_size

    if batch:
        write_and_upload_batch(batch, algo, file_index)

def write_and_upload_batch(lines, algo, index):
    filename = f"founds_{algo}_{index}.txt"
    with open(filename, "w") as f:
        f.write("\n".join(lines))
    with open(filename, "rb") as f:
        response = requests.post(HASHES_COM_UPLOAD_URL, files={
            "userfile": f
        }, data={
            "key": HASHES_COM_API_KEY,
            "algo": algo
        })
    os.remove(filename)

# === MAIN WORKFLOW ===
def main():
    print("Downloading unfound hash lists from hashes.com...")
    jobs = get_hashes_com_jobs()
    hash_groups = {0: [], 1000: [], 100: []}  # MD5, NTLM, SHA1
    algo_names = {0: "MD5", 1000: "NTLM", 100: "SHA1"}

    for job in jobs:
        algo_id = job["algorithmId"]
        if algo_id not in hash_groups:
            continue
        unfound_path = job["leftList"].replace("\\", "")
        hashes = download_unfound_list(unfound_path)
        hash_groups[algo_id].extend(hashes)

    for algo_id, hashlist in hash_groups.items():
        if not hashlist:
            continue
        print(f"Uploading {len(hashlist)} {algo_names[algo_id]} hashes to Hashtopolis...")
        hashlist_id = create_hashlist(f"Auto-{algo_names[algo_id]}", algo_id, hashlist)
        if hashlist_id:
            create_task(hashlist_id)

    print("Checking for new cracked hashes to upload back to hashes.com...")
    tracker = load_tracker()
    hashlists = list_hashlists()
    for hl in hashlists:
        hlid = str(hl["hashlistId"])
        cracked = get_cracked(hlid)
        prev_crack_count = tracker.get(hlid, 0)
        if len(cracked) > prev_crack_count:
            print(f"Uploading {len(cracked) - prev_crack_count} new cracks for hashlist {hlid}")
            upload_cracked_to_hashes_com(hl["hashtypeId"], cracked[prev_crack_count:])
            tracker[hlid] = len(cracked)
    save_tracker(tracker)

if __name__ == "__main__":
    main()
