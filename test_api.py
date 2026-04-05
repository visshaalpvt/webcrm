import requests
import time
import json

base_url = "http://localhost:5000/api"

print("Uploading file...")
with open("sample_colleges.csv", "rb") as f:
    res = requests.post(f"{base_url}/upload", files={"file": f})

print("Upload response status:", res.status_code)
data = res.json()
print("Upload data:", json.dumps(data, indent=2))

job_id = data.get("job_id")
if not job_id:
    print("Failed to get job_id")
    exit(1)

print(f"\nStarting job {job_id}...")
res2 = requests.post(f"{base_url}/start/{job_id}")
print("Start response:", res2.status_code)
print("Start data:", res2.json())

print("\nMonitoring processing...")
last_processed = -1
while True:
    time.sleep(3)
    res3 = requests.get(f"{base_url}/status/{job_id}")
    status_data = res3.json()
    
    current_status = status_data.get('status')
    processed = status_data.get('processed_rows', 0)
    total = status_data.get('total_rows', 0)
    
    if processed != last_processed or current_status in ('completed', 'failed', 'cancelled'):
        print(f"Status: {current_status} | Processed: {processed}/{total} | Active: {status_data.get('active_count')} | Inactive: {status_data.get('inactive_count')} | Not Found: {status_data.get('not_found_count')}")
        last_processed = processed
        
    if current_status in ('completed', 'failed', 'cancelled'):
        break

print("\nFetching final results...")
res4 = requests.get(f"{base_url}/results/{job_id}")
results = res4.json()

print(f"\nExtracted Data for first 3 colleges:")
for c in results.get('colleges', [])[:3]:
    print("-" * 50)
    print(f"College: {c.get('college_name')}")
    print(f"Website: {c.get('found_website') or c.get('original_website') or 'None'}")
    print(f"Phone:   {c.get('extracted_phone')}")
    print(f"Email:   {c.get('extracted_email')}")
    print(f"Status:  {c.get('status')}")

print("\nTest completed successfully! 🎉")
