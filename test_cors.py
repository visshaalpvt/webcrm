import requests
res = requests.get('https://webcrm-r7lk.onrender.com/api/jobs', headers={'Origin': 'https://webcrm-fawn.vercel.app'})
print("Status:", res.status_code)
for k, v in res.headers.items():
    print(f"{k}: {v}")
