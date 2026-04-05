import requests
res = requests.options('https://webcrm-r7lk.onrender.com/api/upload', headers={
    'Origin': 'https://webcrm-fawn.vercel.app',
    'Access-Control-Request-Method': 'POST',
    'Access-Control-Request-Headers': 'content-type'
})
with open('cors2.txt', 'w', encoding='utf-8') as f:
    f.write(f"Status: {res.status_code}\n")
    for k, v in res.headers.items():
        f.write(f"{k}: {v}\n")
