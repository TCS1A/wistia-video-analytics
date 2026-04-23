import requests, os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('WISTIA_API_TOKEN')
MEDIA_ID = os.getenv('MEDIA_ID_2')

url = f'https://api.wistia.com/v1/stats/medias/{MEDIA_ID}.json'
headers = {'Authorization': f'Bearer {API_TOKEN}'}

response = requests.get(url, headers=headers)
print(f'Status: {response.status_code}')

if response.status_code == 200:
    print('SUCCESS! Data:', response.json())
else:
    print('ERROR:', response.text)