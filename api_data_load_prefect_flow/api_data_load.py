import pandas as pd
import json
import requests
import urllib.parse
import os

url = 'https://pokeapi.co/api/v2/pokemon/ditto'

response = requests.get(url)

print(os.getcwd())

if response.status_code == 200:
    data = response.json()
    
    with open(os.path.join(os.getcwd(), 'output.json'), 'w') as json_file:
        json.dump(data, json_file, indent=4)

else:
    print(f"Error fetching data. Status code: {response.status_code}")

