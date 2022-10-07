

import requests
import yaml

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

api_url = "http://" + str(config['KEYVALSTORE']['HOST']) + ":" + str(config['KEYVALSTORE']['PORT']) + "/set-value"
todo = {"key": "pdam=sas", "value": "omg"}
response = requests.get(api_url, json=todo)
response.json()

# def test_create_item():
#     response = client.post(
#         "/get-value",
#         json={"key": "pdam"},
#     )
#     print(response)