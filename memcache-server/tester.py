import requests
import yaml
import json
todo = """{"key": "pdam=sas", "value": "omg"}"""
key_value = json.loads(todo)
if 'key' and 'value' in key_value:
    print('yay')

# with open('../configuration.yaml', "r") as f:
#     config = yaml.safe_load(f)

# api_url = "http://" + str(config['KEYVALSTORE']['HOST']) + ":" + str(config['KEYVALSTORE']['PORT']) + "/set-value"
# todo = {"key": "pdam=sas", "value": "omg"}
# response = requests.put(api_url, json=todo)
# response.json()

# def test_create_item():
#     response = client.post(
#         "/get-value",
#         json={"key": "pdam"},
#     )
#     print(response)