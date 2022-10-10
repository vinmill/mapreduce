import requests
import yaml
import json
import os
# print(os.listdir('../raw-data'))
# todo = """{"key": "pdam=sas", "value": "omg"}"""
# key_value = json.loads(todo)
# if 'key' and 'value' in key_value:
#     print('yay')

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

api_url = "http://" + str(config['KEYVALSTORE']['HOST']) + ":" + str(config['KEYVALSTORE']['PORT']) + "/get-value"
todo = {"key": "pdam=sas", 
"value": str("""The Project Gutenberg eBook of The Adventures of Sherlock Holmes, by Arthur Conan Doyle


International donations are gratefully accepted, but we cannot make
any statements concerning tax treatment of donations received from
outside the United States. U.S. laws alone swamp our small staff.

Please check the Project Gutenberg web pages for current donation
methods and addresses. Donations are accepted in a number of other
ways including checks, online payments and credit card donations. To
donate, please visit: www.gutenberg.org/donate

Section 5. General Information About Project Gutenberg-tm electronic works

Professor Michael S. Hart was the originator of the Project
Gutenberg-tm concept of a library of electronic works that could be
freely shared with anyone. For forty years, he produced and
distributed Project Gutenberg-tm eBooks with only a loose network of
volunteer support.

Project Gutenberg-tm eBooks are often created from several printed
editions, all of which are confirmed as not protected by copyright in
the U.S. unless a copyright notice is included. Thus, we do not
necessarily keep eBooks in compliance with any particular paper
edition.

Most people start at our website which has the main PG search
facility: www.gutenberg.org

This website includes information about Project Gutenberg-tm,
including how to make donations to the Project Gutenberg Literary
Archive Foundation, how to help produce our new eBooks, and how to
subscribe to our email newsletter to hear about new eBooks.
""".encode())}
response = requests.get(api_url, json=todo)
response.json()

# def test_create_item():
#     response = client.post(
#         "/get-value",
#         json={"key": "pdam"},
#     )
#     print(response)

