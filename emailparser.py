


import csv
import json
import requests
import pandas as pd
api_key_aeroloads = "x"
api_key_hunter='x'
api_key_rocket='x'
csv_file='contacts.csv'


def get_emails_from_aeroleads(csv_file,api_key_file):
    print("hitting aeroleads  api")

    api_key_aeroloads=api_key_file
    url_aeroloads = "https://aeroleads.com/apis/details"
    data = {'name': [], 'company_url': [], 'email': [], 'status': []}
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            first_name = row['first_name']
            last_name = row['last_name']
            company_domain = row['company_domain']
            payload = {
                'api_key': api_key_aeroloads,
                'first_name': first_name,
                'last_name': last_name,
                'company_url': company_domain
            }
            
            response = requests.get(url_aeroloads, params=payload)
            if response.status_code == 200:
                data['name'].append(json.loads(response.text)['name'])
                data['company_url'].append(json.loads(response.text)['company_url'])
                data['email'].append(json.loads(response.text)['email'])
                data['status'].append(json.loads(response.text)['status'])
            else:
                print("Error:", response.status_code)
    pd.DataFrame(data).to_csv('Aeroloads_contacts_with_emails.csv',index=False)

    print("Write complete using aeroleads api")

            
#hunter
def get_emails_from_hunter(csv_file, api_key_file):
    print("hitting hunter  api")

    data = {'name': [], 'company_url': [], 'email': [], 'source': [],'phone_number':[],'twitter':[],'linkedin_url':[],'position':[]}
    
    api_keys=api_key_file
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            first_name = row['first_name']
            last_name = row['last_name']
            company_domain = row['company_domain']
            
            # call hunter api
            hunter_url = "https://api.hunter.io/v2/email-finder"
            hunter_params = {
                'domain': company_domain,
                'first_name': first_name,
                'last_name': last_name,
                'api_key': api_keys
            }
            hunter_response = requests.get(hunter_url, params=hunter_params)
            # try:
            if hunter_response.status_code == 200:
                hunter_data = json.loads(hunter_response.text)
                # print(hunter_data)
                data['name'].append(hunter_data['data']['first_name']+hunter_data['data']['last_name'])
                data['company_url'].append(hunter_data['data']['company'])
                data['email'].append(hunter_data['data']['email'])
                data['phone_number'].append(hunter_data['data']['phone_number'])
                data['twitter'].append(hunter_data['data']['twitter'])
                data['linkedin_url'].append(hunter_data['data']['linkedin_url'])
                data['source'].append('Hunter')
                data['position'].append(hunter_data['data']['position'])
            else:
                print("Error:", hunter_response.status_code)

    pd.DataFrame(data).to_csv('hunter_contacts_with_emails.csv',index=False)

    print("Write complete using hunter api")

            
def get_emails_from_rocket(csv_file, api_key_file):
    print("hitting rocket  api")

    data_csv = {'name': [], 'company_url': [], 'email': [], 'source': [],'position':[]}
    

    api_key=api_key_file
    # read contacts from csv file
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            first_name = row['first_name']
            last_name = row['last_name']
            company_domain = row['company_domain']
            
            
            url = "http://api.rocketreach.co/api/v2/person/lookup"
            headers = {'Api-Key': api_key}
            params = {'name': f"{first_name} {last_name}", 'current_employer': company_domain.split('.com')[0]}
            response = requests.get(url, headers=headers, params=params)
#             print(response.text)
            if response.status_code == 200:
                data=response.json()
                emails=response.json()['emails']
                emails=[x['email'] for x in emails]
                emails=','.join(emails)
                data_csv['name'].append(data['name'])
                data_csv['company_url'].append(data['current_employer_domain'])
                data_csv['email'].append(emails)
                data_csv['source'].append('Rocket')
                data_csv['position'].append(data['normalized_title'])

            else:
                print(f"Error: {response.status_code}")
    try:
        pd.DataFrame(data).to_csv('rocket_contacts_with_emails.csv',index=False)
    except:
        print('no data found')
    print("Write complete using rocket api")


if __name__=='__main__':
    # get_emails_from_aeroleads(csv_file,api_key_aeroloads)
    # get_emails_from_hunter(csv_file, api_key_hunter)
    get_emails_from_rocket(csv_file, api_key_rocket)