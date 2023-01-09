import sqlite3
import requests
import json
import sys
import pathlib
import xml.etree.ElementTree as ET
import argparse
import os
from jinja2 import Template
from dotenv import load_dotenv
load_dotenv(".env")



KAFKA_BROKER_PORT=os.environ['KAFKA_BROKER_PORT']
JSON_RECORD_READER=os.environ['JSON_RECORD_READER']
JSON_RECORD_WRITER=os.environ['JSON_RECORD_WRITER']
ELASTIC_URL_PORT=os.environ['ELASTIC_URL_PORT']
ELASTIC_PASSWORD=os.environ['ELASTIC_PASSWORD']
ELASTIC_USERNAME=os.environ['ELASTIC_USERNAME']
DLQ_KAFKA_TOPIC=os.environ['DLQ_KAFKA_TOPIC']
#--------------------------------------------------
hostname = os.environ['hostname']
port = os.environ['port']
template_dir = "/opt/temp"
remove_after_create = "/opt/remove_file"
username = os.environ['username']
password = os.environ['password']
cert_file=False

host_url = "https://" + hostname + ":" + port + "/nifi-api"
#print("Host ip is {0} port is {1} and the host URL is {2}".format(hostname, port, host_url) )
#name=name()
def get_root_resource_id():
    # URL to get root process group information

    conn=sqlite3.connect('db.sqlite')
    cursor = conn.cursor()
    cursor.execute("""SELECT name_hospital FROM db_pipline order by id desc""")
    user1 = cursor.fetchall()
    name1=user1[0][0]
    cursor.execute("""SELECT name_departement FROM db_pipline order by id desc""")
    user2 = cursor.fetchall()
    name2=user2[0][0]
    id=getiddepByName(name1,name2)
    resource_url = host_url + "/flow/process-groups/"+str(id)

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})

    if not response.status_code == 200:
        print(response)
        print(response.content)
    json = response.json()
    resource_id = json["processGroupFlow"]["id"]
    print(resource_id)
    return resource_id


def getiddepByName(h_name,dep_name):
    # URL to get root process group information
    
    id=get_id_hospital_by_name(h_name)
    resource_url = host_url + "/flow/process-groups/"+str(id)

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})


    json = response.json()
    id_pg=[pg["component"]["id"] for pg in json["processGroupFlow"]["flow"]["processGroups"] if pg["component"]["name"].lower()==dep_name.lower()]
    
    if len(id_pg)>0:
        return id_pg[0]
    else:
        return ""

# print out the name of the template
def get_template_name(file):
    #tree = ET.parse(file)
    #root = tree.getroot()
    return 'TEMPLATE_PIPLINE'


# upload the template from local to remote nifi cluster
def upload_template(template_file_name):
    conn=sqlite3.connect('db.sqlite')
    cursor = conn.cursor()
    cursor.execute("""SELECT name_pipline FROM db_pipline order by id desc""")
    user1 = cursor.fetchall()
    pipeliname=user1[0][0]
    cursor.execute("""SELECT name_departement FROM db_pipline order by id desc""")
    user2 = cursor.fetchall()
    deptname=user2[0][0]
    cursor.execute("""SELECT name_hospital FROM db_pipline order by id desc""")
    user3 = cursor.fetchall()
    hospitalname=user3[0][0]

    upload_url = host_url + "/process-groups/" + get_root_resource_id() + "/templates/upload"
    print (upload_url)
    file_string = open(template_dir+ "/" + template_file_name, 'r',encoding = "ISO-8859-1").read().replace('TEMPLATE_PIPELINE__HOSPITALE',str(pipeliname))
    file_string=file_string.replace('KAFKA_TOPIC_NAME', str(hospitalname+'_'+deptname+'_'+pipeliname))
    file_string=file_string.replace('ELASTIC_URL_PORT', ELASTIC_URL_PORT)
    file_string=file_string.replace('ELASTIC_PASSWORD', ELASTIC_PASSWORD)
    file_string=file_string.replace('ELASTIC_USERNAME', ELASTIC_USERNAME)
    file_string=file_string.replace('JSON_RECORD_READER', JSON_RECORD_READER)
    file_string=file_string.replace('JSON_RECORD_WRITER', JSON_RECORD_WRITER)
    file_string=file_string.replace('KAFKA_BROKER_PORT', KAFKA_BROKER_PORT)
    file_string=file_string.replace('KAFKA_Group_ID_NAME', str('elastic_'+hospitalname+'_'+deptname+'_'+pipeliname))
    file_string=file_string.replace('ELASTIC_INDEX_NAME', str(hospitalname+'_'+deptname+'_'+pipeliname))
    file_string=file_string.replace('DLQ_KAFKA_TOPIC', DLQ_KAFKA_TOPIC)



    multipart_form_data = {
      'template': file_string,
    }
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.post(upload_url, files=multipart_form_data, headers= auth_header, verify=cert_file, proxies={'https': ''})
    print (response)


# create an instance using the template id
def instantiate_template(template_file_name, originX, originY):
    create_instance_url = host_url + "/process-groups/" + get_root_resource_id() + "/template-instance"
    payload = {"templateId": get_template_id(template_file_name), "originX": originX, "originY": originY}
    originX = originX + 600
    originY = originY - 50
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.post(create_instance_url, json=payload, headers= auth_header, verify=cert_file, proxies={'https': ''})
    handle_error(create_instance_url, response)


# get list of templates that used for searching template id
def get_templates():
    get_template_instance_url = host_url + "/flow/templates"
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(get_template_instance_url,  headers= auth_header, verify=cert_file, proxies={'https': ''})
    handle_error(get_template_instance_url, response)
    json = response.json()
    templates = json["templates"]
    return templates


# get the id of the template that matches the name of the saved template
def get_template_id(template_file_name):
    templates = get_templates()
    template_id = ""
    for template in templates:
        print(template)
        print(template_file_name)
        if get_template_name(template_dir + "/" + template_file_name) == template["template"]["name"]:
            print ("Creating instance of " + template["template"]["name"] + " ...")
    template_id = template["template"]["id"]
    return template_id


# removes a template from nifi cluster by its id.
def remove_template(template_id):
    if template_id != "":
        delete_template_url = host_url + "/templates/" + template_id
        auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
        response = requests.delete(delete_template_url, headers= auth_header, verify=cert_file, proxies={'https': ''})
        handle_error(delete_template_url, response)
    else:
        raise SystemError("Can not remove template without a template id")


# check current user session if any
def check_current_user():
    current_user_url = host_url + "/flow/current-user"
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    print(current_user_url)
    res = requests.get(current_user_url, headers=auth_header, verify=cert_file, proxies={'https': ''})
    handle_error(current_user_url, res)


# get authentication token(JWT token) using username and password
def get_auth_token() -> str:
    auth_token_url = host_url + "/access/token"
    res = requests.post(auth_token_url, data={'username': username, 'password': password}, verify=cert_file, proxies={'https': ''})
    handle_error(auth_token_url, res)
    return res.text


# Check and raise exception for a given
def handle_error(endpoint, res):
    if not res.status_code == 200 and not res.status_code == 201:
        raise SystemError("Expect {0} call return either 200 or 401 but got status code {1} with response {2}".format(endpoint, res.status_code, res.text))


# deploys a template to nifi for a specified location
def deploy_template(template_file, origin_x, origin_y):
    remove_template(get_template_id(template_file.name))
    upload_template(template_file.name)
    instantiate_template(template_file.name, origin_x, origin_y)
    if remove_after_create == "true":
       remove_template(get_template_id(template_file.name))





def get_id_hospital_by_name(hospital_name):
    # URL to get root process group information
    resource_url = host_url + "/flow/process-groups/root"

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})


    json = response.json()
    id_pg=[pg["component"]["id"] for pg in json["processGroupFlow"]["flow"]["processGroups"] if pg["component"]["name"].lower()==hospital_name.lower()]

    if len(id_pg)>0:
        return id_pg[0]
    else:
        return ""

def getiddepByName(h_name,dep_name):
    # URL to get root process group information
    
    id=get_id_hospital_by_name(h_name)
    resource_url = host_url + "/flow/process-groups/"+str(id)

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})


    json = response.json()
    id_pg=[pg["component"]["id"] for pg in json["processGroupFlow"]["flow"]["processGroups"] if pg["component"]["name"].lower()==dep_name.lower()]
    
    if len(id_pg)>0:
        return id_pg[0]
    else:
        return ""


def getInfo_pipline(id_hospital,name_pipline):
    # URL to get root process group information
    resource_url = host_url + "/flow/process-groups/"+id_hospital

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})


    json = response.json()
    id_pg=[{"id":pg["component"]["id"],"revision":pg["revision"]} for pg in json["processGroupFlow"]["flow"]["processGroups"] if pg["component"]["name"].lower()==name_pipline.lower()]
    
    if len(id_pg)>0:
        return id_pg[0]
    else:
        return ""



def deleteDep(name_hopital,name_dep,name_p):
    
    id_hopital = getiddepByName(name_hopital,name_dep)
    inf_processor= getInfo_pipline(id_hopital,name_p)
    # URL to get root process group information
    resource_url = host_url + "/process-groups/"+inf_processor["id"]+"?clientId="+inf_processor["revision"]["clientId"]+"&version="+str(inf_processor["revision"]["version"])
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.delete(resource_url, headers=auth_header, verify=False, proxies={'https': ''})
    handle_error(resource_url, response)
    json = response.json()

    return json


# main function starts here
def main():

    # start up position
    origin_x = 661
    origin_y = -45

    # Make sure current user login is okay
    check_current_user()

    for template_file in pathlib.Path(template_dir).iterdir():
        if template_file.is_file():
            deploy_template(template_file, origin_x, origin_y)


def getlist_processpipline_indept(data):
    
    
    liste1=[]
    # URL to get root process group information
    id=getiddepByName(data['name_hospital'],data['name_departement'])
    resource_url = host_url + "/flow/process-groups/"+str(id)+"/status?recursive=false"

    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    response = requests.get(resource_url, headers=auth_header, verify=False, proxies={'https': ''})
    json = response.json()

    
    cp=json['processGroupStatus']['aggregateSnapshot']['processGroupStatusSnapshots']
    for i in range(len(cp)):
       
        test=json['processGroupStatus']['aggregateSnapshot']['processGroupStatusSnapshots'][i]
        id= test['processGroupStatusSnapshot']['id']
        name=test['processGroupStatusSnapshot']['name']
        liste1.append(name)
        
    return liste1
# running or stopped process group
def run_pipeline(name_hopital,name_dep,name_pipeline):
    id_hopital = getiddepByName(name_hopital,name_dep)
    info_processor= getInfo_pipline(id_hopital,name_pipeline)
    id = info_processor["id"]
    
    url = host_url + "/flow/process-groups/"+id
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    payload = {"id":id,"state":"RUNNING"}
    res = requests.put(url, headers=auth_header, verify=False, json=payload , proxies={'https': ''})
    handle_error(url,res)

def stop_pipeline(name_hopital,name_dep,name_pipeline):
    id_hopital = getiddepByName(name_hopital,name_dep)
    info_processor= getInfo_pipline(id_hopital,name_pipeline)
    id = info_processor["id"]
    url = host_url + "/flow/process-groups/"+id
    auth_header = {'Authorization': 'Bearer ' + get_auth_token()}
    payload = {"id":id,"state":"STOPPED"}
    res = requests.put(url, headers=auth_header, verify=False,json=payload, proxies={'https': ''})
    handle_error(url,res)


    
