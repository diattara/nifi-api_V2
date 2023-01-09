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

