import sys
import os
import re
import glob
import configparser

from elasticsearch import Elasticsearch

from ceda_elasticsearch_tools.elasticsearch import ceda_elasticsearch_client
import urllib.request

spot_table = {}
f = urllib.request.urlopen('https://cedaarchiveapp.ceda.ac.uk/fileset/download_conf/')
content = f.read().decode('utf-8').splitlines()
for line in content:
    line = line.strip()
    if line == "": continue
    spot, logical_path = line.split()
    spot_table[spot] = logical_path

ICconfig = configparser.ConfigParser()
ICconfig.read("test.conf")

ES_APIKEY = ICconfig.get('elastic_search', 'APIKEY')
ES = ceda_elasticsearch_client.CEDAElasticsearchClient(headers={'x-api-key': ES_APIKEY})

def make_ES():
    # return Elasticsearch([ES_URL], http_auth=(ES_USER, ES_PASSWORD))
    return ceda_elasticsearch_client.CEDAElasticsearchClient(headers={'x-api-key': ES_APIKEY})

def logical_path(path, loc, logical_spot_path):
    print(path, loc, logical_spot_path)
    if path.startswith(loc):
        print("x")
        return path.replace(loc, logical_spot_path)
    elif not path.startswith("/"):
        return os.path.join(logical_spot_path, path)
    else:
        return path
       

def extract_audit_header_info(checkmfile):
    spot = os.path.basename(checkmfile)
    spot = spot.split('.')[1]
    print(spot)
    logical_spot_path = spot_table[spot] 

    # extract audit header info
    with open(checkmfile, encoding='utf-8') as f:
        n = 0
        for line in f:
           m = re.search('# scaning path\s*([\w\d/-]+)', line)
           if m:
               loc = m.group(1) 
               print(loc)
           m = re.search('# audit (id|ID):\s*(\d+)', line)
           if m:
               auditid = int(m.group(2)) 
               print(auditid)
           m = re.search('# generated\s*([\d-]+ [\d:.]+)', line)
           if m:
               audit_gen_time = m.group(1) 
               print(audit_gen_time)
           n += 1
           if n > 100: break
    return {"spot": spot, "auditid": auditid, "event_time": audit_gen_time,             
            "loc": loc, "event_type": "audit", "spot_path": logical_spot_path}

for checkmfile in sys.argv[1:]:

    audit_info = extract_audit_header_info(checkmfile)    
    loc = audit_info['loc']
    spot_path = audit_info['spot_path'] 

    with open(checkmfile, encoding='utf-8') as f:
        for line in f:
            event = audit_info.copy()
            if line.startswith("# DIR "):
                dir_name = line.strip()[6:] 
                directory, name = os.path.split(logical_path(dir_name, loc, spot_path))
                event.update({"item_type": "DIR", "directory": directory, "name": name})

            elif line.startswith("# LINK "):
                m = re.search("# LINK (.*) -> (.*)", line)
                if m:
                    directory, name = os.path.split(logical_path(m.group(1), loc, spot_path))
                    target = m.group(2)
                    event.update({"item_type": "LINK", "directory": directory, "name": name, "link_target": target})
            elif line.startswith("#"):
                continue
            else:
                name, alg, checksum, size, modtime = line.strip().split('|')
                directory, name = os.path.split(logical_path(name, loc, spot_path))
                event.update({"directory": directory, "name": name,
                              "checksum": alg+":"+checksum, "size": size, "modtime": modtime,
                              "item_type": "FILE"})
            print(event)
            ES.index(index="file-events", body=event)





