import sys
import os
import re
import glob
import configparser

from elasticsearch import Elasticsearch

from ceda_elasticsearch_tools.elasticsearch import ceda_elasticsearch_client
import urllib.request

ICconfig = configparser.ConfigParser()
ICconfig.read("test.conf")

ES_APIKEY = ICconfig.get('elastic_search', 'APIKEY')
ES = ceda_elasticsearch_client.CEDAElasticsearchClient(headers={'x-api-key': ES_APIKEY})

def make_ES():
    # return Elasticsearch([ES_URL], http_auth=(ES_USER, ES_PASSWORD))
    return ceda_elasticsearch_client.CEDAElasticsearchClient(headers={'x-api-key': ES_APIKEY})


#2012-11-16 15:39:59:/badc/cru/data/cru_ts_3.2/station/tmp/cru_ts3.20.1991.2000.tmp.st0.gz:DEPOSIT:2107143: (force=None) /datacentre/processing/cru/cru/station/tmp/cru_ts3.20.1991.2000.tmp.st0.gz
# 2021-06-05 02:56:03:/badc/eprofile/data/germany/lindenberg/dwd-jenoptick-chm15k-nimbus_0/2021/06/02/.tmpdeposit_20210605.025603_aqJBR_L2_0-20000-0-10393_0202106021810.nc:TMP_DEPOSIT:80580::{'src': '/datacentre/arrivals/users/dartmetoffice/eprofile/block-10/L2_0-20000-0-10393_0202106021810.nc', 'checksum': None, 'process': 'deposit_client'}

re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:')
(?P<date>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}):(?P<path>.*):(?P<operation>[A-Z_]+):(?P<size>\d+):(?P<extra_text>[\w\d\(\):\.\s]*?):?(?P<extra_json>\{.*\})?
       
for depositlog in sys.argv[1:]:

    with open(depositlog, encoding='utf-8') as f:
        for line in f:
            bits = line.split()
            date = ':'.join(bits[0:3])
            path = bits[3]
            operation = bits[4]
            size = bits[5]
            process = bits[6]
            extra = json.loads(':'.join(bits[7:-1]))
            
            if line.startswith("# DIR "):
                dir_name = line.strip()[6:] 
                dir_name = logical_path(dir_name, loc, logical_spot_path)
                event.update({"item_type": "DIR", "logical_path": dir_name})

            elif line.startswith("# LINK "):
                m = re.search("# LINK (.*) -> (.*)", line)
                if m:
                    link_name = logical_path(m.group(1), loc, logical_spot_path)
                    target = m.group(2)
                    event.update({"item_type": "LINK", "logical_path": link_name, "link_target": target})
            elif line.startswith("#"):
                continue
            else:
                name, alg, checksum, size, modtime = line.strip().split('|')
                event.update({"logical_path": logical_path(name, loc, logical_spot_path),
                              "checksum": alg+":"+checksum, "size": size, "modtime": modtime,
                              "item_type": "FILE"})
            print(event)
            ES.index(index="file-events", body=event)





