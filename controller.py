import datetime, json, logging, os, pprint, sys
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(
    # filename=os.environ['DC__LOG_PATH'],
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S'
    )
log = logging.getLogger(__name__)
log.debug( '\n-------\nstarting standard log' )

if (sys.version_info < (3, 0)):
    raise Exception( 'forcing myself to use python3 always' )

API_ROOT_URL = os.environ['DC__ROOT_URL']
HTTPBASIC_KEY = os.environ['DC__HTTPBASIC_USERNAME']
HTTPBASIC_SECRET = os.environ['DC__HTTPBASIC_PASSWORD']
FILE_DOWNLOAD_DIR = os.environ['DC__FILE_DOWNLOAD_DIR']

## ok, let's get to work! ##

## get the token; looks like it's good for an hour

log.debug( '\n-------\ngetting token' )

token_url = '%stoken' % API_ROOT_URL
log.debug( 'token_url, ```%s```' % token_url )
r = requests.post( token_url, auth=HTTPBasicAuth(HTTPBASIC_KEY, HTTPBASIC_SECRET) )
log.debug( 'token r.content, ```%s```' % r.content )
token = r.json()['access_token']
log.debug( 'token, ```%s```' % token )

# ===================================
# make a bib-request, just as a test
# ===================================

log.debug( '\n-------\ngetting bib info' )

bib_url = '%sbibs/' % API_ROOT_URL
payload = { 'id': '1000001' }
log.debug( 'token_url, ```%s```' % token_url )
custom_headers = {'Authorization': 'Bearer %s' % token }
r = requests.get( bib_url, headers=custom_headers, params=payload )
log.debug( 'bib r.content, ```%s```' % r.content )

# ===================================
# try a json-post
# ===================================

log.debug( '\n-------\trying json post' )

query_json = '''[
    [
      {
        "target": {
          "record": {
            "type": "bib"
          },
          "field": {
            "marcTag": "090",
            "subfields": "a"
          }
        },
        "expr": {
          "op": "regex",
          "operands": [
            "h[bcdefgj].*",
            ""
          ]
        }
      },
      "or",
      {
        "target": {
          "record": {
            "type": "item"
          },
          "field": {
            "marcTag": "090",
            "subfields": "a"
          }
        },
        "expr": {
          "op": "regex",
          "operands": [
            "h[bcdefgj].*",
            ""
          ]
        }
      }
    ],
    "and",
    {
      "target": {
        "record": {
          "type": "bib"
        },
        "id": 31
      },
      "expr": {
        "op": "not_equal",
        "operands": [
          "n",
          ""
        ]
      }
    }
  ]'''

# offset = 0  #
# offset = 100000 # got 100,000
# offset = 200000 # got 100,000
# offset = 1000000 # got 100,000
# offset = 2000000 # got 100,000
# offset = 3000000 # got 100,000
# offset = 4000000 # got 100,000
# offset = 5000000 # got 100,000
# offset = 10000000 # got 0
offset = 0 # got 0
limit = 10000000
items_query_url = f'{API_ROOT_URL}items/query?offset={offset}&limit={limit}'
payload = { 'json': query_json }

custom_headers = {'Authorization': 'Bearer %s' % token }
r = requests.post( items_query_url, headers=custom_headers, json=payload )
log.debug( f'url was, ```{r.url}```' )
log.debug( f'items response, ```{pprint.pformat( r.json() )}```' )


