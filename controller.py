import datetime, json, logging, os, pprint, sys
import requests
from requests.auth import HTTPBasicAuth


## setup

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


def get_token():
    """ Auth-token required for any Sierra API call. """
    log.debug( '\n-------\ngetting token' )
    token_url = '%stoken' % API_ROOT_URL
    log.debug( 'token_url, ```%s```' % token_url )
    r = requests.post( token_url, auth=HTTPBasicAuth(HTTPBASIC_KEY, HTTPBASIC_SECRET) )
    log.debug( 'token r.content, ```%s```' % r.content )
    token = r.json()['access_token']
    log.debug( 'token, ```%s```' % token )
    return token


def save_initial_downloads( jsn_str ):
    """ Saves raw json files from initial json-query calls.
        Called by run_json_query() """
    filename= '%s/%s.json' % ( FILE_DOWNLOAD_DIR, str(datetime.datetime.now()).replace(' ', '_') )
    with open( filename, 'wb' ) as f:
        f.write( jsn_str )
    return


def run_json_query():
    """ Runs the json query and saves all files. """
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
    limit = 100
    continue_flag = True
    custom_headers = {'Authorization': 'Bearer %s' % token }
    while continue_flag is True:
        items_query_url = f'{API_ROOT_URL}items/query?offset={offset}&limit={limit}'
        payload = { 'json': query_json }
        r = requests.post( items_query_url, headers=custom_headers, json=payload )
        log.debug( f'url was, ```{r.url}```' )
        save_file( r.content )
        offset = offset + limit
        if offset > 10000000:
            continue_flag = False
        rsp_dct = r.json()
        if rsp_dct['total'] < 100000:
            continue_flag = False

    ## end def run_json_query()


if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) == 2 else None
    log.debug( f'argument, `{arg}`' )


