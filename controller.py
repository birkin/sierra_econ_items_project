import datetime, json, logging, os, pprint, sys
import asks, requests, trio
from asks import BasicAuth as asksBasicAuth
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


def save_initial_downloads( jsn_str, counter ):
    """ Saves raw json files from initial json-query calls.
        Called by run_json_query() """
    counter_str = f'{counter:03}'
    filename= '%s/a_initial_downloads/%s.json' % ( FILE_DOWNLOAD_DIR, counter_str )
    with open( filename, 'wb' ) as f:
        f.write( jsn_str )
    return


def save_items_dct( jsn_str, counter ):
    """ Saves updated items dct.  TODO: merge with above, passing in directory name
        Called by run_json_query() """
    log.debug( f'counter, `{counter}`')
    counter_str = f'{counter:03}'
    filename= '%s/c_items_dct/%s.json' % ( FILE_DOWNLOAD_DIR, counter_str )
    with open( filename, 'w' ) as f:
        f.write( jsn_str )
    return


def save_items_and_bibs_dct( jsn_str, counter ):
    """ Saves added bibs.  TODO: merge with above, passing in directory name
        Called by run_json_query() """
    log.debug( f'counter, `{counter}`')
    counter_str = f'{counter:03}'
    filename= '%s/d_items_dct/%s.json' % ( FILE_DOWNLOAD_DIR, counter_str )
    with open( filename, 'w' ) as f:
        f.write( jsn_str )
    return


def run_json_query():
    """ Runs the json query and saves all files. """
    log.debug( '\n-------\nrunning the json query' )
    start_time = datetime.datetime.now()
    token = get_token()
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
    offset = 0
    limit = 100
    continue_flag = True
    custom_headers = {'Authorization': 'Bearer %s' % token }
    counter = 1
    while continue_flag is True:
        items_query_url = f'{API_ROOT_URL}items/query?offset={offset}&limit={limit}'
        payload = { 'json': query_json }
        r = requests.post( items_query_url, headers=custom_headers, json=payload )
        log.debug( f'url was, ```{r.url}```' )
        save_initial_downloads( r.content, counter )
        offset = offset + limit
        counter += 1
        # if offset > 10000000:  # this is the REAL CODE
        #     continue_flag = False
        # rsp_dct = r.json()
        # if rsp_dct['total'] < 100000:
        #     continue_flag = False
        if offset > 200:  # TESTING -- to save 3 files of 100 each
            continue_flag = False
    time_taken = datetime.datetime.now() - start_time
    log.debug( f'run_json_query time_taken, ```{time_taken}```' )

    ## end def run_json_query()


def make_items_dcts():
    """ For each json-query file, prepares a dict with key of item-id. """
    start_time = datetime.datetime.now()
    json_downloads_dir = f'{FILE_DOWNLOAD_DIR}/a_initial_downloads'
    items_dct_dir = f'{FILE_DOWNLOAD_DIR}/b_items_dct'
    counter = 1
    for jfile in os.listdir( json_downloads_dir ):
        log.debug( f'processing file, ```{jfile}```' )
        counter_str = f'{counter:03}'
        if jfile.endswith( '.json' ):
            entries_filepath = f'{json_downloads_dir}/{jfile}'
            log.debug( f'entries_filepath, ```{entries_filepath}```' )
            entries_dct = {}
            items_dct = {}
            with open( entries_filepath, 'r' ) as f:
                entries_dct = json.loads( f.read() )
            for entry_dct in entries_dct['entries']:
                url = entry_dct['link']
                item_id = url[-8:]
                items_dct[item_id] = { 'link': url, 'item_dct': None, 'bib_dct': None }
            items_filepath = f'{items_dct_dir}/{counter_str}.json'
            log.debug( f'about to write out item-dct-file, ```{items_filepath}```' )
            with open( items_filepath, 'w' ) as f2:
                jstring = json.dumps( items_dct, sort_keys=True, indent=2 )
                f2.write( jstring )
            counter +=1
    time_taken = datetime.datetime.now() - start_time
    log.debug( f'make_items_dcts time_taken, ```{time_taken}```' )
    return


# async def get_item_data():
#     start_time = datetime.datetime.now()
#     auth_token = get_token()
#     custom_headers = {'Authorization': f'Bearer {auth_token}' }
#     url = 'https://catalog.library.brown.edu/iii/sierra-api/v5/items/10000001'
#     rsp = await asks.get( url, headers=custom_headers, timeout=2 )
#     log.debug( f'output, ```{rsp.json()}```' )
#     time_taken = datetime.datetime.now() - start_time
#     log.debug( f'get_item_data time_taken, ```{time_taken}```' )
#     return


async def fetch_item_data( index_key, custom_headers, results_holder_dct ):
    url = f'https://catalog.library.brown.edu/iii/sierra-api/v5/items/{index_key}'
    log.debug( f'url, ```{url}```')
    rsp = await asks.get( url, headers=custom_headers, timeout=3 )
    results_holder_dct[ str(index_key) ] = {
        'item_dct': rsp.json(),
        'bib_dct': None
        }
    log.debug( 'fetch done' )
    return

async def get_item_data():
    """ Populates each item-dct with item-data, and saves file. """
    start_time = datetime.datetime.now()
    auth_token = get_token()
    custom_headers = {'Authorization': f'Bearer {auth_token}' }
    source_dir = f'{FILE_DOWNLOAD_DIR}/b_items_dct'
    destination_dir = f'{FILE_DOWNLOAD_DIR}/c_items_dct'
    counter = 1
    for source_file in os.listdir( source_dir ):
        if source_file.endswith( '.json' ):
            source_filepath = f'{source_dir}/{source_file}'
            source_dct = {}
            with open( source_filepath, 'r' ) as f:
                source_dct = json.loads( f.read() )

        key_lst = list( source_dct.keys() )
        # log.debug( f'key_lst, ```{key_lst}```' )

        key_count = len( key_lst )
        # key_count = 10  # TEMP!!
        worker_count = 3

        ( range_count, extra ) = divmod( len(key_lst), worker_count )
        log.debug( f'range_count, `{range_count}`; extra, `{extra}`' )

        ## <https://stackoverflow.com/a/1335618>
        # zip(*(iter(range(10)),) * 3)
        # map( None, *(iter(range(10)),) * 3 )

        # ranges = list( zip(*(iter(range(len(key_lst))),) * worker_count) )  # output: ```[(0, 1, 2), ... (96, 97, 98)]```
        ranges = list( zip(*(iter(range(key_count)),) * worker_count) )  # output: ```[(0, 1, 2), ... (96, 97, 98)]```
        log.debug( f'ranges, ```{ranges}```' )

        range_values = []
        for sub_range in ranges:
            sub_range_values = []
            for index in sub_range:
                value = key_lst[index]
                sub_range_values.append( value )
            range_values.append( sub_range_values )
        log.debug( f'initial range_values, ```{range_values}```' )

        extra_range = None
        if extra > 0:
            extra_range = key_lst[ -extra: ]
            log.debug( f'extra_range, ```{extra_range}```' )

        results_holder_dct = {}
        for sub_range in range_values:
            async with trio.open_nursery() as nursery:
                for index in sub_range:
                    log.debug( f'index, `{index}' )
                    nursery.start_soon( fetch_item_data, index, custom_headers, results_holder_dct )
            log.debug( f'end of sub_range, results_holder_dct, ```{results_holder_dct}```' )
        if extra_range:
            async with trio.open_nursery() as nursery:
                for index in extra_range:
                    log.debug( f'extra-range index, `{index}' )
                    nursery.start_soon( fetch_item_data, index, custom_headers, results_holder_dct )
        log.debug( f'end of extra_range, results_holder_dct, ```{results_holder_dct}```' )

        save_items_dct( json.dumps(results_holder_dct, sort_keys=True, indent=2), counter )
        counter += 1

    time_taken = datetime.datetime.now() - start_time
    log.debug( f'get_item_data time_taken, ```{time_taken}```' )
    return

    ## end get_item_data()


def add_bib_data():
    """ Populates each item-dct with bib-data, and saves file. """
    start_time = datetime.datetime.now()
    auth_token = get_token()
    custom_headers = {'Authorization': f'Bearer {auth_token}' }
    source_dir = f'{FILE_DOWNLOAD_DIR}/c_items_dct'
    counter = 1
    for source_file in os.listdir( source_dir ):
        if source_file.endswith( '.json' ):
            source_filepath = f'{source_dir}/{source_file}'
            source_dct = {}
            with open( source_filepath, 'r' ) as f:
                source_dct = json.loads( f.read() )
            for (key, val_dct) in source_dct.items():
                log.debug( f'key, `{key}`' )
                bibs = source_dct[key]['item_dct']['bibIds']
                source_dct[key]['bib_dct'] = bibs
            save_items_and_bibs_dct( json.dumps(source_dct, sort_keys=True, indent=2), counter )
            counter += 1


# def add_bib_data():
#     """ Populates each item-dct with bib-data, and saves file. """
#     start_time = datetime.datetime.now()
#     auth_token = get_token()
#     custom_headers = {'Authorization': f'Bearer {auth_token}' }
#     source_dir = f'{FILE_DOWNLOAD_DIR}/c_items_dct'
#     counter = 1
#     for source_file in os.listdir( source_dir ):
#         if source_file.endswith( '.json' ):
#             source_filepath = f'{source_dir}/{source_file}'
#             source_dct = {}
#             with open( source_filepath, 'r' ) as f:
#                 source_dct = json.loads( f.read() )
#             for (key, val_dct) in source_dct.items():
#                 log.debug( f'key, `{key}`' )

#                 # bibs = source_dct[key]['item_dct']['bibIds']
#                 # source_dct[key]['bib_dct'] = bibs

#                 bibs = source_dct[key]['item_dct']['bibIds']

#                 bib_url = '%sbibs/' % API_ROOT_URL
#                 payload = { 'id': '1000001' }
#                 log.debug( 'token_url, ```%s```' % token_url )
#                 custom_headers = {'Authorization': 'Bearer %s' % token }
#                 r = requests.get( bib_url, headers=custom_headers, params=payload )
#                 log.debug( 'bib r.content, ```%s```' % r.content )

#             save_items_and_bibs_dct( json.dumps(source_dct, sort_keys=True, indent=2), counter )
#             counter += 1


if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) == 2 else None
    log.debug( f'argument, `{arg}`' )
    if arg == 'run_json_query':
        run_json_query()
    elif arg == 'make_items_dct':
        make_items_dcts()
    elif arg == 'get_item_data':
        trio.run( get_item_data )
    elif arg == 'add_bib_data':
        add_bib_data()
    # elif arg == 'enhance_bib_data':
    #     enhance_bib_data()


