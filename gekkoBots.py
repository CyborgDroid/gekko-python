import json, codecs
import requests, urllib3
import time
import click
from short_utm import UniversalTableMethods as utm

#when running self-signed SSL certs on a VPS, ignore warnings in console:
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class gekkoServer(object):
    def __init__(self):
        self.get_server_info()
        self.gekkos = self.get_gekkos()
        self.scansets = []

    def get_scansets(self):
        response = self.api_calls(type='post', call='scansets')
        if response.status_code == 200:
            data = response.json()['datasets']
            return data
    
    #NOT WORKING AT ALL
    def start_imports_for_gaps(self):
        self.scansets = self.get_scansets()  
        for d in self.scansets:
            print(d['asset'])
            print(d)
            from_values = [r['from'] for r in d['ranges']]
            to_values = [r['to'] for r in d['ranges']]
            print('from values: ', from_values)
            print('to values: ', to_values)
            print('Max: ', max([r['to'] for r in d['ranges']]))

    def import_data(self, exchange, currency, asset, from_date, to_date):
        conf = {"watch":{
            "exchange":exchange,
            "currency":currency,
            "asset":asset},
            "importer":{
                "daterange":{
                    "from": from_date,            #"2018-03-19 04:35"
                    "to": to_date }},             #"2018-04-05 04:38"
                "candleWriter":{"enabled":True}}
        response = self.api_calls(type='post', call='import', data=conf)

    def start_from_file(self, file_name):
        with open(file_name, 'r') as f:
            d = json.load(f)
            f.close()
            #start all watchers first
            [self.start_watcher(x['exchange'], x['currency'], x['asset']) for x in d if x['type'] == 'watcher']
            #start all tradebots now
            [self.start_trader(x['exchange'], x['currency'], x['asset'], x['candleSize'], x['historySize'], x['strat_name'], x['strat_settings']) for x in d if x['type'] == 'tradebot']


    def save_gekkos(self, gekko_list_id):
        watchers = [
            {'type': w['type'], 'exchange': w['watch']['exchange'], 'asset': w['watch']['asset'], 'currency': w['watch']['currency']} for w in self.gekkos if w['type'] == 'watcher']
        tradebots = [
            {'type': w['trader'], 'exchange': w['watch']['exchange'], 'asset': w['watch']['asset'], 'currency': w['watch']['currency'],
            'strat_name': w['strat']['name'], 'candleSize': w['strat']['tradingAdvisor']['candleSize'], 'historySize': w['strat']['tradingAdvisor']['historySize'],
            'strat_settings': w['strat']['params']} for w in self.gekkos if w['type'] == 'leech' and w['trader'] == 'tradebot']
        # ****** ADD PAPER TRADERS HERE TO SAVE
        watchers.extend(tradebots)
        with open('saved_bots-' + gekko_list_id + '.txt', 'wb') as f:
            json.dump(watchers, codecs.getwriter('utf-8')(f), sort_keys = True, indent = 4, ensure_ascii=False)
            f.close()
        return 'saved_bots-' + gekko_list_id + '.txt'

    def get_server_info(self):
        # ########################
        # server_credentials_api.json file structure: 
        # (for protecting your website with login credentials)
        # [{
        #   "server_addr" : "https://123.456.789/api/"
        #   "username" : "myuser"
        #   "password" : "mypassword"
        # }]
        ############################
        #UNCOMMENT THE FOLLOWING 4 LINES TO RUN ON A VPS, (see above instructions too)
        #data = json.load(open('server_credentials_api.json', 'r'))
        #self.api_url = data[0]['server_addr']
        #self.u_name = data[0]['username']
        #self.p_word = data[0]['password']
        #UNCOMMENT TO RUN LOCALLY:
        self.api_url = 'http://localhost:3000/api/'

    def api_calls(self, type='get', call='gekkos', data={}):
        data = json.dumps(data)
        header = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json'
        }
        if type == 'get':
            if 'localhost' in self.api_url:
               response = requests.get(self.api_url + call, data=data, headers=header)
            else:
               response = requests.get(self.api_url + call, data=data, headers=header, verify=False, auth=(self.u_name, self.p_word))
        elif type == 'post':
            if 'localhost' in self.api_url:
                response = requests.post(self.api_url + call, data=data, headers=header)
                print(response.request.body)
            else:
                response = requests.post(self.api_url + call, data=data, headers=header, verify=False, auth=(self.u_name, self.p_word))
        return response

    def get_gekkos(self):
        response = self.api_calls('get', 'gekkos')
        if response.status_code == 200:
            data = response.json()
            return data

    def kill_gekko(self, id_to_kill):
        gekko_ids = [d['id'] for d in self.gekkos]
        def killer(x):
            if x in gekko_ids:
                response = self.api_calls('post','killGekko', {'id':x})
                if response.status_code == 200:
                    print('************* ', x, ' Gekko killed', response.json())
                else:
                    print('************* kill failed', response)
            else:
                print('******* ' + id_to_kill, ' not in list of gekko ids ' , gekko_ids , '*******' )
        if id_to_kill == "all":
            [killer(d['id']) for d in self.gekkos]
        else:
            killer(id_to_kill)
        self.gekkos = self.get_gekkos()
    
    def start_all(self):
        self.scansets = self.get_scansets()
        [self.start_watcher(i['exchange'], i['currency'], i['asset']) for i in self.scansets]

    def start_watcher(self, exchange, currency, asset):
        conf = {
            "type":"market watcher", 
            "mode":"realtime",
            "watch":{
                "exchange":exchange,
                "currency":currency,
                "asset":asset
            },
            "candleWriter":{"enabled":True, "adapter":"sqlite"}
        }
        matching_watchers=False
        if self.gekkos:
            flattened_gekkos = [utm.flatten_json(x) for x in self.gekkos]
            matching_watchers = [w for w in flattened_gekkos if w['type'] == 'watcher' and w['watch.exchange'] == exchange and w['watch.currency'] == currency and w['watch.asset'] == asset]
        if matching_watchers:
            utm.print_lod('watcher already active: ', utm.filter_lod_keys(['trader', 'type', 'watch.exchange', 'watch.currency', 'watch.asset', 'id'],matching_watchers))
        else:
            self.start_config(conf)
        

    def start_trader(self, exchange, currency, asset, candleSize, historySize, strat_name, strat_settings):
        #create conf file
        conf= {"market":{"type":"leech"},
            "mode":"realtime",
            "watch":{
                "exchange":exchange,
                "currency":currency,
                "asset":asset
            },
            "tradingAdvisor":{
                "enabled":True,
                "method":strat_name,
                "candleSize":candleSize,
                "historySize":historySize
            },
            strat_name: strat_settings,
            "candleWriter":{"enabled":True,"adapter":"sqlite"},
            "type":"tradebot",
            "performanceAnalyzer":{"riskFreeReturn":2,"enabled":True},
            "trader":{"enabled":True},"valid":True}
        #check if watcher already exists:
        flattened_gekkos = [utm.flatten_json(x) for x in self.gekkos]
        matching_leeches = [w for w in flattened_gekkos if w['type'] == 'leech' and w['watch.exchange'] == exchange and w['watch.currency'] == currency and w['watch.asset'] == asset]
        if matching_leeches:
            utm.print_lod('Trader already active: ', utm.filter_lod_keys(['trader', 'type', 'watch.exchange', 'watch.currency', 'watch.asset', 'id'],matching_leeches))
        else:
            self.start_config(conf)
    
    def start_config(self, config):
        header = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json'
        }
        try:
            self.api_calls(type='post', call='startGekko', data=config)
            time.sleep(2)
            self.gekkos = self.get_gekkos()
        except Exception as e:
            print('EXCEPTION:', e)

@click.command()
@click.option('--server', help='local or VPS')
@click.option('--kill', help='id of gekko to kill, or "all"')
@click.option('--start', help='"filename.txt" starts all saved gekkos in file. "all" starts watchers for imports ("all" NOT WORKING).')
@click.option('--save', help='Save current gekkos for later. Specify gekko file identifier to distinguish from other saves.')
@click.option('--import_all', help='set to "true" to import all missing data in databases')
def init_Gekko(server='local' ,kill=False, start=False, save=False, import_all=False):
    srv = gekkoServer()
    if server == 'VPS':
        print('use VPS')
    if kill:
        srv.kill_gekko(kill)
    if import_all:
        srv.start_imports_for_gaps()
    if start:
        srv.scansets = srv.get_scansets()
        if start == 'all':
            srv.start_all()
        else:
            srv.start_from_file(start)
    #print gekko info:
    if srv.gekkos and len(srv.gekkos) > 0:
        gekkos = [utm.flatten_json(x) for x in srv.gekkos]
        gekkos = utm.filter_lod_keys(['trader', 'type', 'watch.exchange', 'watch.currency', 'watch.asset', 'id', 'startAt'], gekkos)
        gekkos = utm.fill_in_missing_keys_in_lod(gekkos)
        utm.print_lod( "****************** RUNNING GEKKOS *******************", gekkos)
        if save:
            print('GEKKOS SAVED TO: ', srv.save_gekkos(save))
    else:
        print('****************** NO GEKKOS RUNNING *******************')

if __name__ == '__main__':
    init_Gekko()
