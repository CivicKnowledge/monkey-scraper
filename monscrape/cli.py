"""

"""

import argparse
import http.client
import mimetypes
from pathlib import Path
import json
from yaml import load, safe_load
import hashlib
import pandas as pd
import os
import sys
from shutil import rmtree
from . import __version__

def main():
    """Scrape Survey Monkey responses for a United Way program

    If neither --download nor --process are specified, both
    operations are executed.

    Specify the auth token in the YAM config file, or in an env var:

    $ export MONSCRAPE_TOKEN=0gwc1qdjRCLe6BRKH0fM...kpOtdskQSlIWyim55pgd1fmKx

    The download function will always download and re-cache the first page. If the
    total number of record has changed, it will also re-download the last page,
    and download and cache any subsequent pages.

    """

    parser = argparse.ArgumentParser(description=main.__doc__)

    #parser.add_argument('integers', metavar='N', type=int, nargs='+',
    #                    help='an integer for the accumulator')

    parser.add_argument('-C', '--clean', action='store_true', help='Clean the cache')
    parser.add_argument('-k', '--cache', help='Cache directory')
    parser.add_argument('-d', '--download', action='store_true', help='Download new data')
    parser.add_argument('-c', '--csv', action='store_true', help='Process cached documents and create CSV file')
    parser.add_argument('-g', '--google', action='store_true', help='Process cached documents and upload to a google sheet.')

    parser.add_argument('-v', '--version', action='store_true', help='print the version')

    parser.add_argument('collector_id', nargs='?', help = 'Collector id to run')

    parser.add_argument('output_file', nargs='?', help = 'File to write output to. If not specified, write to stdout')

    args = parser.parse_args()


    if not any([args.download, args.csv, args.google]):
        args.download = True
        args.csv  = True

    try:
        cf = Path('monscrape.yaml')
        config = safe_load(cf.read_text())

    except FileNotFoundError:
        cf = None
        config= None

    if args.version:
        print(f"Version: {__version__} ")
        if config:
            print(f"Config file: {str(cf)}")
        else:
            print("Config file not found")
        sys.exit(0)


    if args.collector_id:
        run_for_collector(config, args, args.collector_id)
    else:
        if not config:
            print("ERROR: did not find config file")
            sys.exit(1)

        for collector_id in config['collectors']:
            print("== ", collector_id)
            run_for_collector(config, args, str(collector_id))

def run_for_collector(config, args, collector_id):

    s = Scraper(config, collector_id, args.output_file, args.cache)

    if args.clean:
        s.clean_cache()

    if args.download:
        for d in s.get_pages(s.collector_id):
            pass

    if args.csv:
        s.save_to_csv()

    if args.google:
        s.write_to_google()

class Scraper(object):

    def __init__(self,config, collector_id, output_file, cache_dir=None):
        self.config = config
        self.cache_dir = cache_dir
        self._conn = None
        self.collector_id = collector_id
        self.output_file = output_file

    @property
    def token(self):

        token = self.config.get('auth', {}).get('token', {})

        if not token:
            token = os.environ.get('MONSCRAPE_TOKEN')

        if not token:
            print("ERROR: No token is set in the configuration, nor in the MONSCRAPE_TOKEN environmental variable")
            sys, exit(1)

        return token

    @property
    def cache(self):

        if self.cache_dir:
            cache_dir = self.cache_dir
        elif 'cache' in self.config:
            cache_dir = self.config['cache']
        else:
            cache_dir = './cache'

        if not cache_dir:
            print("ERROR: Cache dir not specified properly")
            sys, exit(1)

        cache = Path(cache_dir)

        if not cache.exists():
            cache.mkdir()

        return cache

    def clean_cache(self):
        rmtree(self.cache.joinpath(self.collector_id))


    @property
    def headers(self):

        return {
            'Content-Type': 'application/json',
            'Authorization': 'bearer {}'.format(self.token),
            #'Authorization': 'Bearer BjVjlV9MiVBgfe1XpS2xPRHlzllEqnNygHWI0gwc1qdjRCLe6BRKH0fMMkpOtdskQSlIWyim55pgd1fmKxfiizg8B2XJaRcq3bvaZIKx97B.8zGoycOC1m-ob78PnFLz',
            'Cookie': 'ep202=Ur98chLJzFeuGODG4KwqKoznzbk=; ep203=g35YDLOje7psBMfg5k8L+RogFlk=; attr_multitouch=pZuSd4dXtr67zGj9MVeKEC82erE=; ep201=3NpzlwyFtsYd7oBTBoAk0y9AX+I='
        }

    @property
    def connection(self):
        if not self._conn:
            self._conn =http.client.HTTPSConnection("api.surveymonkey.net")

        return self._conn

    def bulk_url(self,collector_id):
        return f"/v3/collectors/{collector_id}/responses/bulk"

    def get_page(self, url, cache_read=True, cache_write=True, report=True):

        hash = hashlib.md5(url.encode('utf-8')).hexdigest()

        cp = self.cache.joinpath(self.collector_id, hash)

        cp.parent.mkdir(parents=True, exist_ok=True)

        if cp.exists() and cache_read:
            t = cp.read_text()
        else:
            if report:
                print("Downloading ", url, cp)
            conn = self.connection
            headers = self.headers
            payload = ''

            conn.request("GET", url, payload, headers)
            t = conn.getresponse().read().decode('utf-8')

            if cache_write:
                cp.write_text(t,'utf-8')

        return json.loads(t)

    def get_pages(self, collector_id):
        """Download and cache pages"""

        d = self.get_page(self.bulk_url(collector_id))
        d2 = self.get_page(self.bulk_url(collector_id), cache_read=False, cache_write=True, report=False)

        # Do we need to re-fetch the last page?
        refetch_last = d['total'] !=  d2['total']

        while True:
            yield d
            next = d['links'].get('next')
            if not next:
                break

            if next == d['links']['last'] and refetch_last:
                use_cache = False
                refetch_last = False
            else:
                use_cache = True

            d = self.get_page(next, cache_read=use_cache, cache_write=True)

    def process_page(self, d):
        """Yield records from one page. """
        for e in d.get('data'):
            h = {
                'Survey ID': e.get('survey_id'),
                'Respondent ID': e.get('id'),
                'Collector': self.collector_id,
                'Edit URL': e.get('edit_url'),
                'Analyze URL': e.get('analyze_url'),
                'API Link': e.get('href'),
                'Node ID': None,
                'File Name': None,
                'Download URL': None
            }

            n_rows = 0
            for page in e.get('pages'):
                for question in page.get('questions'):
                    for answer in question.get('answers'):
                        if 'download_url' in answer:
                            r = h.copy()
                            r.update({
                                'Node ID': question.get('id'),
                                'File Name': answer.get('text'),
                                'Download URL': answer.get('download_url'),
                            })
                            yield r
                            n_rows += 1
            if n_rows == 0:
                yield h

    def _process_cached_pages(self):
        recs = []

        for page in self.cache.joinpath(str(self.collector_id)).rglob("*"):
            if page.is_file():
                d = json.loads(page.read_text())
                for rec in self.process_page(d):
                    recs.append(rec)

        df = pd.DataFrame(recs)

        return df

    def save_to_csv(self):

        df = self._process_cached_pages()

        if self.output_file:
            if self.output_file == '-':
                print(df.to_csv())
            else:
                f = Path(self.output_file).with_suffix('.csv')
                print(f'Wrote to {len(df)} rows to {str(f)}')
                df.to_csv(f)
        else:
            f = Path(str(self.collector_id)).with_suffix('.csv')
            print(f'Wrote to {len(df)} rows to {str(f)}')
            df.to_csv(f)


    def write_to_google(self):

        df = self._process_cached_pages()

        import gspread
        import gspread_dataframe as gd
        from oauth2client.service_account import ServiceAccountCredentials

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

        credentials = ServiceAccountCredentials \
            .from_json_keyfile_name(self.config['google']['cred_file'], scope)

        gc = gspread.authorize(credentials)

        wks = gc.open_by_key(self.config['google']['gs_key']).worksheet(self.collector_id)

        df = gd.get_as_dataframe(wks)
        print(f"Current sheet has  {len(df)} rows ")

        df = self._process_cached_pages()

        print(f"Writing {len(df)} rows to Google Sheet")
        gd.set_with_dataframe(wks,df)





