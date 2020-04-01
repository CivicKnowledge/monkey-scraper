"""

"""

import argparse
import http.client
import mimetypes
from pathlib import Path
import json
from slugify import slugify
from yaml import load, safe_load
import hashlib
import pandas as pd
import os
import sys

def main():
    """Scrape Survey Monkey responses for a United Way program

    If neither --download nor --process are specified, both
    operations are executed.

    Specify the auth token in the YAM config file, or in an env var:

    $ export MONSCRAPE_TOKEN=0gwc1qdjRCLe6BRKH0fM...kpOtdskQSlIWyim55pgd1fmKx

    BUGS: This will probably not catch new records that are added to the last page

    """

    parser = argparse.ArgumentParser(description=main.__doc__)

    #parser.add_argument('integers', metavar='N', type=int, nargs='+',
    #                    help='an integer for the accumulator')

    parser.add_argument('-d', '--download', action='store_true', help='Process cached documents')
    parser.add_argument('-p', '--process', action='store_true', help='Process cached documents')

    parser.add_argument('collector_id', help = 'Collector id to run')


    parser.add_argument('output_file', nargs='?', help = 'File to write output to. If not specified, write to stdout')

    args = parser.parse_args()

    if not any([args.download, args.process]):
        args.download = True
        args.process  = True

    try:
        cf = Path('monscrape.yaml')
        config = safe_load(cf.read_text())
        token = config.get('auth', {}).get('token',{})
    except FileNotFoundError:
        token = None

    if not token:
        token = os.environ.get('MONSCRAPE_TOKEN')

    if not token:
        print("ERROR: No token is set in the configuration, nor in the MONSCRAPE_TOKEN environmental variable")
        sys,exit(10)

    s = Scraper(token, args.collector_id, args.output_file)

    if args.download:
        for d in s.get_pages(args.collector_id):
            pass

    if args.process:
        s.process_cached_pages()

class Scraper(object):

    def __init__(self,token, collector_id, output_file, cache_dir=None):
        self.token = token
        self.cache_dir = None
        self._conn = None
        self.collector_id = collector_id
        self.output_file = output_file

    @property
    def cache(self):

        cache_dir = self.cache_dir or './cache'

        cache = Path(cache_dir)

        if not cache.exists():
            cache.mkdir()

        return cache

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

    def get_page(self, url):

        hash = hashlib.md5(url.encode('utf-8')).hexdigest()

        cp = self.cache.joinpath(self.collector_id, hash)

        cp.parent.mkdir(parents=True, exist_ok=True)

        if cp.exists():

            t = cp.read_text()
        else:
            print("Downloading ", url)
            conn = self.connection
            headers = self.headers
            payload = ''

            conn.request("GET", url, payload, headers)
            t = conn.getresponse().read().decode('utf-8')

            cp.write_text(t,'utf-8')

        return json.loads(t)

    def get_pages(self, collector_id):
        """Download and cache pages"""

        d = self.get_page(self.bulk_url(collector_id))

        while True:
            yield d
            next = d.get('links').get('next')
            if not next:
                break
            d = self.get_page(next)

    def process_page(self, d):
        """Yield records from one page. """
        for e in d.get('data'):
            h = {
                'Survey ID': e.get('survey_id'),
                'Respondent ID': e.get('id'),
                'Collector': self.collector_id,
                'Edit URL': e.get('edit_url'),
                'Analyze URL': e.get('analyze_url'),
                'API Link': e.get('href')
            }

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

    def process_cached_pages(self):

        recs = []
        for page in self.cache.joinpath(self.collector_id).rglob("*"):
            if page.is_file():
                d = json.loads(page.read_text())
                for rec in  self.process_page(d):
                    recs.append(rec)

        df = pd.DataFrame(recs)

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







