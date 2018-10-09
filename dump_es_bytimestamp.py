#!/usr/bin/env python
import os
import json
from elasticsearch import helpers as es_helpers
from elasticsearch import Elasticsearch

from datetime import datetime
from argparse import ArgumentParser

from transfer_helpers import print_progress
from transfer_helpers import read_es_config

def date_to_timestamp(year, month, day):
    try:
        dt = datetime(year, month, day)
    except ValueError, e:
        print "Invalid date: %s" % str(e)
        return None

    return int((dt - datetime(1970, 1, 1)).total_seconds())


def date_string_to_timestamp(date_string):
    date = tuple([int(d) for d in date_string.split('-')])
    timestamp = date_to_timestamp(*date)
    return timestamp


_es_handle = None
def get_es_handle():
    global _es_handle
    if not _es_handle:
        es_conf = read_es_config("es.conf")
        _es_handle = Elasticsearch([{"host": es_conf['host'],
                                     "port":es_conf['port'], 
                                     "http_auth": "{user}:{pass}".format(**es_conf)}],
                                     verify_certs=True,
                                     use_ssl=True,
                                     ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt')


def make_query(ts_from, ts_to=None):
    ts_to = ts_to or ts_from + 24*60*60
    query = {"query": {
            "range": {
                "RecordTime": {
                        "gte" : ts_from,
                        "lt"  : ts_to
                    }
                }
            }
        }

    return query


def get_total_hits(query, index='cms-20*'):
    get_es_handle()
    res = _es_handle.search(index=index,
                            body=json.dumps(query))

    return res['hits']['total']


def get_es_scan(query, index='cms-20*', buffer_size=5000):
    get_es_handle()

    es_scan = es_helpers.scan(
            _es_handle,
            query=query,
            index='cms-20*',
            doc_type='job',
            size=buffer_size
        )

    return es_scan


def dump_to_file(data, n_docs, filename):
    count = 0
    print_progress(count, n_docs)
    with open(filename, 'w') as dfile:
        for doc in data:
            json.dump(doc, dfile)
            dfile.write('\n')
            count += 1
            if count % 100 == 0:
                print_progress(count, n_docs)

    print ">>> Wrote %d/%d [100.0%%]" % (count, n_docs)
    print 'Dumped %d docs into %s' % (count, filename)


def main(args):
    for date_string in args.recordtimes:
        timestamp = date_string_to_timestamp(date_string)
        print "Querying for %s, %d-%d" % (date_string, timestamp, timestamp+24*60*60)

        data, n_docs = scan_timestamp_range(timestamp, timestamp+24*60*60)

        dumpfile = os.path.join(args.target, 'es-cms-dump-%s.json' % date_string)
        dump_to_file(data, n_docs, dumpfile)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('recordtimes', metavar='recordtimes', type=str, nargs='+',
                        help='Dump these RecordTimes')
    parser.add_argument("--target", default='/data/raw_index_data/',
                        type=str, dest="target",
                        help="Target destination [default: %(default)s]")
    args = parser.parse_args()

    main(args)
