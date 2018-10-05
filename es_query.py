#!/usr/bin/env python
import os
import sys
import time
import json
import es_transfer
from elasticsearch import helpers as es_helpers
from elasticsearch import Elasticsearch

from datetime import datetime
from argparse import ArgumentParser


def date_to_timestamp(year, month, day):
    dt = datetime(year, month, day)
    return int((dt - datetime(1970, 1, 1)).total_seconds())


_es_handle = None
def get_es_handle(hostname="es-cms.cern.ch", port=9203):
    global _es_handle
    if not _es_handle:
        username, passwd = es_transfer.read_es_config("es.conf")
        _es_handle = Elasticsearch([{"host": hostname, "port":port, 
                                     "http_auth":username+":"+passwd}],
                                     verify_certs=True,
                                     use_ssl=True,
                                     ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt')


def query_by_timestamp(timestamp):
    get_es_handle(args.hostname, args.port)
    query = {"query": {
                "range": {
                    "RecordTime": {
                            "gte" : timestamp,
                            "lt"  : timestamp + 24*60*60
                        }
                    }
                }
            }

    res = _es_handle.search(index='cms-20*',
                            body=json.dumps(query))

    total = res['hits']['total']
    print 'Found %d documents total' % total

    es_scan = es_helpers.scan(
            _es_handle,
            query=query,
            index='cms-20*',
            doc_type='job',
            size=10000
        )

    return es_scan, total


def print_progress(current, total):
    sys.stdout.write(">>> Wrote {}/{} [{:.1%}]\r".format(
                    current, total,
                    current/float(total)))
    sys.stdout.flush()



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
    for date_string in args.recordtime:
        date = tuple([int(d) for d in date_string.split('-')])
        timestamp = date_to_timestamp(*date)
        print "Querying for %s, %d-%d" % (date_string, timestamp, timestamp+24*60*60)

        data, n_docs = query_by_timestamp(timestamp)

        dumpfile = os.path.join(args.target, 'es-cms-dump-%s.json' % date_string)
        dump_to_file(data, n_docs, dumpfile)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('recordtime', metavar='recordtime', type=str, nargs='+',
                        help='Dump these RecordTimes')
    parser.add_argument("--target", default='/data/raw_index_data/',
                        type=str, dest="target",
                        help="Target destination [default: %(default)s]")
    parser.add_argument("--hostname", default='es-cms.cern.ch',
                        type=str, dest="hostname",
                        help="ES hostname [default: %(default)s]")
    parser.add_argument("--port", default=9203,
                        type=int, dest="port",
                        help="ES port [default: %(default)s]")
    parser.add_argument("--dry_run", action='store_true',
                        dest="dry_run",
                        help="Don't do anything")
    args = parser.parse_args()

    main(args)
