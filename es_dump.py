#!/usr/bin/env python
import os
import sys
import time
import shlex
import subprocess
import es_transfer

from argparse import ArgumentParser


def dump_index(index, hostname="es-cms.cern.ch", port=9203,
               target='/data/raw_index_data/', dry_run=False):

    if not os.path.isdir(target) and not dry_run:
        os.makedirs(target)
    
    destination = os.path.join(target, "%s.json"%index)
    username, password = es_transfer.read_es_config("es.conf")
    starttime = time.time()
    print ">>> Running elasticdump"
    cmd = "elasticdump --input=https://%s:%s@%s:%d/%s" % (username, password, hostname, port, index)
    cmd += " --output=%s/%s.json --type data --limit 2500" % (target, index)
    if not dry_run:
        result = subprocess.Popen(shlex.split(cmd),
                                  stdout=sys.stdout,
                                  stderr=sys.stderr).communicate()[0]
    else:
        print cmd

    print "Index %s dumped to %s in %.2f mins" % (index, target, (time.time()-starttime)/60.)


def main(args):
    for index in args.indices:
        dump_index(index, hostname=args.hostname, port=args.port,
                   target=args.target, dry_run=args.dry_run)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('indices', metavar='indices', type=str, nargs='+',
                        help='Dump these indices')
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
