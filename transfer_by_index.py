#!/usr/bin/env python
import os
import sys
import csv
import json
import time
import shlex
import subprocess

from argparse import ArgumentParser

import dump_es_index

from amq import post_ads
from transfer_helpers import convert_dates_to_millisecs
from transfer_helpers import read_es_config
from transfer_helpers import free_diskspace
from transfer_helpers import set_up_logging


def get_index_names_quick(pattern='cms-20'):
    """Download and return a list of all index names"""
    es_conf = read_es_config("es.conf")

    cmd = "curl https://{user}:{pass}@{host}:{port}/_indices?v".format(**es_conf)
    result = subprocess.Popen(shlex.split(cmd),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE).communicate()[0]

    result = json.loads(result)
    indices = [k for k in result.keys() if k.startswith(pattern)]

    return sorted(indices)


def get_index_data(pattern='cms-20', outputfile='indices.json'):
    """Download a list of all indices with more information"""
    es_conf = read_es_config("es.conf")

    cmd = "curl https://{user}:{pass}@{host}:{port}/_cat/_indices?v".format(**es_conf)
    result = subprocess.Popen(shlex.split(cmd),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE).communicate()[0]

    lines = result.split('\n')
    reader = csv.DictReader(lines, delimiter=' ', skipinitialspace=True)

    indices = {}
    for row in reader:
        if not row['index'].startswith(pattern):
            continue
        indices[row['index']] = row

    print "Found %d indices on %s:%d for pattern '%s'" % (len(indices.keys()), hostname, port, pattern)

    with open(outputfile, 'w') as jsonfile:
        json.dump(indices, jsonfile, indent=2, sort_keys=True)

    print "Wrote index information to '%s'" % outputfile 


def remove_local_dump(index, target='/data/raw_index_data/'):
    location = os.path.join(target, '%s.json'%index)
    try:
        os.remove(location)
        print ">>> Index file %s deleted" % location
    except OSError:
        pass


def dump_or_load(index, source):
    location = os.path.join(source, '%s.json'%index)
    if not os.path.isfile(location):
        dump_es_index.dump_index(index, target=source)

    return location


class ESTransferByIndex(object):
    def __init__(self, args):
        self.args = args
        self.index_info_file = 'indices.json'
        self.dump_location = '/data/raw_index_data/'
        self.buffer_size = 10000
        self.buffer = []

        self.checkpoint = []

        self.load_index_info()
        self.load_checkpoint()

        self.selected_indices = None
        if self.args.process_these:
            self.selected_indices = [i.strip() for i in self.args.process_these.split(',')]
            assert(all([i in self.index_info.keys() for i in self.selected_indices]))


    def load_index_info(self):
        with open(self.index_info_file, 'r') as idxfile:
            self.index_info = json.load(idxfile)


    def load_checkpoint(self):
        try:
            with open(self.args.checkpoint_file, 'r') as chkpfile:
                self.checkpoint = [l.strip() for l in chkpfile]
        except IOError, e:
            print "Checkpoint file %s empty!" % self.args.checkpoint_file

        # Make sure there are no duplicates
        assert(len(self.checkpoint) == len(set(self.checkpoint)))


    def mark_as_done(self, index):
        self.checkpoint.append(index)
        if self.args.dry_run:
            return
        with open(self.args.checkpoint_file, 'a') as chkpfile:
            chkpfile.write('%s\n'%index)

        if self.args.clean_after_upload:
            remove_local_dump(index, self.dump_location)


    def dump(self, check=False):
        """
        Process indices that is not in checkpoint file (i.e. marked as done),
        and run elasticdump to download them to local disk.

        If check is true, check whether the number of entries are consistent.
        """
        starttime = time.time()

        indices_to_process = self.selected_indices or sorted(self.index_info.keys())
        indices_to_process = set(indices_to_process).difference(set(self.checkpoint))

        # Process first index that is not in checkpoint
        for index in sorted(indices_to_process):

            mystart = time.time()
            print (">>> Processing index %s (size: %s, ndocs: %d)" %
                         (index, self.index_info[index]['pri.store.size'],
                             int(self.index_info[index]['docs.count'])))

            if free_diskspace("/data/") < 20e9:
                print (">>> Less than 20 GB free disk space, aborting.")
                return

            with open(dump_or_load(index, source=self.dump_location), 'r') as dumpfile:
                if check:
                    count = 0
                    for line in dumpfile:
                        count += 1

                    # Check if length is what we expected from the index data
                    assert(count == int(self.index_info[index]['docs.count']))

                print (">>> Index %s done, %d docs, %s size, %.2f mins, %.2f mins total" %
                        (index,
                         int(self.index_info[index]['docs.count']),
                         self.index_info[index]['pri.store.size'],
                         (time.time()-mystart)/60.,
                         (time.time()-starttime)/60.))


    def clear_buffer(self):
        bunch = ((d['GlobalJobId'], convert_dates_to_millisecs(d)) for d in self.buffer)
        if self.args.dry_run:
            self.buffer = []
            return

        n_sent = post_ads(bunch)
        assert(n_sent == len(self.buffer))
        self.buffer = []


    def run(self):
        starttime = time.time()

        # Process first index that is not in checkpoint
        for index in sorted(self.index_info.keys()):
            if index in self.checkpoint:
                continue

            self.load_checkpoint() # Refresh checkpoint and check again
            if index in self.checkpoint:
                continue

            print ">>> %d of %d indices processed according to %s" % (
                len(self.checkpoint), len(self.index_info.keys()), self.args.checkpoint_file)

            # if index == 'cms-2017-06-14': break

            if self.selected_indices and not index in self.selected_indices:
                continue

            mystart = time.time()
            n_total = int(self.index_info[index]['docs.count'])
            print (">>> Processing index %s (size: %s, ndocs: %d)" %
                         (index, self.index_info[index]['pri.store.size'], n_total))

            count = 0
            with open(dump_or_load(index, source=self.dump_location), 'r') as dumpfile:
                for line in dumpfile:
                    try:
                        raw = json.loads(line)
                        doc = raw['_source']
                    except ValueError, e:
                        print "&&& ERROR: Failed to parse doc from line in raw data! index %s, line %d" % (index, count+1)
                        raise e

                    self.buffer.append(doc)
                    count += 1

                    if len(self.buffer) == self.buffer_size:
                        self.clear_buffer()
                        sys.stdout.write(">>> Sent {}/{} [{:.1%}]\r".format(
                                    count, n_total,
                                    count/float(n_total)))
                        sys.stdout.flush()

            # Check if length is what we expected from the index data
            assert(count == n_total)

            if len(self.buffer):
                self.clear_buffer()
                print ">>> Sent %d/%d [100.0%%]" % (count, n_total)

            self.mark_as_done(index)
            print (">>> Index %s done, %d docs, %s size, %.2f mins, %.2f mins total" %
                    (index, count, self.index_info[index]['pri.store.size'],
                    (time.time()-mystart)/60.,
                    (time.time()-starttime)/60.))

            if index == self.args.until_index:
                break


def main(args):

    if args.get_index_data != '':
        outputfile = args.get_index_data
        if not outputfile.endswith('.json'):
            outputfile += '.json'
        get_index_data(outputfile=outputfile)
        return

    est = ESTransferByIndex(args=args)

    if args.dump:
        est.dump()
    else:
        est.run()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--get_index_data", default='',
                        type=str, dest="get_index_data",
                        help="Dump a list of indices to this file [default: %(default)s]")
    parser.add_argument("--checkpoint_file", default='index_checkpoint.dat',
                        type=str, dest="checkpoint_file",
                        help="Process the indices from this file [default: %(default)s]")
    parser.add_argument("--dump", action='store_true',
                        dest="dump",
                        help="Just run elasticdump")
    parser.add_argument("--dry_run", action='store_true',
                        dest="dry_run",
                        help="Don't do anything")
    parser.add_argument("--clean_after_upload", action='store_true',
                        dest="clean_after_upload",
                        help="Remove the local dump after uploading (to clear space)")

    parser.add_argument("--until_index", default='',
                        type=str, dest="until_index",
                        help="Process everything up to and including this index [default: %(default)s]")
    parser.add_argument("--process_these", default='',
                        type=str, dest="process_these",
                        help="Process these indices (comma-sep list) [default: %(default)s]")

    args = parser.parse_args()

    set_up_logging()
    main(args)
