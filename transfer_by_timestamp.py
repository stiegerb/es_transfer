#!/usr/bin/env python
import os
import sys
import csv
import time

from argparse import ArgumentParser

from dump_es_bytimestamp import date_string_to_timestamp
from dump_es_bytimestamp import scan_timestamp_range

from amq import post_ads
from transfer_helpers import convert_dates_to_millisecs
from transfer_helpers import read_es_config


class ESTransferByTimestamp(object):
    def __init__(self, args):
        self.args = args
        self.buffer = []

        self.checkpoint = []
        self.load_checkpoint()


    def load_checkpoint(self):
        try:
            with open(self.args.checkpoint_file, 'r') as chkpfile:
                self.checkpoint = [l.strip() for l in chkpfile]
        except IOError, e:
            print "Checkpoint file %s empty, creating new" % self.args.checkpoint_file

        # Make sure there are no duplicates
        assert(len(self.checkpoint) == len(set(self.checkpoint)))


    def mark_as_done(self, index):
        self.checkpoint.append(index)
        if self.args.dry_run:
            return
        with open(self.args.checkpoint_file, 'a') as chkpfile:
            chkpfile.write('%s\n' % index)


    def clear_buffer(self):
        bunch = ((d['GlobalJobId'], convert_dates_to_millisecs(d)) for d in self.buffer)
        if self.args.dry_run:
            self.buffer = []
            return

        # n_sent = post_ads(bunch)
        assert(n_sent == len(self.buffer))
        self.buffer = []


    def run(self):
        starttime = time.time()

        # Process first index that is not in checkpoint
        for date_string in self.args.date_strings:
            self.load_checkpoint() # Refresh checkpoint before checking
            if date_string in self.checkpoint:
                continue

            mystart = time.time()

            timestamp = date_string_to_timestamp(date_string)
            if not timestamp:
                print 'Invalid date "%s", skipping' % date_string
                continue
            print ">>> Processing %s," % date_string

            es_scan, n_total = scan_timestamp_range(timestamp, timestamp+24*60*60,
                                                    buffer_size=self.args.es_buffer_size)
            print "... found %d documents" % n_total

            count = 0
            for raw_doc in es_scan:
                try:
                    doc = raw_doc['_source']
                except ValueError, e:
                    print ("&&& ERROR: Failed to parse doc from line in raw data!"
                           " date_string %s, line %d") % (date_string, count+1)
                    raise e

                self.buffer.append(doc)
                count += 1

                if len(self.buffer) == self.args.amq_buffer_size:
                    self.clear_buffer()
                    sys.stdout.write(">>> Sent {}/{} [{:.1%}]\r".format(
                                count, n_total,
                                count/float(n_total)))
                    sys.stdout.flush()

            # Check if length is what we expected from the initial query
            assert(count == n_total)

            if len(self.buffer):
                self.clear_buffer()
                print ">>> Sent %d/%d [100.0%%]" % (count, n_total)

            self.mark_as_done(date_string)
            print (">>> date_string %s done, %d docs, %.2f mins, %.2f mins total" %
                    (date_string, count,
                    (time.time()-mystart)/60.,
                    (time.time()-starttime)/60.))


def main(args):
    est = ESTransferByTimestamp(args=args)
    est.run()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('date_strings', metavar='date_strings', type=str, nargs='+',
                        help='Transfer these days')
    parser.add_argument("--checkpoint_file", default='checkpoint.dat',
                        type=str, dest="checkpoint_file",
                        help="Processed the date_strings from this file [default: %(default)s]")
    parser.add_argument("--es_buffer_size", default=5000,
                        type=int, dest="es_buffer_size",
                        help="Buffer size for elasticsearch scan [default: %(default)s]")
    parser.add_argument("--amq_buffer_size", default=1000,
                        type=int, dest="amq_buffer_size",
                        help="Buffer size for AMQ upload [default: %(default)s]")

    parser.add_argument("--dry_run", action='store_true',
                        dest="dry_run",
                        help="Don't do anything")

    args = parser.parse_args()

    main(args)
