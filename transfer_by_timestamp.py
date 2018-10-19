#!/usr/bin/env python
import os
import time
import json
import multiprocessing

from argparse import ArgumentParser

from dump_es_bytimestamp import make_query
from dump_es_bytimestamp import get_es_scan
from dump_es_bytimestamp import get_total_hits
from dump_es_bytimestamp import date_string_to_timestamp
from dump_es_bytimestamp import get_es_scan_sliced
from dump_es_bytimestamp import get_total_hits_sliced

from amq import post_ads
from transfer_helpers import print_progress
from transfer_helpers import convert_dates_to_millisecs
from transfer_helpers import read_es_config
from transfer_helpers import get_total_lines


def es_query_worker(query, query_queue, buffer_size, n_total):
    """
    Do an ES scan for a given query and feed the
    resulting docs into the queue
    """
    count = 0
    for raw_doc in get_es_scan(query, buffer_size=buffer_size):
        try:
            doc = raw_doc['_source']
        except ValueError, e:
            print "&&& ERROR: Failed to parse doc from line in raw data!"
            print str(doc[:200])
            raise e

        query_queue.put(doc)
        count += 1

    query_queue.put(None) # send poison pill
    assert(count == n_total), "Inconsistent count (query worker)"


def es_query_worker_sliced(query, slice_id, max_slices, query_queue, buffer_size):
    """
    Do an ES scan for a given query and feed the
    resulting docs into the queue
    """
    n_total_in_slice = get_total_hits_sliced(query, slice_id, max_slices)
    count = 0
    for raw_doc in get_es_scan_sliced(query, slice_id,
                                      max_slices=max_slices,
                                      buffer_size=buffer_size)():
        try:
            doc = raw_doc['_source']
        except ValueError, e:
            print "&&& ERROR: Failed to parse doc from line in raw data!"
            print str(doc[:200])
            raise e

        query_queue.put(doc)
        count += 1

    query_queue.put(None) # send poison pill
    assert(count == n_total_in_slice), "Inconsistent count (sliced query worker)"


def file_read_worker(filename, query_queue, n_total):
    query_queue.put(n_total) # first put the total expected

    count = 0
    with open(filename, "r") as dumpfile:
        for line in dumpfile:
            raw_doc = json.loads(line)
            try:
                doc = raw_doc['_source']
            except ValueError, e:
                print "&&& ERROR: Failed to parse doc from line in raw data!"
                print str(doc[:200])
                raise e

            query_queue.put(doc)
            count += 1

    query_queue.put(None) # send poison pill
    assert(count == n_total), "Inconsistent count (query worker)"


def amq_upload_worker(query_queue, batch_size=5000, dry_run=False, max_slices=1):
    batch = []
    count_in = 0
    count_out = 0
    n_pills_swallowed = 0
    n_total = query_queue.get() # first get total expected

    while True:
        doc = query_queue.get()
        if doc == None: # swallow poison pills
            n_pills_swallowed += 1
            if n_pills_swallowed == max_slices:
                break

            continue

        batch.append(doc)
        count_in += 1
        if len(batch) == batch_size:
            count_out += upload_batch(batch, dry_run=dry_run)
            batch = []

        if count_in % 250 == 0:
            print_progress(count_in, n_total)


    if batch:
        count_out += upload_batch(batch, dry_run=dry_run)
        batch = []
    print ">>> Processed {}/{} [{:.1%}]".format(count_in, n_total, count_in/float(n_total))

    assert(count_in == count_out == n_total), "Inconsistent count (upload worker)"


def upload_batch(batch, dry_run=False):
    data = ((d['GlobalJobId'], convert_dates_to_millisecs(d)) for d in batch)
    n_sent = post_ads(data, dry_run)
    assert(n_sent == len(batch)), "Inconsistent count (batch uploader)"
    return n_sent


def process_date_string(date_string, args):
    starttime = time.time()

    mp_manager = multiprocessing.Manager()
    query_queue = mp_manager.Queue(maxsize=10000)


    print ">>> Processing %s" % date_string
    timestamp = date_string_to_timestamp(date_string)
    if not timestamp:
        print 'Invalid date "%s", skipping' % date_string
        return

    query = make_query(timestamp, timestamp + 24*60*60)


    processes = []
    if args.streaming:
        print "    Streaming from ES"    
        n_total = get_total_hits(query)
        query_queue.put(n_total) # first put the total expected

        if args.es_slices == 1:
            qproc = multiprocessing.Process(target=es_query_worker,
                                            args=(query, query_queue, args.es_buffer_size, n_total),
                                            name="es_query_worker")
            qproc.start()
            processes.append(qproc)

        else:
            print "      processing %d slices in parallel" % args.es_slices

            for slice_id in range(args.es_slices):
                qproc = multiprocessing.Process(target=es_query_worker_sliced,
                                                args=(query, slice_id, args.es_slices,
                                                      query_queue, args.es_buffer_size),
                                                name="es_query_worker_sliced_%d" % slice_id)
                qproc.start()
                processes.append(qproc)


    else:
        dumpfile = os.path.join(args.dump_location, 'es-cms-dump-%s.json' % date_string)
        if not os.path.isfile(dumpfile):
            print 'Dumpfile not found: %s, skipping' % dumpfile
            return
        print "    Reading from %s" % dumpfile
        n_total = get_total_lines(dumpfile)
        read_proc =  multiprocessing.Process(target=file_read_worker,
                                             args=(dumpfile, query_queue, n_total),
                                             name="file_read_worker")
        read_proc.start()
        processes.append(read_proc)

    upload_proc = multiprocessing.Process(target=amq_upload_worker,
                                          args=(query_queue,
                                                args.amq_buffer_size,
                                                args.dry_run,
                                                args.es_slices),
                                          name='amq_upload_worker')
    upload_proc.start()
    processes.append(upload_proc)

    for p in processes:
        p.join()

    print ">>> %s done in %.2f mins" % (date_string, (time.time()-starttime)/60.)


_checkpoint = None
def load_checkpoint(checkpoint_file):
    global _checkpoint
    if not _checkpoint:
        try:
            with open(checkpoint_file, 'r') as chkpfile:
                _checkpoint = [l.strip() for l in chkpfile]
        except IOError, e:
            _checkpoint = []

        # Make sure there are no duplicates
        assert(len(_checkpoint) == len(set(_checkpoint))), "Duplicates in checkpoint file"


def mark_as_done(date_string, checkpoint_file):
    global _checkpoint
    _checkpoint.append(date_string)
    with open(checkpoint_file, 'a') as chkpfile:
        chkpfile.write('%s\n' % date_string)


def main(args):
    load_checkpoint(args.checkpoint_file)
    for date_string in args.date_strings:
        if date_string in _checkpoint:
            print "%s already done, skipping..." % date_string
            continue

        process_date_string(date_string, args)

        if not args.dry_run:
            mark_as_done(date_string, args.checkpoint_file)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('date_strings', metavar='date_strings', type=str, nargs='+',
                        help='Transfer these days')
    parser.add_argument("--streaming", action='store_true',
                        dest="streaming",
                        help="Read directly from ES rather than from a dump file")
    parser.add_argument("--es_slices", default=1,
                        type=int, dest="es_slices",
                        help="Number of slices to be scanned in parallel [default: %(default)s]")
    parser.add_argument("--dump_location", default='/data/raw_index_data/',
                        type=str, dest="dump_location",
                        help="Directory to look for file dumps [default: %(default)s]")
    parser.add_argument("--checkpoint_file", default='checkpoint.dat',
                        type=str, dest="checkpoint_file",
                        help="Processed the date_strings from this file [default: %(default)s]")

    parser.add_argument("--es_buffer_size", default=5000,
                        type=int, dest="es_buffer_size",
                        help="Buffer size for elasticsearch scan [default: %(default)s]")
    parser.add_argument("--amq_buffer_size", default=5000,
                        type=int, dest="amq_buffer_size",
                        help="Buffer size for AMQ upload [default: %(default)s]")
    parser.add_argument("--queue_size", default=10000,
                        type=int, dest="queue_size",
                        help="Size of internal queue [default: %(default)s]")

    parser.add_argument("--dry_run", action='store_true',
                        dest="dry_run",
                        help="Don't do anything")

    args = parser.parse_args()

    main(args)
