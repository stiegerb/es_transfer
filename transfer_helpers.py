#!/usr/bin/env python
import os
import sys
import shlex
import logging
import subprocess

from logging.handlers import RotatingFileHandler


date_vals = set([ \
    "CompletionDate",
    "EnteredCurrentStatus",
    "JobCurrentStartDate",
    "JobCurrentStartExecutingDate",
    "JobCurrentStartTransferOutputDate",
    "JobLastStartDate",
    "JobStartDate",
    "LastMatchTime",
    "LastSuspensionTime",
    "LastVacateTime_RAW",
    "QDate",
    "ShadowBday",
    "StageInFinish",
    "StageInStart",
    "JobFinishedHookDone",
    "LastJobLeaseRenewal",
    "LastRemoteStatusUpdate",
    "GLIDEIN_ToDie",
    "GLIDEIN_ToRetire",
    "DataCollectionDate",
    "RecordTime",
    "ChirpCMSSWLastUpdate",
])


def convert_dates_to_millisecs(record):
    for date_field in date_vals:
        try:
            record[date_field] *= 1000
        except (KeyError, TypeError): continue

    return record


def read_es_config(filename="es.conf"):
    es_conf = {}
    with open(filename, "r") as conf:
        for line in conf:
            key, val = line.split(":")
            key = key.strip().lower()
            if key in ['user', 'pass', 'host']:
                val = str(val.strip())
            if key == 'port':
                val = int(val.strip())

            es_conf[key] = val

    return es_conf


def free_diskspace(path):
    res = os.statvfs(path)
    return res.f_bavail * res.f_frsize


def set_up_logging(log_dir='log/'):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)

    if not os.path.isdir(log_dir):
        os.system('mkdir -p %s' % log_dir)

    log_file = os.path.join(log_dir, 'spider_cms.log')
    filehandler = RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter('%(asctime)s : %(name)s:%(levelname)s - %(message)s'))

    logger.addHandler(filehandler)


def print_progress(current, total):
    sys.stdout.write(">>> Processed {}/{} [{:.1%}]\r".format(
                    current, total,
                    current/float(total)))
    sys.stdout.flush()


def get_total_lines(filename):
    cmd = "wc -l %s" % filename
    result = subprocess.check_output(shlex.split(cmd))

    try:
        count = int(result.split()[0])
    except ValueError:
        count = None
    return count

