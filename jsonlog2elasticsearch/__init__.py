#!/usr/bin/env python3

import os
import json
import argparse
import pygtail
import elasticsearch
import functools
import elasticsearch.helpers
import importlib
import datetime
import dateutil.parser
import signal
import pytz
import time
import sys
from pathlib import Path
from mflog import get_logger, set_config

DESCRIPTION = "daemon to send json logs read from a file to an " \
    "elasticsearch instance"

LOG = get_logger("jsonlog2elasticsearch")
RUNNING = True
SLEEP_AFTER_EACH_ITERATION = 3
TO_SEND = []


# Thanks to https://stackoverflow.com/questions/312443/
#     how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def signal_handler(signum, frame):
    global RUNNING
    LOG.info("Signal: %i handled", signum)
    RUNNING = False


def process(line, transform_func, index_func):
    if line is None:
        return False
    tmp = line.strip()
    if len(tmp) == 0:
        return False
    try:
        decoded = json.loads(tmp)
    except Exception:
        LOG.warning("can't decode the line: %s as JSON" % tmp)
        return False
    if not isinstance(decoded, dict):
        LOG.warning("the decoded line: %s must be a JSON dict" % tmp)
        return False
    index = index_func(decoded)
    transformed = transform_func(decoded)
    if transformed is None:
        return False
    if not isinstance(transformed, dict):
        LOG.warning("the transformed function must return a dict (or None)"
                    ", got: %s" % type(transformed))
        return False
    TO_SEND.append({
        "_op_type": "index",
        "_index": index,
        "_type": "_doc",
        "_source": transformed
    })
    return True


def commit(es, force=False):
    global TO_SEND
    if not(force) and len(TO_SEND) < 500:
        return False
    if len(TO_SEND) == 0:
        return False
    LOG.info("commiting %i line(s) to ES..." % len(TO_SEND))
    for chunk in chunks(TO_SEND, 500):
        res = elasticsearch.helpers.bulk(es, chunk, stats_only=False,
                                         raise_on_error=False)
        if res[0] != len(chunk):
            LOG.warning("can't post %i lines to ES" % (len(chunk) - res[0]))
            LOG.debug("raw output: %s" % res[1])
    TO_SEND = []
    return True


def patched_is_new_file(self):
    size1 = self._filehandle().tell()
    size2 = os.fstat(self._filehandle().fileno()).st_size
    inode1 = os.fstat(self._filehandle().fileno()).st_ino
    inode2 = os.stat(self.filename).st_ino
    return self._rotated_logfile or (size1 == size2 and inode1 != inode2) or \
        (inode1 == inode2 and size1 > size2)


def patched_update_offset_file(self):
    return


def touch(filepath):
    LOG.info("The file: %s does not exist => let's touch it" % filepath)
    Path(filepath).touch()


def no_transform(dict_object):
    return dict_object


def _get_func(func_label, func_path):
    func_name = func_path.split('.')[-1]
    module_path = ".".join(func_path.split('.')[0:-1])
    if module_path == "":
        LOG.error("%s must follow 'pkg.function_name'" % func_label)
        sys.exit(1)
    if func_path.endswith(')'):
        LOG.error("%s must follow 'pkg.function_name'" % func_label)
        sys.exit(1)
    mod = importlib.import_module(module_path)
    return getattr(mod, func_name)


def get_transform_func(func_path):
    return _get_func("transform-func", func_path)


def get_index_func(func_path):
    return _get_func("transform-func", func_path)


def default_index_func(index_const_value, dict_object):
    if "@timestamp" in dict_object:
        ref = dateutil.parser.parse(dict_object["@timestamp"])
        ref_utc = ref.replace(tzinfo=pytz.utc) - ref.utcoffset()
    else:
        ref_utc = datetime.datetime.utcnow()
    return ref_utc.strftime(index_const_value)


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--transform-func",
                        default="jsonlog2elasticsearch.no_transform",
                        help="python function to transform each json line")
    parser.add_argument("--debug",
                        action="store_true",
                        help="force debug mode")
    parser.add_argument("--index-override-func",
                        default="no",
                        help="python function to override the ES_INDEX value")
    parser.add_argument("ES_HOST", help="ES hostname/ip")
    parser.add_argument("ES_PORT", help="ES port", type=int)
    parser.add_argument("ES_INDEX", help="ES index name", type=str)
    parser.add_argument("LOG_PATH", help="json log file fullpath")
    args = parser.parse_args()
    if args.debug:
        set_config(minimal_level="DEBUG")
    if not os.path.isfile(args.LOG_PATH):
        touch(args.LOG_PATH)
    transform_func = get_transform_func(args.transform_func)
    if args.index_override_func == "no":
        index_func = functools.partial(default_index_func, args.ES_INDEX)
    else:
        index_func = get_index_func(args.index_override_func)
    pygtail.Pygtail._is_new_file = patched_is_new_file
    pygtail.Pygtail._update_offset_file = patched_update_offset_file
    pt = pygtail.Pygtail(filename=args.LOG_PATH, read_from_end=True)
    es = elasticsearch.Elasticsearch(hosts=[{
        "host": args.ES_HOST,
        "port": args.ES_PORT,
        "use_ssl": False
    }])
    signal.signal(signal.SIGTERM, signal_handler)
    LOG.info("started")
    while RUNNING:
        while True:
            try:
                line = pt.next()
            except StopIteration:
                break
            except FileNotFoundError:
                touch(args.LOG_PATH)
                break
            else:
                if process(line, transform_func, index_func):
                    commit(es)
        commit(es, True)
        LOG.debug("sleeping %i seconds..." % SLEEP_AFTER_EACH_ITERATION)
        time.sleep(SLEEP_AFTER_EACH_ITERATION)
    LOG.info("exited")


if __name__ == "__main__":
    main()
