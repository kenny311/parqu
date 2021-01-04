#!/usr/bin/env python3
from pyarrow import fs as FS
import argparse
import pyarrow.parquet as pq
import json
import logging as logger
from multiprocessing import Pool
import jmespath
import tracemalloc


# https://parquet.apache.org/documentation/latest/

# !todo: possible bug there seems f.size issue... 
# if that is not returned properly, its not going to get the 
# parquet footer / metadata


def byte_me(data, hex=True):
    # some parquet files has bytes in schema/descriptions
    # some metadata statistics contain bytes - # conver to hex for easier reading
    if isinstance(data, dict):
        return dict([byte_me(d, hex) for d in data.items()])
    if isinstance(data, bytes):
        return data.hex() if hex else data.decode('utf-8')
    if isinstance(data, tuple):
        return [byte_me(d, hex) for d in data]
    if isinstance(data, list):
        return [byte_me(d, hex) for d in data]
    return str(data)


def get_filelist(input_path: str, pattern=".parquet", recurse=False):
    """Return filesystem type list of files"""

    logger.debug(input_path)
    fs, path = FS.FileSystem.from_uri(input_path)
    fileinfo = fs.get_file_info([path])

    if fileinfo[0].type == FS.FileType.NotFound:
        logger.error("File or object not found")
        exit()

    if fileinfo[0].type == FS.FileType.File:
        return fs, fileinfo

    if fileinfo[0].type == FS.FileType.Directory:
        fileinfo = fs.get_file_info(FS.FileSelector(path, recursive=recurse))
        fileinfo = [f for f in fileinfo if pattern in f.path]

    logger.info(
        f"finished collecting {len(fileinfo)} objects from {fs.type_name} file system")
    return fs, fileinfo


def meta2dict(meta, details=False):
    # schema = schema2dict(meta.schema)
    s = meta.schema
    columns = []
    for i in range(len(s)):
        columns.append({'field_name': s.column(i).name, 'physical_type': str(
            s.column(i).physical_type), 'logical_type': str(s.column(i).logical_type)})

    result = {
        'format_version': meta.format_version,
        'created_by': meta.created_by,
        'num_columns': meta.num_columns,
        'num_rows': meta.num_rows,
        'num_row_groups': meta.num_row_groups,
        'schema': columns,
    }

    return result


def get_object_metadata(path) -> dict:
    """
    returns (path, status, schema and meta data)
    """
    f = FTYPE.open_input_file(path)

    result = {'status': 'Error', 'file_name': path,
              'file_size': f.size(), 'meta_data': None}

    if f.size() == 0: 
        logger.error(f"Problem with {path}")
        return '|'.join([result['status'], result['file_name'], str(result['file_size'])])

    try:
        # meta = meta2dict(pq.ParquetFile(f).metadata)
        # result['meta_data'] = meta
        # result['status'] = "OK"
        # # if DETAILS:
        #     result['details_meta'] = byte_me(
        #         pq.ParquetFile(f).metadata.to_dict())
        _ = pq.ParquetFile(f).metadata
        result['status'] = "OK"
    except Exception as e:
        logger.error(f"Problem with {path}")
        logger.exception(e)
    finally:
        f.close()
        return '|'.join([result['status'], result['file_name'], str(result['file_size'])])

    # return result


def dump_metadata(objs: dict, args):
    simple = '[status, file_name, file_size, meta_data.[format_version,created_by,num_rows, num_columns, schema[].[field_name,physical_type,logical_type][]][]][]'
    check_only = '[status, file_name, file_size ]'

    with Pool(NPOOL) as p:
        # for path, stat, meta, schema in p.imap(get_object_metadata, objs.keys()):
        for t in p.imap(get_object_metadata, objs.keys()):
            if args.check:
                # pass
                # print('|'.join([t['status'],t['file_name'],str(t['file_size'])]))
                # line = jmespath.search(check_only, t)
                print(t)
            elif args.simple:
                line = jmespath.search(simple, t)
                print('|'.join([str(l) for l in line]))
            else:
                print(json.dumps(t, indent=4))


def main(args):
    global FTYPE, NPOOL, DETAILS
    NPOOL = args.npool
    DETAILS = args.details
    path = args.path

    FTYPE, objs = get_filelist(path, pattern=args.pat, recurse=args.recurse)

    objs = {obj.path: obj for obj in objs}

    dump_metadata(objs, args)


if __name__ == "__main__":
    """ process to extract metadata from parquet file(s).  If a directory is given
    it will attempt to go through all the files in the directory - recursively.  
    Make sure you specific the path at the right level as it will recursively go thru the directories.
    This works on local, S3 and HDFS paths.  
    """

    FTYPE = ''
    NPOOL = 16
    DETAILS = False

    parser = argparse.ArgumentParser(
        description='Extract metadata from parquet files')
    parser.add_argument(
        "--path",  help="directory or file name", required=True)
    parser.add_argument(
        "--simple",  help="simplifed output; one line per parquet file", action="store_true", required=False)
    parser.add_argument(
        "--recurse",  help="simplifed output; one line per parquet file", action="store_true", default=False, required=False)
    parser.add_argument(
        "--pat",  help="extension or pattern of the filename (no regex match)", default=".parquet", required=False)
    parser.add_argument(
        "--check", help="check that the file is ok by reading the metadata", action="store_true", default=False, required=False)
    parser.add_argument(
        "--npool", help="number of workers to start for this run", type=int, default=16, required=False)
    parser.add_argument(
        "--details",  help="lots of details for the parquet file", action="store_true", default=False, required=False)

    args = parser.parse_args()
    logger.basicConfig(level=logger.INFO)
    main(args)
