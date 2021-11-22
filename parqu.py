#!/usr/bin/env python3
"""
Description:
    Extracts metadata from parquet file(s) in the given path

Usage:
    
    parque.py <path> [--inc=*.parquet] [--details=n] [--pool=8] [--log=DEBUG] [--recurse] [--help]

Options:
    -h --help      Show this screen
    --inc=PATTERN  Find files matching the glob patterns [default: *.parquet]
    --recurse      Recursively find files matching <glob> starting from <path>
    --details=n    Print extra details 0,1,2 [default: 1]
    --pool=<n>     Spin up n processes to process in parallel [default: 8]. 
    --log=<lvl>    Log level: choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL") [default: ERROR]

"""

import multiprocessing
from pyarrow import fs as FS
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid
from logzero import logger
import json
import fnmatch
from multiprocessing import Pool


# https://parquet.apache.org/documentation/latest/


def simple_schema(file_meta: pq.FileMetaData) -> dict:
    """Returns the simple schema for given parquet file"""

    columns = [
        {
            "field_name": c.name,
            "physical_type": str(c.physical_type),
            "logical_type": str(c.logical_type),
        }
        for c in file_meta.schema
    ]

    result = {
        "format_version": file_meta.format_version,
        "created_by": file_meta.created_by,
        "num_columns": file_meta.num_columns,
        "num_rows": file_meta.num_rows,
        "num_row_groups": file_meta.num_row_groups,
        "schema": columns,
    }
    return result


def get_metadata(FS: pq.FileSystem, path: str, details: int) -> dict:
    """Returns the schema of parquet file

    If details is 0 it will return the extensive/exhaustive details
    """

    result = {"status": "Error", "file_name": path, "file_size": 0}
    with FS.open_input_file(path) as f:
        logger.debug(f"Opening {path}")
        try:
            result["file_size"] = f.size()
            # set the size
            if details == 0:
                # checks the file - dont care to process
                _ = pq.ParquetFile(f).metadata
            elif details == 1:
                result["meta_data"] = simple_schema(pq.ParquetFile(f).metadata)
            elif details == 2:
                result["meta_data"] = byte_me(pq.ParquetFile(f).metadata.to_dict())
            else:
                logger.error("Unknown option for details")
                # raise (ValueError)
            # if we haven't got an exeception, set status to be OK
            result["status"] = "OK"
        except ArrowInvalid as e:
            logger.error(f"Cannot process [{path}] - invalid format?")
            # logger.exception(e)
    return result


def byte_me(data, hex=True):
    """recursively convert bytes into either hex or utf-8 strings"""
    # some parquet files have bytes in schema/descriptions
    # some metadata statistics contain bytes - # conver to hex for easier reading
    if isinstance(data, dict):
        return dict([byte_me(d, hex) for d in data.items()])
    if isinstance(data, bytes):
        return data.hex() if hex else data.decode("utf-8")
    if isinstance(data, tuple):
        return [byte_me(d, hex) for d in data]
    if isinstance(data, list):
        return [byte_me(d, hex) for d in data]
    return str(data)


def get_filelist(input_path: str, pattern="*.parquet", recurse=False):
    """Returns a FileSystem object and a list of FileInfo objects that matches the pattern"""
    fs, path = FS.FileSystem.from_uri(input_path)

    # pretend path is an array makes the logic for single file easier
    fileinfo = fs.get_file_info([path])
    if (
        fileinfo[0].type == FS.FileType.NotFound
        or fileinfo[0].type == FS.FileType.Unknown
    ):
        logger.error("File not found or unknown file type")
        raise (FileNotFoundError)

    if fileinfo[0].type == FS.FileType.File:
        logger.info(f"Checking a single file: {fileinfo[0].path}")
        pass
    elif fileinfo[0].type == FS.FileType.Directory:
        fileinfo = fs.get_file_info(FS.FileSelector(path, recursive=recurse))
        fileinfo = [f for f in fileinfo if fnmatch.fnmatch(f.base_name, pattern)]

    logger.debug(
        f"Finished collecting {len(fileinfo)} objects from {fs.type_name} file system"
    )
    return fs, fileinfo


def main(args):
    logger.setLevel(args.log)
    FS, objs = get_filelist(args.path, pattern=args.inc, recurse=args.recurse)

    pool = int(args.pool)
    details = int(args.details)

    workers = pool if pool > 1 else (multiprocessing.cpu_count() * 2 - 1)
    logger.debug(f"Number of workers set to : {workers}")
    params = zip([FS] * len(objs), [o.path for o in objs], [details] * len(objs))

    with Pool(workers) as p:
        results = p.starmap(get_metadata, params)
        print(json.dumps(results, indent=4, sort_keys=True))


if __name__ == "__main__":
    try:
        from docopt import docopt, DocoptExit

        args = docopt(__doc__)
    except DocoptExit as e:
        exit(__doc__)

    main(args)
