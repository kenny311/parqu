#!/usr/bin/env python3
import multiprocessing
from pyarrow import fs as FS
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid
import argparse
import json
import logging as logger
# from logzero import logger
import fnmatch
from multiprocessing import Pool

# https://parquet.apache.org/documentation/latest/


class Parqu():
    def __init__(self, FS, path, details=False) -> None:
        self.path = path
        self.FS = FS
        self.details = details

    @staticmethod
    def simple_schema(file_meta: pq.FileMetaData) -> dict:
        """Returns the simple schema for given parquet file"""

        columns = [{'field_name': c.name, 'physical_type': str(
            c.physical_type), 'logical_type': str(c.logical_type)} for c in file_meta.schema]

        result = {
            'format_version': file_meta.format_version,
            'created_by': file_meta.created_by,
            'num_columns': file_meta.num_columns,
            'num_rows': file_meta.num_rows,
            'num_row_groups': file_meta.num_row_groups,
            'schema': columns,
        }
        return result

    @staticmethod
    def byte_me(data, hex=True):
        """recursively convert bytes into either hex or utf-8 strings"""
        # some parquet files have bytes in schema/descriptions
        # some metadata statistics contain bytes - # conver to hex for easier reading
        if isinstance(data, dict):
            return dict([Parqu.byte_me(d, hex) for d in data.items()])
        if isinstance(data, bytes):
            return data.hex() if hex else data.decode('utf-8')
        if isinstance(data, tuple):
            return [Parqu.byte_me(d, hex) for d in data]
        if isinstance(data, list):
            return [Parqu.byte_me(d, hex) for d in data]
        return str(data)

    @staticmethod
    def _get_metadata(FS: pq.FileSystem, path: str, details: int) -> dict:
        """Returns the schema of parquet file

        If details is 0 it will return the extensive/exhaustive details
        """

        result = {'status': 'Error', 'file_name': path, 'file_size': 0}
        with FS.open_input_file(path) as f:
            logger.debug(f"Opening {path}")
            try:
                result['file_size'] = f.size()
                # set the size
                if details == 0:
                    # checks the file - dont care to process
                    _ = pq.ParquetFile(f).metadata
                elif details == 1:
                    result['meta_data'] = Parqu.simple_schema(
                        pq.ParquetFile(f).metadata)
                elif details == 2:
                    result['meta_data'] = Parqu.byte_me(
                        pq.ParquetFile(f).metadata.to_dict())
                else:
                    logger.critical("Unknown details option")
                    raise(ValueError)
                # if we haven't got an exeception, set status to be OK
                result['status'] = "OK"
            except ArrowInvalid as e:
                logger.error(f"Cannot process [{path}] - invalid format?")
                # logger.exception(e)
        return result

    @property
    def meta_data(self) -> dict:
        return Parqu._get_metadata(self.FS, self.path, self.details)

    @staticmethod
    def get_filelist(input_path: str, pattern="*.parquet", recurse=False):
        """Returns a FileSystem object and a list of FileInfo objects that matches the pattern"""
        fs, path = FS.FileSystem.from_uri(input_path)

        fileinfo = fs.get_file_info([path])
        if fileinfo[0].type == FS.FileType.NotFound or fileinfo[0].type == FS.FileType.Unknown:
            logger.error("File not found or unknown file type")
            raise(FileNotFoundError)

        if fileinfo[0].type == FS.FileType.File:
            logger.debug(f"Checking a single file: {fileinfo[0].path}")
            pass
        elif fileinfo[0].type == FS.FileType.Directory:
            fileinfo = fs.get_file_info(
                FS.FileSelector(path, recursive=recurse))
            fileinfo = [f for f in fileinfo if fnmatch.fnmatch(
                f.base_name, pattern)]

        logger.debug(
            f"Finished collecting {len(fileinfo)} objects from {fs.type_name} file system")
        return fs, fileinfo


def pool_get_meta(FS, path, details):
    f = Parqu(FS, path=path, details=details)
    return f.meta_data


def main(args):
    FS, objs = Parqu.get_filelist(
        args.path, pattern=args.pat, recurse=args.recurse)

    workers = args.pool if args.pool >= 1 else (
        multiprocessing.cpu_count() * 2 - 1)
    logger.debug(f"Number of workers set to : {workers}")
    params = zip([FS] * len(objs), [o.path for o in objs],
                 [args.details]*len(objs))

    with Pool(workers) as p:
        results = p.starmap(pool_get_meta, params)
        print(json.dumps(results, indent=4, sort_keys=True))


if __name__ == "__main__":
    """ Extracts metadata from parquet file(s).  

    If a directory is given, will process files in the directory - recursively if specified.  

    This works on local, S3 and HDFS paths.
    """

    parser = argparse.ArgumentParser(
        description='Extract metadata from parquet files')
    parser.add_argument(
        "--path",  help="directory or file name", required=True)
    parser.add_argument(
        "--pat",  help="unix style glob for filenames; applicable only if --path is a directory.  Default to '*.parquet'", default="*.parquet", required=False)
    parser.add_argument(
        "--recurse",  help="recursivly list the directory path to find files", action="store_true", default=False, required=False)
    parser.add_argument(
        "--pool", help="number of concurrent workers for the run; defaults to number of 2 x CPUs - 1", type=int, default=-1, required=False)
    parser.add_argument(
        "--details",  help="0:file check; 1:basic schema dump [default], 2:exhaustive dump", type=int, choices=range(0, 3), required=False, default=1)
    parser.add_argument(
        "--log",  help="log level; defaults to INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], required=False, default="INFO")

    args = parser.parse_args()
    loglevel = getattr(logger, args.log.upper())
    logger.basicConfig(level=loglevel)
    main(args)
