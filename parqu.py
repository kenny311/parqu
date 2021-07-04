#!/usr/bin/env python3
import multiprocessing
from pyarrow import fs as FS
import pyarrow.parquet as pq
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
    def simple_schema(file_meta: pq.FileMetaData):

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
    def _get_metadata(FS: pq.FileSystem, path: str, details: bool) -> dict:
        """returns the dict of result"""
        result = {'status': 'Error', 'file_name': path,
                  'file_size': 0, 'meta_data': None}
        with FS.open_input_file(path) as f:
            logger.debug(f"Opening {path}")
            try:
                result['file_size'] = f.size()
                result['meta_data'] = Parqu.byte_me(pq.ParquetFile(f).metadata.to_dict(
                )) if details else Parqu.simple_schema(pq.ParquetFile(f).metadata)
                result['status'] = "OK"
            except Exception as e:
                logger.error(f"Problem with {path}")
                # logger.exception(e)
        return result

    @property
    def meta_data(self) -> dict:
        return Parqu._get_metadata(self.FS, self.path, self.details)
    
    @property
    def file_status(self) -> str:
        field_names=['file_name', 'status', 'file_size']
        meta = Parqu._get_metadata(self.FS, self.path, details=False)
        values = [ str(meta[f]) for f in field_names]
        return str.join('|', values)

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

def pool_check_files(FS, path, details):
    f = Parqu(FS, path=path, details=False)
    return f.file_status

def main(args):
    FS, objs = Parqu.get_filelist(
        args.path, pattern=args.pat, recurse=args.recurse)

    workers = args.pool if args.pool >=  1 else (multiprocessing.cpu_count() * 2 -1)
    logger.info(f"Number of workers: {args.pool}")
    logger.info(f"Number of workers: {workers}")
    params = zip([FS] * len(objs), [o.path for o in objs], [args.details]*len(objs) )


    pool_func = pool_check_files if args.check else pool_get_meta
    with Pool(workers) as p:
        results = p.starmap(pool_func, params)        
        for r in results:
            print(r) if isinstance(r, str) else print(json.dumps(r, indent=4, sort_keys=True))        

if __name__ == "__main__":

    """ Extracts metadata from parquet file(s).  If a directory is given
    it will attempt to go through all the files in the directory - recursively if needed.  
    Make sure you specify the path at the right level as it will recursively go thru the directories.

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
        "--details",  help="detail info about the parquet file", action="store_true", required=False, default=False)
    parser.add_argument(
        "--check",  help="check that files are ok by reading the metadata", action="store_true", required=False, default=False)
    parser.add_argument(
        "--pool", help="number of concurrent workers for the run; defaults to number of 2 x CPUs - 1", type=int, default=-1, required=False)
    parser.add_argument(
        "--log",  help="log level; defaults to INFO", choices=["DEBUG", "INFO","WARNING","ERROR","CRITICAL"], required=False, default="INFO")

    args = parser.parse_args()
    loglevel = getattr(logger, args.log.upper())
    logger.basicConfig(level=loglevel)
    main(args)
