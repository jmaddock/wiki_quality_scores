import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import os
import logging

INPUT_COLUMNS = [
    'page_id', 'namespace', 'title', 'archive', 'user_text', 'revert', 'ts', 'quality_change', 'new_quality_scores',
    'max_quality', 'parse_error', 'deleted_text'
]

NA_VALUES = {
    'title': [''],
    'archive': ['None'],
    'user_text': [''],
    'user_id': ['None'],
    'revert': ['None'],
    'quality_change': ['None'],
    'new_quality_scores': ['None'],
    'min_quality': ['None'],
    'mean_quality': ['None'],
    'max_quality': ['None'],
}

DTYPES = {
    'page_id':np.int64,
    'namespace':np.int64,
    'title': object,
    'archive': object,
    'user_text': object,
    'user_id': np.float64,
    'revert': object,
    'ts': object,
    'quality_change': np.float64,
    'new_quality_scores': np.float64,
    'min_quality': np.float64,
    'mean_quality': np.float64,
    'max_quality': np.float64,
    'parse_error': bool,
    'deleted_text': bool
}

class ThreadedCSVLoader(object):

    def __init__(self, num_workers, **kwargs):
        self.num_workers = num_workers
        self.infile_list = kwargs.get('infile_list', [])
        self.logger = None

    def get_file_list(self, indir):
        self.infile_list = [os.path.join(indir, f) for f in os.listdir(indir) if f[-4:] == '.csv']
        if self.logger:
            self.logger.debug(self.infile_list)
            self.logger.info('found {0} files in {1}'.format(len(self.infile_list), indir))

    def load_raw_edit_file(self, infile):
        if self.logger:
            self.logger.info('loading data from file {0}'.format(infile))

        df = pd.read_csv(infile,
                         na_values=NA_VALUES,
                         keep_default_na=False,
                         dtype=DTYPES,
                         usecols=INPUT_COLUMNS)
        return df

    def multiprocess_load(self):
        if self.logger:
            self.logger.info('loading {0} files with {1} workers'.format(len(self.infile_list), self.num_workers))
        executor = ProcessPoolExecutor(max_workers=4)
        df_list = [infile for infile in executor.map(self.load_raw_edit_file, self.infile_list)]

        df = pd.concat(df_list, ignore_index=True)
        df['archive'] = df['archive'].astype("category")
        df['revert'] = df['revert'].astype("category")
        df['namespace'] = df['namespace'].astype("category")
        return df

    def singleprocess_chunk_load(self, infile):
        if self.logger:
            self.logger.info('loading data from file {0}'.format(infile))

        # if processing EN Wikipedia, automatically load w/ iterator to avoid memory errors
        tp = pd.read_csv(infile,
                         na_values=NA_VALUES,
                         keep_default_na=False,
                         dtype=DTYPES,
                         iterator=True,
                         chunksize=1000,
                         usecols=INPUT_COLUMNS)
        df = pd.concat(tp, ignore_index=True)
        df['archive'] = df['archive'].astype("category")
        df['revert'] = df['revert'].astype("category")
        df['namespace'] = df['namespace'].astype("category")
        return df