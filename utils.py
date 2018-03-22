import config
import os
import argparse
import datetime
import pandas as pd
import numpy as np
import traceback

SCRIPT_DIR = os.path.abspath(__file__)

## read file and return pd dataframe
## if lang is EN or file it too large read w/ iterator
def read_wiki_edits_file(infile,lang=None):
    if lang == 'en':
        tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
        df = pd.concat(tp, ignore_index=True)
    else:
        try:
            df = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object})
        except MemoryError:
            log('file too large, importing with iterator...')
            tp = pd.read_csv(infile,na_values={'title':''},keep_default_na=False,dtype={'title': object},iterator=True,chunksize=1000)
            df = pd.concat(tp, ignore_index=True)
    return df