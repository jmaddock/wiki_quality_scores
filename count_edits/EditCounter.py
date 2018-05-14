import datetime
import pandas as pd
import numpy as np
import logging
from RawEditPreProcessor import RawEditPreProcessor
from EditCountPostProcessor import EditCountPostProcessor
from pysal.inequality import gini
import argparse

HALF_YEAR = lambda x: int((x-1)/6)

INPUT_COLUMNS = [
    'page_id', 'namespace', 'title', 'archive', 'user_text', 'revert', 'ts', 'quality_change', 'new_quality_scores',
    'max_quality', 'parse_error', 'deleted_text'
]

PROCESSED_COLUMNS = [
    'title', 'year', 'half_year', 'edit_count_0', 'cumsum_edit_count_inclusive_0', 'cumsum_edit_count_exclusive_0',
    'page_id_0', 'page_age_0', 'editor_count_0', 'cumsum_editor_count_inclusive_0',
    'cumsum_editor_count_exclusive_0', 'gini_coef_0', 'edit_count_1', 'cumsum_edit_count_inclusive_1',
    'cumsum_edit_count_exclusive_1', 'page_id_1', 'page_age_1', 'editor_count_1', 'cumsum_editor_count_inclusive_1',
    'cumsum_editor_count_exclusive_1', 'gini_coef_1', 'has_quality_assessment_1', 'max_quality_1',
    'quality_change_1', 'lang'
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

class EditCounter(object):

    def __init__(self, lang, **kwargs):
        # two letter wikipedia language version code
        self.lang = lang
        self.preprocessor = RawEditPreProcessor(**kwargs)
        self.postprocessor = EditCountPostProcessor(**kwargs)

        # some logging stuff
        self.logger = kwargs.get('logger')

        # dataframe of raw edits
        self.df = None

        # dataframe of edit counts
        self.processed_df = None

    def load_raw_edit_file(self, infile):
        if self.logger:
            self.logger.info('loading data from file {0}'.format(infile))

        # if processing EN Wikipedia, automatically load w/ iterator to avoid memory errors
        if self.lang == 'en':
            tp = pd.read_csv(infile,
                             na_values=NA_VALUES,
                             keep_default_na=False,
                             dtype=DTYPES,
                             iterator=True,
                             chunksize=1000,
                             usecols=INPUT_COLUMNS)
            df = pd.concat(tp, ignore_index=True)
        # if not EN Wikipedia, try loading w/out iterator, but fall back to iterator
        else:
            try:
                df = pd.read_csv(infile,
                                 na_values=NA_VALUES,
                                 keep_default_na=False,
                                 dtype=DTYPES,
                                 usecols=INPUT_COLUMNS)
            except MemoryError:
                if self.logger:
                    self.logger.warning('file too large, importing with iterator...')
                tp = pd.read_csv(infile,
                                 na_values=NA_VALUES,
                                 keep_default_na=False,
                                 dtype=NA_VALUES,
                                 iterator=True,
                                 chunksize=1000,
                                 usecols=INPUT_COLUMNS)
                df = pd.concat(tp, ignore_index=True)

        df['archive'] = df['archive'].astype("category")
        df['revert'] = df['revert'].astype("category")
        df['namespace'] = df['namespace'].astype("category")
        self.df = df[INPUT_COLUMNS]

    def write_to_file(self, outfile):
        self.processed_df.to_csv(outfile)
        if self.logger:
            self.logger.info('wrote file to {0}'.format(outfile))

    def process(self):
        MERGE_ON = ['title', 'namespace', 'year', 'half_year']

        # preprocess the raw edit dataframe
        self.df = self.preprocessor.preprocess(self.df)

        # calculate the edit counts for each page
        result = self.count_edits(self.df)
        # calculate the age of a given page
        age = self.page_age(self.df)
        # calculate the number of editors that have contributed to a give page
        editors = self.num_editors(self.df)
        # calcuate the gini coefficient for each page
        gini = self.calculate_editor_gini_coef(self.df)
        # propagate quality scores
        quality_scores = self.propagate_quality_scores(self.df)
        # merge the editor and age columns w/ the result df
        result = result.merge(age, on=MERGE_ON, how='outer').merge(editors, on=MERGE_ON, how='outer').merge(gini, on=MERGE_ON, how='outer').merge(quality_scores, on=MERGE_ON, how='outer')
        # link articles and talk pages
        result = self.link_articles_and_talk(result)
        # add language column
        result['lang'] = self.lang
        # post processing
        result = self.postprocessor.post_process(result)
        self.processed_df = result[PROCESSED_COLUMNS]

    def link_articles_and_talk(self, df):
        if self.logger:
            self.logger.info('linking articles and talk pages')
        df1 = df.loc[df['namespace'] == 1]
        df0 = df.loc[df['namespace'] == 0]
        result = df0.merge(df1, on=['title','year','half_year'], suffixes=('_0','_1'), how='outer')
        return result

    # reduce edit csv to page level csv counting edits
    def count_edits(self, df):
        if self.logger:
            self.logger.info('calculating edit counts')
        # drop all duplicate IDs
        result = df[['page_id', 'title', 'namespace']].drop_duplicates(subset='page_id')
        # group df by namespace, title, year, and whether the edit occured in first or second half of the year
        # count occurrences
        # convert to dataframe
        # move namespace, title, year, and half year from index to column
        group_by_columns = ['title', 'namespace', df['ts'].dt.year, df['ts'].dt.month.apply(HALF_YEAR)]
        count_df = df.groupby(group_by_columns).size().to_frame('edit_count')
        count_df.index.names = ['title', 'namespace', 'year', 'half_year']
        # reindex and fill missing values w/ 0s
        count_df = count_df.reindex(pd.MultiIndex.from_product(count_df.index.levels, names=count_df.index.names), fill_value=0).reset_index()
        # get the cumulative sum for each time bin (total number of edits up through the end of the time bin)
        count_df = count_df.merge(
            count_df.groupby(['title', 'namespace'])['edit_count'].cumsum().to_frame('cumsum_edit_count_inclusive'),
            right_index=True, left_index=True)
        # get the cumulative sum excluding the current time bin
        count_df = count_df.merge(
            count_df['cumsum_edit_count_inclusive'].subtract(count_df['edit_count']).to_frame('cumsum_edit_count_exclusive'),
            right_index=True, left_index=True).reset_index()
        # merge the aggregated df (including reverts) with the result df by title and namespace
        result = count_df.merge(result, on=['title', 'namespace'])
        return result

    # find the timedelta between the first and the last edit to a page (not between edits)
    def page_age(self, df):
        if self.logger:
            self.logger.info('calculating page ages')
        # calculate the age of the first edit for each page
        first = df.groupby(['title', 'namespace'])['ts'].min().to_frame('first_edit').reset_index()
        # convert first edit age to months
        first = first.merge(
            first['first_edit'].dt.month.add(first['first_edit'].dt.year.multiply(12)).to_frame('first_edit_months'),
            right_index=True, left_index=True)
        last = df[['title', 'namespace', 'ts']]
        # calculate the half year of each edit
        last = last.merge(last['ts'].dt.month.apply(HALF_YEAR).to_frame('half_year'), right_index=True, left_index=True)
        # calculate the year of each edit
        last = last.merge(last['ts'].dt.year.to_frame('year'), right_index=True, left_index=True)
        # get the unique years and half years for each page
        last = last.drop_duplicates(subset=['title', 'namespace', 'year', 'half_year'])[
            ['title', 'namespace', 'year', 'half_year']]
        # reindex to fill missing values
        last = last.set_index(['title', 'namespace', 'year', 'half_year'])
        last = last.reindex(pd.MultiIndex.from_product(last.index.levels, names=last.index.names)).reset_index()
        # calculate the age in months of the last edit
        last['last_edit_months'] = last['year'].multiply(12).add(last['half_year'].add(1).multiply(6))
        # merge the first and last edit dfs
        first_and_last = last.merge(first, on=['title', 'namespace'])
        # get the age in months by subtracting the first from last
        # NOTE: if age < 0, the page has not yet been created
        age = first_and_last['last_edit_months'].subtract(first_and_last['first_edit_months']).to_frame('page_age')
        result = first_and_last.merge(age, right_index=True, left_index=True)[['title', 'namespace', 'year', 'half_year', 'page_age']]
        return result

    # get the number of unique editors who have contributed to a group of articles
    def num_editors(self, df):
        if self.logger:
            self.logger.info('calculating editor counts')
        group_by_columns = ['title', 'namespace', df['ts'].dt.year, df['ts'].dt.month.apply(HALF_YEAR)]
        count_df = df.groupby(group_by_columns)['user_text'].nunique().to_frame('editor_count')
        count_df.index.names = ['title', 'namespace', 'year', 'half_year']
        # reindex and fill missing values w/ 0s
        count_df = count_df.reindex(pd.MultiIndex.from_product(count_df.index.levels, names=count_df.index.names),
                                    fill_value=0).reset_index()
        # get the cumulative sum for each time bin (total number of editors up through the end of the time bin)
        count_df = count_df.merge(
            count_df.groupby(['title', 'namespace'])['editor_count'].cumsum().to_frame('cumsum_editor_count_inclusive'),
            right_index=True, left_index=True)
        # get the cumulative sum excluding the current time bin
        count_df = count_df.merge(
            count_df['cumsum_editor_count_inclusive'].subtract(count_df['editor_count']).to_frame('cumsum_editor_count_exclusive'),
            right_index=True, left_index=True).reset_index()
        # merge the aggregated df (including reverts) with the result df by title and namespace
        return count_df

    # calculate the gini coef for num edits per editor
    def calculate_editor_gini_coef(self, df):
        if self.logger:
            self.logger.info('calculating gini coefs')
        # calculate the number of edits each editor made to a specific page for a specific time bin
        df = df.groupby(
            ['title', 'namespace', df['ts'].dt.year, df['ts'].dt.month.apply(HALF_YEAR), 'user_text']
        ).size().to_frame('edits_per_editor')
        # rename index columns
        df.index.names = ['title', 'namespace', 'year', 'half_year', 'user_text']
        df = df.reset_index()
        df = df.groupby(
            ['title', 'namespace', 'year', 'half_year']
        )['edits_per_editor'].apply(lambda x: gini._gini(x.values)).to_frame('gini_coef').reset_index()
        return df

    # return has_quality_assessment(true/false), max quality, quality change
    def propagate_quality_scores(self, df):
        if self.logger:
            self.logger.info('propagating quality scores')
        # convert max quality and quality change columns to numeric values and NaNs
        df['max_quality'] = pd.to_numeric(df['max_quality'], errors='coerce')
        df['quality_change'] = pd.to_numeric(df['quality_change'], errors='coerce')
        # group
        group = df.groupby(['title', 'namespace', df['ts'].dt.year, df['ts'].dt.month.apply(HALF_YEAR)])
        # determine the maximum quality during each time bin
        max_quality = group['max_quality'].max().to_frame('max_quality')
        # determine the change in quality during each time bin
        quality_change = group['quality_change'].sum().to_frame('quality_change')
        # recombine all the dataframes
        result = max_quality.merge(quality_change, how='outer', right_index=True, left_index=True)
        # reset the index to add missing time bins
        result.index.names = ['title', 'namespace', 'year', 'half_year']
        result = result.reindex(pd.MultiIndex.from_product(result.index.levels, names=result.index.names)).reset_index()
        # forward fill values from missing time bins
        # TODO FIX HAS QUALITY ASSESSMENT FFILL
        result['max_quality'] = result.groupby(['title', 'namespace'])['max_quality'].fillna(method='ffill')
        result['quality_change'] = result.groupby(['title','namespace'])['quality_change'].fillna(value=0)
        # determine whether there was a quality assessment during each time bin
        result['has_quality_assessment'] = result['max_quality'].notnull()
        return result


def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l', '--lang',
                        help='the two letter wiki language codes to to process')
    parser.add_argument('-i', '--infile',
                        required=True,
                        help='path to a file of raw edits')
    parser.add_argument('-o', '--outfile',
                        required=True,
                        help='path to the output file')

    # logging options
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='print verbose output')
    parser.add_argument('--log_file',
                        help='a file to log output')

    # preprocessing date threshold options
    parser.add_argument('--date_threshold',
                        help='only include edits before a set date.  use format Y-m-d H:M:S (e.g. 2015-10-03 07:23:40)')
    parser.add_argument('--relative_date_threshold',
                        help='number of [d,w,m,y] to include in the output after the first edit')
    parser.add_argument('--date_offset',
                        help='# a number of months to offset the dataset by.  For instance, if the dataset should start and end in April, date_offset = 4')

    # preprocessing filtering options
    parser.add_argument('--drop',
                        nargs='*',
                        default=[],
                        choices=['bots','reverts','parse_errors','lists','deleted_edits','drop1','unlinked'],
                        help='edits to drop in pre and post processing')
    parser.add_argument('--bot_list_filepath',
                        help='file path to a list of bot names to drop')
    args = parser.parse_args()

    # create logger
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter(fmt='[%(levelname)s %(asctime)s] %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # set output log file if specified
    if args.log_file:
        fileHandler = logging.FileHandler(filename=args.log_file,
                                          mode='w')

        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)
    else:
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

    ec = EditCounter(lang=args.lang,
                     date_threshold=args.date_threshold,
                     relative_date_threshold=args.relative_date_threshold,
                     date_offset=args.date_offset,
                     drop=args.drop,
                     bot_list_filepath=args.bot_list_filepath,
                     logger=logger)

    ec.load_raw_edit_file(args.infile)
    ec.process()
    ec.write_to_file(args.outfile)

if __name__ == "__main__":
    main()
