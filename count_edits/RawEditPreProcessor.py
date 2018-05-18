import pandas as pd
import numpy as np
import os
import datetime
import gc
from functools import wraps


def debug_logging(f):
    @wraps(f)
    def wrapper(self, df, *args, **kwargs):
        # number of rows before f
        before_rows = len(df)
        # drop rows
        df = f(self, df, *args, **kwargs)
        # number of rows after f
        after_rows = len(df)
        # log num rows before, after, diff
        if self.logger:
            self.logger.debug('rows before: {0}'.format(before_rows))
            self.logger.debug('rows after: {0}'.format(after_rows))
            self.logger.debug('rows dropped: {0}'.format(before_rows - after_rows))
        return df
    return wrapper


class RawEditPreProcessor(object):

    def __init__(self, **kwargs):
        # some logging stuff
        self.logger = kwargs.get('logger')

        # optional parameters
        # thresholding by a specfic date
        if 'date_threshold' in kwargs and kwargs['date_threshold']:
            self.date_threshold = datetime.datetime.strptime(kwargs['date_threshold'], '%Y-%m-%d %H:%M:%S')
        else:
            self.date_threshold = None

        # threshold by a duration relative to the first edit in the wiki
        self.relative_date_threshold = kwargs.get('relative_date_threshold')

        # a number of months to offset the dataset by.  For instance, if the dataset should start and end in April,
        # date_offset = 4
        self.date_offset = kwargs.get('date_offset')

        # list of edits to drop in preprocessing
        self.drop = kwargs.get('drop', [])
        self.bot_list_filepath = kwargs.get('bot_list_filepath')
        # check to see if the user specified a bot file
        self._validate_bot_list()

    def _validate_bot_list(self):
        if self.bot_list_filepath and 'bots' in self.drop:
            if not os.path.isfile(self.bot_list_filepath):
                message = 'missing bot list file!'
                if self.logger:
                    self.logger.error(message)
                raise FileNotFoundError(message)
        elif 'bots' in self.drop and not self.bot_list_filepath:
            message = 'bot_list_filepath argument required when no_bots == True'
            if self.logger:
                self.logger.error(message)
            raise ValueError(message)
        else:
            return True

    # remove trailing and leading spaces from page titles
    def normalize_titles(self, df):
        df['title'] = df['title'].str.strip()
        return df

    ## drop of df duplicates based on page_id field
    ## log number of duplicates dropped
    def drop_dups(self, df):
        num_dups = len(df.set_index('page_id', drop=False).index.get_duplicates())
        percent = num_dups / len(df)
        if self.logger:
            self.logger.info('dropped %s (%.2f%%) duplicates' % (num_dups, percent))
        return df.drop_duplicates(subset='page_id', keep=False)

    def flag_bots(self, df):
        bot_list = pd.read_csv(self.bot_list_filepath, dtype={'bot_name': object}, na_values={'title': ''},
                               keep_default_na=False, )
        df['is_bot'] = df['user_text'].isin(bot_list['bot_name'])
        gc.collect()
        return df

    def remove_bots(self, df):
        if self.logger:
            self.logger.info('dropping bot edits')
        num_bots = len(df.loc[df['is_bot'] == True])
        percent = num_bots / len(df)
        if self.logger:
            self.logger.info('dropped {0} ({1:.2f}%) bot edits'.format(num_bots, percent))
        df = df.loc[df['is_bot'] == False]
        gc.collect()
        return df

    @debug_logging
    def remove_lists(self, df):
        if self.logger:
            self.logger.info('removing pages that are lists')
        df = df.loc[~df['title'].str.contains('list of|lists of', case=False)]
        gc.collect()
        return df

    # remove all edits that are reverts or have been reverted
    @debug_logging
    def drop_reverts(self, df):
        if self.logger:
            self.logger.info('removing reverted and reverting edits')
        df = df.loc[df['revert'].isnull()]
        gc.collect()
        return df

    @debug_logging
    def drop_deleted(self, df):
        if self.logger:
            self.logger.info('removing deleted edits')
        df = df.loc[df['deleted_text'] == False]
        gc.collect()
        return df

    @debug_logging
    def drop_parse_error(self, df):
        if self.logger:
            self.logger.info('dropping parser errors')
        df = df.loc[df['parse_error'] == False]
        gc.collect()
        return df

    @debug_logging
    def threshold_by_date(self, df):
        if self.logger:
            self.logger.info('applying date threshold {0}'.format(self.date_threshold))
        # get all edits earlier than date_threshold
        df = df.loc[df['ts'] <= self.date_threshold]
        gc.collect()
        return df

    @debug_logging
    def threshold_by_relative_date(self, df):
        if self.logger:
            self.logger.info('applying relative date threshold {0}'.format(self.relative_date_threshold))
        # get all edits earlier than date_threshold
        df = df.loc[df['relative_age'] <= self.relative_date_threshold]
        gc.collect()
        return df

    @debug_logging
    def reletive_page_age(self, df, duration='month'):
        first_row = np.datetime64((df['ts'].min()))
        if duration == 'month':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[M]')
        elif duration == 'year':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[Y]')
        elif duration == 'week':
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[W]')
        else:
            df['relative_age'] = df['ts'].subtract(first_row, axis='index').astype('timedelta64[D]')
        return df

    def subtract_date_offset(self, df):
        if self.logger:
            self.logger.info('applying date offset {0}'.format(self.date_offset))
        df['ts'] = df['ts'].subtract(pd.Timedelta(self.date_offset, unit='M'))
        gc.collect()
        return df

    ## remove all rows containing an archive to get only active page ids
    ## if the page only contains archive, get a random archived ID
    def process_archive_names(self, df):
        if self.logger:
            self.logger.info('collapsing archives')
        # get all non-archived ids
        result = df.loc[df['archive'].isnull()]
        # get an archived id for each archive that doesn't have an un-archived page
        only_archive = self._get_archives_without_unarchived(df)
        # concat the 2 dfs
        result = pd.concat([result, only_archive])
        gc.collect()
        return result

    def _get_archives_without_unarchived(self, df):
        # get all unarchived talk page titles
        unarchived_talk_page_titles = df.loc[(df['namespace'] == 1) & (df['archive'].isnull())]['title']
        # get all pages with titles not in unarchived_talk_page_titles
        archived_talk_pages_without_non_archives = df.loc[
            (~df['title'].isin(unarchived_talk_page_titles)) & (df['namespace'] == 1)]
        # remove duplicated titles (multiple archives)
        archived_talk_pages_without_non_archives = archived_talk_pages_without_non_archives.drop_duplicates('title')
        # get the number of pages
        gc.collect()
        return archived_talk_pages_without_non_archives

    def preprocess(self, df):
        # convert 'ts' field to pd.datetime
        df['ts'] = pd.to_datetime(df['ts'], format="%Y-%m-%d %H:%M:%S")

        # remove edits belonging to pages with no title
        df = df.loc[df['title'].notnull()]

        # remove reverts
        if 'reverts' in self.drop:
            df = self.drop_reverts(df)

        # remove edits with quality parser errors
        if 'parse_errors' in self.drop:
            df = self.drop_parse_error(df)
        # remove lists
        if 'lists' in self.drop:
            df = self.remove_lists(df)

        # remove deleted edits
        if 'deleted_edits' in self.drop:
            df = self.drop_deleted(df)

        # remove all edits made by registered bots
        if 'no_bots' in self.drop:
            df = self.flag_bots(df)
            df = self.remove_bots(df)
        
        # drop all edits older than a specific date
        if self.date_threshold is not None:
            df = self.threshold_by_date(df)

        # threshold by edit age relative to the first edit in the wiki
        if self.relative_date_threshold is not None:
            df = self.reletive_page_age(df)
            df = self.threshold_by_relative_date(df)

        if self.date_offset:
            df = self.subtract_date_offset(df)
        
        # collapse all archives into the same page
        df = self.process_archive_names(df)
        # remove trailing and leading spaces from page titles
        #df = self.normalize_titles(df)
        gc.collect()
        return df
