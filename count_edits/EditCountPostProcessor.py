import pandas as pd
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


class EditCountPostProcessor(object):

    def __init__(self, **kwargs):
        # some logging stuff
        self.logger = kwargs.get('logger')

        # list of post processing drop options
        self.drop = kwargs.get('drop', [])

    @debug_logging
    def drop1(self, df):
        if self.logger:
            self.logger.info('dropping pages with only one editor')
        df = df.loc[(df['cumsum_editor_count_inclusive_1'] > 1) & (df['cumsum_editor_count_inclusive_0'] > 1)]
        df = df.loc[(df['cumsum_edit_count_inclusive_1'] > 1) & (df['cumsum_edit_count_inclusive_0'] > 1)]
        return df

    @debug_logging
    def drop_unlinked_pages(self, df):
        if self.logger:
            self.logger.info('dropping unlinked pages')
        df = df.loc[(df['page_id_0'].notnull()) & (df['page_id_1'].notnull())]
        return df

    def post_process(self, df):
        if 'drop1' in self.drop:
            df = self.drop1(df)

        if 'unlinked' in self.drop:
            df = self.drop_unlinked_pages(df)

        return df
