import pandas as pd

class EditCountPostProcessor(object):

    def __init__(self, **kwargs):
        # some logging stuff
        self.logger = kwargs.get('logger')

        # list of post processing drop options
        self.drop = kwargs.get('drop',[])

    def drop1(self, df):
        df = df.loc[(df['cumsum_editor_count_inclusive_1'] > 1) & (df['cumsum_editor_count_inclusive_0'] > 1)]
        df = df.loc[(df['cumsum_edit_count_inclusive_1'] > 1) & (df['cumsum_edit_count_inclusive_0'] > 1)]
        return df

    def drop_unlinked_pages(self, df):
        df = df.loc[(df['page_id_0'].notnull()) & (df['page_id_1'].notnull())]
        return df

    def post_process(self, df):
        if 'drop1' in self.drop:
            df = self.drop1(df)

        if 'unlinked' in self.drop:
            df = self.drop_unlinked_pages(df)

        return df