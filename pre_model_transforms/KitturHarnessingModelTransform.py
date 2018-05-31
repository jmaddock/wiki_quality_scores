import pandas as pd
import numpy as np
import logging
import argparse

COLUMNS = [
    'title', 'year', 'half_year', 'page_id_0', 'gini_coef_0', 'page_id_1', 'has_quality_assessment_1', 'lang',
    'kittur_quality_change', 'max_start_quality', 'log_page_age_0', 'log_editor_count_0', 'log_editor_count_1',
    'log_cumsum_edit_count_inclusive_1'
]

class KitturHarnessingModelTransform(object):

    def __init__(self, **kwargs):
        # some logging stuff
        self.logger = kwargs.get('logger')

        # dataframe of edit counts
        self.df = None
        # dataframe of transformed data for modeling
        self.processed_df = None
        # date range in years [start, end)
        self.date_range = kwargs.get('date_range')

    def load_count_data(self, infile):
        if self.logger:
            self.logger.info('loading data from file {0}'.format(infile))
        self.df = pd.read_csv(infile)

    def write_to_file(self, outfile):
        self.processed_df.to_csv(outfile)
        if self.logger:
            self.logger.info('wrote file to {0}'.format(outfile))

    def create_log_messures(self, df):
        if self.logger:
            self.logger.info('creating log count measures')
        df[['log_page_age_0', 'log_editor_count_0', 'log_editor_count_1', 'log_cumsum_edit_count_inclusive_1']] = df[
            ['page_age_0', 'editor_count_0', 'editor_count_1', 'cumsum_edit_count_inclusive_1']].apply(np.log2)
        return df

    def kittur_quality_change(self, df):
        if self.logger:
            self.logger.info('creating quality change measure')
        # create a column that represents both the year and month column
        df['time_bin'] = df['year'].add(df['half_year'].multiply(.5))
        # offset the end quality by 6 months
        start_quality = df[['time_bin', 'title', 'max_quality_1']].rename(columns={'max_quality_1': 'max_start_quality'})
        start_quality['time_bin'] = start_quality['time_bin'].add(.5)
        # merge the offset end quality with the original df
        df = df.merge(start_quality, on=['time_bin', 'title'], how='outer')
        # subtract the original (t=n) quality from the offset quality (t=n+1) to get quality change
        df['kittur_quality_change'] = df['max_quality_1'].subtract(df['max_start_quality'])
        return df

    def subset_by_year(self, df):
        if self.logger:
            self.logger.info('subsetting by date range {0}'.format(self.date_range))
        # exclude all rows not within the specified date range
        df = df.loc[(df['year'] >= self.date_range[0]) & (df['year'] < self.date_range[1])]
        # remove all pages/articles that have not yet been created
        df = df.loc[(df['page_age_1'] > 0) & (df['page_age_0'] > 0)]
        return df

    def transform_count_data(self):
        df = self.df
        # log transform count skewed count measures
        df = self.create_log_messures(df)
        # create the quality change metric used in kittur et al.
        df = self.kittur_quality_change(df)
        # if a date range is provided, only include that date range
        if self.date_range:
            df = self.subset_by_year(df)
        # only store the required columns for modeling
        self.processed_df = df[COLUMNS]


def main():
    parser = argparse.ArgumentParser(description='process wiki data')
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

    parser.add_argument('--date_range',
                        nargs=2,
                        type=int,
                        help='date range in years [start, end)')
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

    mt = KitturHarnessingModelTransform(
        date_range=args.date_range,
        logger=logger
    )

    mt.load_count_data(args.infile)
    mt.transform_count_data()
    mt.write_to_file(args.outfile)

if __name__ == "__main__":
    main()
