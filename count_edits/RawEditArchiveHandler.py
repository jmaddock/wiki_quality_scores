from RawEditPreProcessor import RawEditPreProcessor
from ThreadedCSVLoader import ThreadedCSVLoader
import pandas as pd
import argparse
import logging
import os

class RawEditArchiveHandler(object):

    def __init__(self, df, num_bins, lang, **kwargs):

        # some logging stuff
        self.logger = kwargs.get('logger')

        # dataframe of raw edits
        self.df = df
        self.num_bins = num_bins
        self.lang = lang

        # preprocessor for handling archives
        self.preprocessor = RawEditPreProcessor(**kwargs)

    def split_and_write(self, outdir):
        if self.logger:
            self.logger.info('writing {0} files to {1}'.format(self.num_bins, outdir))
        # collapse archives
        self.df = self.preprocessor.preprocess(self.df)
        # bin columns by page_id
        self.df['bin'] = pd.cut(self.df['page_id'], bins=self.num_bins, labels=range(self.num_bins))
        # write each group to its own file
        for i, g in self.df.groupby('bin'):
            outfile = os.path.join(outdir, '{0}_{1}.csv'.format(self.lang, i))
            g.to_csv(outfile, header=False, index_label=False)
            if self.logger:
                self.logger.debug('wrote file {0}'.format(outfile))

def main():
    parser = argparse.ArgumentParser(description='process wiki data')
    parser.add_argument('-l', '--lang',
                        required=True,
                        help='the two letter wiki language codes to to process')
    parser.add_argument('-i', '--indir',
                        required=True,
                        help='path to a directory of files containing raw edits')
    parser.add_argument('-o', '--outdir',
                        required=True,
                        help='a directory to output files')

    # logging options
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='print verbose output')
    parser.add_argument('--log_file',
                        help='a file to log output')

    parser.add_argument('-w', '--workers',
                        type=int,
                        required=True,
                        help='the number of processes to fork for threaded csv loading')
    parser.add_argument('-b', '--bins',
                        type=int,
                        help='the number of output files, omit to output the number as the number of input files.')
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

    # load all of the CSV files
    CSVloader = ThreadedCSVLoader(
        num_workers=args.workers,
    )
    CSVloader.get_file_list(args.indir)
    df = CSVloader.multiprocess_load()

    # if the number of files was not specified, output the same number as input
    if args.bins:
        bins = args.bins
    else:
        bins = len(CSVloader.infile_list)

    # group and write the files to output directory
    ah = RawEditArchiveHandler(
        df=df,
        num_bins=bins,
        lang=args.lang,
        logger=logger,
    )
    ah.split_and_write(args.outdir)

if __name__ == "__main__":
    main()