import os
import pandas as pd
import argparse
import logging

class CombineRawEdits(object):

    def __init__(self,**kwargs):
        if 'filelist' in kwargs:
            self.filelist = kwargs['filelist']
        else:
            self.filelist = None
        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = None
        self.combined_df = None

    def get_file_list_from_dir(self,indir):
        self.filelist = [os.path.join(indir,f) for f in os.listdir(indir) if f[-4:] == '.csv']
        if self.logger:
            self.logger.debug(self.filelist)
            self.logger.info('found {0} files in {1}'.format(len(self.filelist),indir))

    def combine(self):
        self.combined_df = pd.concat((pd.read_csv(f,na_values={'title':''},keep_default_na=False) for f in self.filelist))
        if self.logger:
            self.logger.info('combined {0} rows from {1} files'.format(len(self.combined_df),len(self.filelist)))

    def write_to_file(self,outfile):
        self.combined_df.to_csv(outfile)
        if self.logger:
            self.logger.info('wrote file to {0}'.format(outfile))


def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-i','--indir',
                        help='combine all .csv files from this directory.  do not specify --filelist')
    parser.add_argument('-o', '--outfile',
                        required=True,
                        help='write the combined dataframe to this file path')
    parser.add_argument('-f', '--filelist',
                        nargs='*',
                        help='a list of files to combine.  do not specify --indir')
    parser.add_argument('-l', '--log_file',
                        help='a file to log output')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='print verbose output')
    args = parser.parse_args()

    # logging stuff
    if args.log_file:
        logger = logging.getLogger(__name__)
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        handler = logging.FileHandler(filename=args.log_file,
                                      mode='w')
        formatter = logging.Formatter(fmt='[%(levelname)s %(asctime)s] %(message)s',
                                      datefmt='%m/%d/%Y %I:%M:%S %p')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        logger = None

    cre = CombineRawEdits(filelist=args.filelist,
                          logger=logger)
    if args.indir:
        cre.get_file_list_from_dir(args.indir)
    cre.combine()
    cre.write_to_file(args.outfile)

if __name__ == "__main__":
    main()
