import os
import argparse
import sys
import logging
from BaseJobScript import BaseJobScript

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir,'count_edits','EditCounter.py'))

class CountEditsJobScript(BaseJobScript):

    def __init__(self,indir,outdir,jobscript_file_path,script_path,**kwargs):
        self.date_threshold = kwargs.get('date_threshold')
        self.relative_date_threshold = kwargs.get('relative_date_threshold')
        self.drop = kwargs.get('drop', [])
        self.bot_list_file_path = kwargs.get('bot_list_file_path')
        if 'bots' in self.drop:
            self.validate_bot_list()

        super().__init__(
            indir=indir,
            outdir=outdir,
            jobscript_file_path=jobscript_file_path,
            script_path=script_path,
            **kwargs
        )

    def validate_bot_list(self):
        if 'bots' in self.drop and not self.bot_list_file_path:
            raise ValueError('bot_list_file_path argument required when no_bots == True')
        if not os.path.exists(self.bot_list_file_path):
            raise FileNotFoundError('{0} is not a valid bot list file'.format(self.bot_list_file_path))

    def make_script(self):
        # make sure all the directories actually exist
        self.validate_dirs()
        # create the job script file, passed in command line params with -j flag
        job_script = open(self.jobscript_file_path, 'w')
        # initialize the output string that will be writen to the jobscript file
        out = ''
        for l in self.lang_list:
            # get the specific file path for a given language
            base_lang_dir = os.path.join(self.indir, l)
            # get a list of .csv files in the language directory
            file_list = [os.path.join(base_lang_dir, x) for x in os.listdir(base_lang_dir) if '.csv' in x]
            # raise exception in there's more than one file in the directory
            # reformat output to fix this
            if len(file_list) > 1:
                message = 'too many files in dir {0}'.format(base_lang_dir)
                self.logger.error(message)
                raise IOError(message)
            for f in file_list:
                outfile_path = os.path.join(self.outdir, l, '{0}_edit_counts.csv'.format(l))
                out += 'python3 {0} -i {1} -l {2} -o {3}'.format(SCRIPT_PATH, f, l, outfile_path)
                if self.date_threshold:
                    out = '{0} --date_threshold {1}'.format(self.date_threshold)
                if self.relative_date_threshold:
                    out = '{0} --relative_date_threshold {1}'.format(self.relative_date_threshold)
                if self.log_dir:
                    logfile_path = '{0}_{1}.log'.format(self.log_dir, l)
                    out = '{0} --log_file {1}'.format(out, logfile_path)
                if self.bot_list_file_path:
                    out = '{0} --bot_list_filepath {1}'.format(out, self.bot_list_file_path)
                if len(self.drop) > 0:
                    out = '{0} --drop'.format(out)
                    for d in self.drop:
                        out = '{0} {1}'.format(out, d)
                out += '\n'
        job_script.write(out)
        self.logger.debug(out)
        self.logger.info('wrote job script to file: {0}'.format(self.jobscript_file_path))

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')

    parser.add_argument('-l','--lang',
                        nargs='*',
                        required=True,
                        help='a list of two letter language codes to process, or "infer" to infer the language list from the directory structure')
    parser.add_argument('-i', '--indir',
                        required=True,
                        help='the base input directory containing sub-diretories named by two letter language code')
    parser.add_argument('-o', '--outdir',
                        required=True,
                        help='the base output directory')
    parser.add_argument('-j', '--jobscript_outfile',
                        required=True,
                        help='a file path for the job script')
    parser.add_argument('--log_dir',
                        help='directory to store log files')
    parser.add_argument('--create_language_dirs',
                        action='store_true',
                        help='create output language directories if they do not already exist')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='verbose output')

    # preprocessing date threshold options
    parser.add_argument('--date_threshold',
                        help='only include edits before a set date.  use format Y-m-d H:M:S (e.g. 2015-10-03 07:23:40)')
    parser.add_argument('--relative_date_threshold',
                        help='number of [d,w,m,y] to include in the output after the first edit')

    # preprocessing filtering options
    parser.add_argument('--drop',
                        nargs='*',
                        choices=['bots','reverts','parse_errors','lists','deleted_edits','drop1','unlinked'],
                        help='possible preprocessing drop options')
    parser.add_argument('--bot_list_filepath',
                        help='file path to a list of bot names to drop')

    args = parser.parse_args()

    js = CountEditsJobScript(
        indir=args.indir,
        outdir=args.outdir,
        jobscript_file_path=args.jobscript_outfile,
        log_dir=args.log_dir,
        verbose=args.verbose,
        script_path=SCRIPT_PATH,
        date_threshold=args.date_threshold,
        relative_date_threshold=args.relative_date_threshold,
        drop=args.drop,
        bot_list_file_path=args.bot_list_filepath,
    )
    # if the user does not provide a list of language codes, infer it from the directory structure
    if args.lang[0] == 'infer':
        js.infer_lang_list(args.indir)
    else:
        js.set_lang_list(args.lang)
    js.make_script()

if __name__ == "__main__":
    main()
