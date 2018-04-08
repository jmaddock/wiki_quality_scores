import argparse
import os
import sys
import logging
from BaseJobScript import BaseJobScript

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir,'combine','CombineRawEdits.py'))

class CombineRawEditsJobScript(BaseJobScript):

    def __init__(self,indir,outdir,jobscript_file_path,script_path,**kwargs):
        super().__init__(
            indir=indir,
            outdir=outdir,
            jobscript_file_path=jobscript_file_path,
            script_path=script_path,
            **kwargs
        )

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
            outfile_path = os.path.join(self.outdir, l, '{0}_combined_raw_edits.csv'.format(l))
            out += 'python3 {0} -i {1} -o {2}'.format(self.script_path, base_lang_dir, outfile_path)
            if self.log_dir:
                logfile_path = '{0}_{1}_{2}.log'.format(self.log_dir, l, i)
                out = '{0} --log_file {1}'.format(out, logfile_path)
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
    args = parser.parse_args()

    js = CombineRawEditsJobScript(
        indir=args.indir,
        outdir=args.outdir,
        jobscript_file_path=args.jobscript_outfile,
        script_path=SCRIPT_PATH,
        log_dir=args.log_dir,
        verbose=args.verbose,
    )
    # if the user does not provide a list of language codes, infer it from the directory structure
    if args.lang[0] == 'infer':
        js.infer_lang_list(args.indir)
    else:
        js.set_lang_list(args.lang)
    js.make_script()

if __name__ == "__main__":
    main()
