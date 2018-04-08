import argparse
import os
import sys
import logging
from BaseJobScript import BaseJobScript

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir,'dump_processing','WikiDumpProcessor.py'))

class ProcessDumpJobScript(BaseJobScript):

    def __init__(self,indir,outdir,jobscript_file_path,script_path,**kwargs):
        if 'stdin' in kwargs and kwargs['stdin'] == True:
            self.stdin = True
        else:
            self.stdin = False
        super().__init__(indir,outdir,jobscript_file_path,script_path,**kwargs)

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
            # get a list of .7z files in the language directory
            file_list = [os.path.join(base_lang_dir, x) for x in os.listdir(base_lang_dir) if '.7z' in x]
            for i, f in enumerate(file_list):
                outfile_path = os.path.join(self.outdir, l, '{0}_{1}.csv'.format(l, i))
                if self.stdin:
                    out += '7z x {0} -so -bso0 -bsp0 -aoa | python3 {1} -l {2} -o {3} --stdin'.format(f, SCRIPT_PATH, l,
                                                                                                      outfile_path)
                else:
                    out += 'python3 {0} -l {1} -i {2} -o {3}'.format(self.script_path, l, f, outfile_path)
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
    parser.add_argument('--stdin',
                        action='store_true',
                        help='process input from stdin rather than a file')
    args = parser.parse_args()

    pdjs = ProcessDumpJobScript(
        indir=args.indir,
        outdir=args.outdir,
        job_script_file_path=args.jobscript_outfile,
        log_dir=args.log_dir,
        verbose=args.verbose,
        stdin=args.stdin,
        script_path=SCRIPT_PATH
    )
    # if the user does not provide a list of language codes, infer it from the directory structure
    if args.lang[0] == 'infer':
        pdjs.infer_lang_list(args.indir)
    else:
        pdjs.set_lang_list(args.lang_list)
    pdjs.make_script()

if __name__ == "__main__":
    main()
