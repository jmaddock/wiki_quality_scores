import argparse
import os
import sys
import logging

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),os.pardir,'WikiDumpProcessor.py'))

# set up some logging stuff
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='[%(levelname)s %(asctime)s] %(message)s',
                              datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)

## infer a list of language codes to process based on the directory structure
def infer_lang_list(basedir):
        lang_list = [name for name in os.listdir(basedir) if
                     os.path.isdir(os.path.join(basedir, name))]
        logger.info('found language directories {0}'.format(lang_list))
        return lang_list

## make sure all the directories provided by the user actually exist
def validate_dirs(args):
    if not os.path.isdir(args.indir):
        raise FileNotFoundError('{0} is not a valid input directory'.format(args.indir))
    if not os.path.isdir(os.path.dirname(args.outdir)):
        raise FileNotFoundError('{0} is not a valid output directory'.format(args.outdir))
    if args.log_dir and not os.path.isdir(os.path.dirname(args.log_dir)):
        raise FileNotFoundError('{0} is not a valid logging directory'.format(args.log_dir))
    if not os.path.exists(SCRIPT_PATH):
        raise FileNotFoundError('{0} is not a valid processing script file'.format(SCRIPT_PATH))
    logger.info('all specified base directories and files are valid')

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
    parser.add_argument('--verbose',
                        action='store_true',
                        help='verbose output')
    args = parser.parse_args()
    # make sure all the directories actually exist
    validate_dirs(args)
    # set log level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # create the job script file, passed in command line params with -j flag
    job_script = open(args.jobscript_outfile, 'w')
    # if the user does not provide a list of language codes, infer it from the directory structure
    if args.lang[0] == 'infer':
        lang_list = infer_lang_list(args.indir)
    else:
        lang_list = args.lang
    # initialize the output string that will be writen to the jobscript file
    out = ''
    for l in lang_list:
        # get the specific file path for a given language
        base_lang_dir = os.path.join(args.indir, l)
        # get a list of .7z files in the language directory
        file_list = [os.path.join(base_lang_dir, x) for x in os.listdir(base_lang_dir) if '.7z' in x]
        for i, f in enumerate(file_list):
            outfile_path = '{0}_{1}_{2}.csv'.format(args.outdir, l, i)
            out += 'python3 {0} -l {1} -i {2} -o {3}'.format(SCRIPT_PATH, l, f, outfile_path)
            if args.log_dir:
                logfile_path = '{0}_{1}_{2}.log'.format(args.log_dir, l, i)
                out = '{0} --log_file {1}'.format(out, logfile_path)
            out += '\n'
    job_script.write(out)
    logger.debug(out)
    logger.info('wrote job script to file: {0}'.format(args.jobscript_outfile))

if __name__ == "__main__":
    main()
