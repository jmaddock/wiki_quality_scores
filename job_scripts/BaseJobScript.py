import os
import logging

# set up some logging stuff
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(fmt='[%(levelname)s %(asctime)s] %(message)s',
                              datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)

class BaseJobScript(object):

    def __init__(self,indir,outdir,jobscript_file_path,script_path,**kwargs):
        self.logger = logger
        if 'lang_list' in kwargs:
            self.lang_list = kwargs['lang_list']
        else:
            self.lang_list = None
        if 'log_dir' in kwargs:
            self.log_dir = kwargs['log_dir']
        else:
            self.log_dir = None
        if 'verbose' in kwargs:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.indir = indir
        self.outdir = outdir
        self.jobscript_file_path = jobscript_file_path
        self.script_path = script_path

    ## infer a list of language codes to process based on the directory structure
    def infer_lang_list(self):
        self.lang_list = [name for name in os.listdir(self.indir) if
                     os.path.isdir(os.path.join(self.indir, name))]
        logger.info('found language directories {0}'.format(self.lang_list))
        return self.lang_list

    def set_lang_list(self,lang_list):
        self.lang_list = lang_list

    ## make sure all the directories provided by the user actually exist
    def validate_dirs(self):
        if not os.path.isdir(self.indir):
            raise FileNotFoundError('{0} is not a valid input directory'.format(self.indir))
        if not os.path.isdir(os.path.dirname(self.outdir)):
            raise FileNotFoundError('{0} is not a valid output directory'.format(self.outdir))
        if self.log_dir and not os.path.isdir(os.path.dirname(self.log_dir)):
            raise FileNotFoundError('{0} is not a valid logging directory'.format(self.log_dir))
        if not os.path.exists(self.script_path):
            raise FileNotFoundError('{0} is not a valid processing script file'.format(self.script_path))
        logger.info('all specified base directories and files are valid')

    def make_script(self):
        pass