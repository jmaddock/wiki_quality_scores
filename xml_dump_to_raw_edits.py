import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mwxml
import mwtypes
import pandas as pd
import datetime
import utils
import subprocess
import codecs
import argparse
import config
import translations
import uuid
import single_revision_template_extractor
import timeit

# define the directory where this script lives
SCRIPT_DIR = os.path.abspath(__file__)

## class for tracking reverts from SHA1 hash
class Revert_Tracker(object):
    def __init__(self):
        self.hashes = []

    def is_revert(self,new_hash):
        if new_hash in self.hashes:
            return True
        else:
            self.hashes.append(new_hash)
            return False

## Class for decompressing, iterating through, and re-compressing wiki xml dumps
## Will not decompress/recompress in debug mode
class Single_Dump_Handler(object):
    def __init__(self,f_in):
        self.f_in = f_in
        self.uncompressed = f_in.rsplit('.7z', 1)[0]
        self.dump = None
        self.base_dir = f_in.rsplit('/', 1)[0]

    # create an iterator for a dump that has already been decompressed
    # depreciated
    def open_dump(self):
        utils.log('opening file: %s' % self.f_in)
        self.dump = mwxml.Dump.from_file(self.db_path)
        return self.dump

    # decompress a dump from a .7z archive
    def decompress(self):
        utils.log('decompressing file: %s' % self.f_in)
        subprocess.call(['7z','x',self.f_in,'-o' + self.base_dir])

    # remove the decompressed dump after processing
    def remove_dump(self):
        self.dump = None
        utils.log('removing file: %s' % self.uncompressed)
        subprocess.call(['rm',self.uncompressed])

    # convenience method for decompressing and opening a dump
    def process_dump(self):
        if os.path.exists(self.uncompressed):
            self.remove_dump()
        self.decompress()
        self.open_dump()
        return self.dump

## class for processing an already decompressed wikipedia xml dump
## specify xml_dump param to use specific path to xml_dump, otherwise use config file
class CSV_Creator(object):
    def __init__(self,lang,xml_dump=None,verbose=False):
        utils.log('creating importer...')
        self.lang = lang
        self.dh = None
        self.edit_count = 0
        self.page_count = 0
        if xml_dump:
            self.db_path = xml_dump
        else:
            self.db_path = config.ROOT_PROCESSED_DIR
        self.uuid_list = pd.DataFrame({'title':[],'namespace':[],'uuid':[]})
        self.verbose = verbose

    def create_db_dir(self):
        utils.log(self.db_path+self.lang)
        if not os.path.exists(self.db_path+self.lang):
            utils.log('creating dir: %s' % self.db_path+self.lang)
            os.makedirs(self.db_path+self.lang)

    def get_extractor_name(self):
        if self.lang == 'simple':
            extractor_name = 'enwiki'
        else:
            extractor_name = '{0}wiki'.format(self.lang)
        return extractor_name

    # process the entire xml dump, ignoring pages that are not namespace 0 or 1 (articles or talk)
    def process_dump(self,f_in=None,f_out=None,n=None,v=False,debug=False):
        # create an iterator for the xml file
        self.dh = mwxml.Dump.from_file(self.db_path)
        # create an empty .csv file for the output (processed edits)
        db_file = open(f_out,'w')
        # write the csv file header
        db_file.write('"page_id","namespace","title","archive","user_text","user_id","revert","ts","project","quality"\n')
        # define a quality score extractor
        extractor_name = self.get_extractor_name()
        extractor = single_revision_template_extractor.load_extractor(extractor_name)
        # start iterating through the xml dump
        # break after n iterations if the --debug flag has been specified
        for page in self.dh:
            if page.namespace == 1 or page.namespace == 0:
                # create a list to track already used edit hashes
                rt = Revert_Tracker()
                # process page level data
                d = self.process_page(page)
                if self.verbose:
                    utils.log(d['title'])
                for rev in page:
                    # process revision level data
                    # deleted revisions return None
                    r = self.process_edit(rev,rt)
                    if r:
                        q = self.process_quality(rev,extractor)
                        self.write_row(d,r,q,db_file)
                    else:
                        continue
                self.page_count += 1
            if debug and debug > 0 and debug == self.page_count:
                break
        utils.log('processed %s pages and %s edits' % (self.page_count,self.edit_count))


    # input page and rev data for one edit
    # write a row of the csv outfile
    def write_row(self,d,r,q,db_file):
        result = '"%s",%s,%s,"%s","%s","%s","%s",%s,"%s","%s","%s",\n' % (
            d['uuid'],
            d['page_id'],
            d['namespace'],
            d['title'],
            d['full_title'],
            d['archive'],
            r['user_text'],
            r['user_id'],
            r['revert'],
            r['ts'],
            q['quality']
        )
        db_file.write(result)

    def generate_uuid(self,title,namespace):
        if len(self.uuid_list) > 0:
            query = self.uuid_list.loc[(self.uuid_list['namespace'] == namespace) & (self.uuid_list['title'] == title)]
        else:
            query = []
        if len(query) == 1:
            page_uuid = query['uuid'].values[0]
        else:
            page_uuid = uuid.uuid1()
            self.uuid_list = self.uuid_list.append(pd.DataFrame([{
                'title':title,
                'namespace':namespace,
                'uuid':page_uuid
            }]))
        return uuid

    # process page level data
    def process_page(self,page):
        # results dictionary for each page
        d = {}
        # get the page id
        d['page_id'] = page.id
        # get the namespace (0 or 1)
        d['namespace'] = page.namespace
        # replace quote chars with an escape character, remove trailing spaces, and convert to lowercase
        stripped_title = page.title.replace('"', config.QUOTE_ESCAPE_CHAR).strip()  # .lower()
        # capture the full title w/ escaped quotes
        d['full_title'] = stripped_title
        # if the page is a talk page, strip "Talk:" from the title
        if page.namespace == 1:
            stripped_title = stripped_title.split(':', 1)[-1]
        # remove trailing "/archive" from the title
        # get the archive number or title (if any)
        if len(stripped_title.split(
                '/{0}'.format(translations.translations['archive'][self.lang]))) > 1 and page.namespace == 1:
            d['title'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[0]
            d['archive'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[
                1].strip()
            # if no text follows "/archive" in the title, add a 0
            if len(d['archive']) < 1:
                d['archive'] = 0
        else:
            d['archive'] = None
            d['title'] = stripped_title
        d['uuid'] = self.generate_uuid(title=d['title'], namespace=d['namespace'])
        self.page_count += 1
        return d

    def process_edit(self,rev,rt):
        r = {}
        # replace quote chars in user text
        if rev.user and rev.user.text:
            r['user_text'] = rev.user.text.replace('"', '')
        elif rev.user:
            r['user_text'] = rev.user.text
        else:
            return None
        # get the user id
        r['user_id'] = rev.user.id
        # determine if the revert hash has been used previously, set revert to True or False
        r['revert'] = rt.is_revert(rev.sha1)
        # get the datetime of the edit
        r['ts'] = str(datetime.datetime.fromtimestamp(rev.timestamp))
        self.edit_count += 1
        return r

    def process_quality(self,revision,extractor):
        q = {}
        quality = extractor.extract(revision)
        if quality:
            print(quality)
            #q['project'] = quality['project']
            q['quality'] = quality
        else:
            #q['project'] = None
            q['quality'] = None
        return q

    def document_robustness_checks(self,f_in):
        utils.log('running document tests')
        df = pd.read_csv(f_in,na_values={'title':''},keep_default_na=False,dtype={'title': object})
        assert len(df) == self.edit_count
        utils.log('passed edit count test: iteration count and document line count match')
        assert len(df['page_id'].unique()) == self.page_count
        utils.log('passed page count test: iteration count and unique page_id match')
        assert len(df.loc[df['namespace'] == 0]['title'].unique()) == len(df.loc[df['namespace'] == 0]['page_id'].unique())
        assert len(df.loc[(df['namespace'] == 1) & (df['archive'] == 'None')]['title'].unique()) == len(df.loc[(df['namespace'] == 1) & (df['archive'] == 'None')]['page_id'].unique())
        utils.log('passed title uniqueness test: equal number of unique titles and page_ids')
        assert len(df.loc[(df['namespace'] >= 0) & (df['namespace'] <= 1)]) == len(df)
        utils.log('passed namespace test: namespaces equal 0 or 1')

# IN: path and file name of job script file, optional list of languages (leave empty for all langs)
def job_script(job_script_file_name,lang_list=None):
    # create the job script file, passed in command line params with -j flag
    job_script = open(job_script_file_name,'w')
    # get a list of language dirs if lang isn't specified
    if not lang_list or len(lang_list) == 0:
        lang_list = [name for name in os.listdir(config.ROOT_RAW_XML_DIR) if os.path.isdir(os.path.join(config.ROOT_RAW_XML_DIR,name))]
    for l in lang_list:
        base_lang_dir = os.path.join(config.ROOT_RAW_XML_DIR,l)
        file_list = [os.path.join(base_lang_dir,x) for x in os.listdir(base_lang_dir) if '.7z' in x]
        utils.log(file_list)
        for i,f in enumerate(file_list):
            outfile_name = '{0}{1}.csv'.format(config.RAW_EDITS_BASE,i+1)
            outfile_path = os.path.join(config.ROOT_PROCESSED_DIR,l,outfile_name)
            out = 'python3 {0} -l {1} -i {2} -o {3}\n'.format(SCRIPT_DIR,l,f,outfile_path)
            job_script.write(out)

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang',
                        nargs='*',
                        help='the language of the xml dump')
    parser.add_argument('-i','--infile',
                        help='the file path of a wikipedia xml dump to process')
    parser.add_argument('-o','--outfile',
                        help='a file path for an output .csv of edits')
    parser.add_argument('-j','--job_script',
                        help='the path to output a job script file for HYAK batch processing')
    parser.add_argument('-d','--debug',
                        type=int,
                        help='the number of iterations through the file xml dump to process, enter 0 to process the entire dump but do not decompress file')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='print verbose output')
    args = parser.parse_args()
    if args.job_script:
        job_script(args.job_script,args.lang)
    else:
        infile = args.infile
        outfile = args.outfile
        # create an object to handle the compressed dump if it hasn't already been decompressed
        # return a decompressed xml file
        if args.debug == None:
            DumpHandler = Single_Dump_Handler(infile)
            xml_dump = DumpHandler.decompress()
        else:
            xml_dump = infile
        # create an object to handle the decompressed xml dump
        c = CSV_Creator(lang=args.lang[0],
                        xml_dump=xml_dump,
                        verbose=args.verbose)
        c.process_dump(f_out=outfile,debug=args.debug)
        #c.document_robustness_checks(outfile)
        # remove the decompressed file if the user has not specified the --debug flag
        if args.debug == None:
            DumpHandler.remove_dump()

if __name__ == "__main__":
    main()
