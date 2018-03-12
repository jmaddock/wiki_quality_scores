from WikiPageProcessor import WikiPageProcessor
from DumpHandler import DumpHandler
import mwxml
import utils
import argparse
import single_revision_template_extractor

COLUMN_LIST = [
    "page_id",
    "namespace",
    "title",
    "archive",
    "user_text",
    "user_id",
    "revert",
    "ts",
    "quality_change",
    "new_quality_scores"
]

class WikiDumpProcessor(object):
    def __init__(self,lang,dump_path,outfile_path,**kwargs):
        self.lang = lang
        self.dump_path = dump_path
        self.outfile_path = outfile_path
        if 'column_list' in kwargs:
            self.colunm_list = kwargs['column_list']
        else:
            self.column_list = COLUMN_LIST
        if 'num_rows' in kwargs:
            self.num_rows = kwargs['num_rows']
        else:
            self.num_rows = None
        # keep track of the number of processed pages and edits debug/logging purposes
        self.page_count = 0
        self.edit_count = 0

    def get_extractor_name(self):
        if self.lang == 'simple':
            extractor_name = 'enwiki'
        else:
            extractor_name = '{0}wiki'.format(self.lang)
        return extractor_name

    def write_header(self,outfile):
        for column in self.column_list:
            outfile.write('"{0}",'.format(column))
        outfile.write('\n')

    def write_processed_page(self,processed_page,outfile):
        for rev in processed_page['revisions']:
            for column in self.column_list:
                if column in processed_page['page']:
                    outfile.write('"{0}",'.format(processed_page['page'][column]))
                elif column in processed_page['revisions'][rev]:
                    outfile.write('"{0}",'.format(processed_page['revisions'][rev][column]))
                else:
                    message = 'Column name {0} not found in page {1}'.format(column,processed_page['page']['page_id'])
                    raise KeyError(message)
            outfile.write("\n")

    def process_dump(self, n=None, v=False, debug=False):
        # create an iterator for the xml file
        dump = mwxml.Dump.from_file(self.dump_path)
        # create an empty .csv file for the output (processed edits)
        outfile = open(self.outfile_path, 'w')
        # write the csv file header
        self.write_header(outfile)
        # start iterating through the xml dump
        # break after n iterations if the --debug flag has been specified
        extractor_name = self.get_extractor_name()
        quality_extractor = single_revision_template_extractor.load_extractor(extractor_name)
        for page in dump:
            if page.namespace == 1 or page.namespace == 0:
                wpp = WikiPageProcessor(page=page,
                                        lang=self.lang,
                                        quality_extractor=quality_extractor)
                quality_extractor.reset()
                # process page level data
                processed_page = wpp.process()
                self.write_processed_page(processed_page,outfile)
                self.page_count += 1
                self.edit_count += wpp.edit_count
            if self.num_rows == self.page_count:
                break
        utils.log('processed %s pages and %s edits' % (self.page_count, self.edit_count))

def job_script(args):
    return

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
    parser.add_argument('--num_rows',
                        type=int,
                        help='DEBUG: limit the number of rows in the dump file to process')
    parser.add_argument('--no_decompress',
                        action='store_true',
                        help='DEBUG: do not decompress the xml dump or remove the deompressed file after processing')
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
        if args.no_decompress:
            xml_dump = infile
        else:
            dh = DumpHandler(infile)
            xml_dump = dh.decompress()
        # create an object to handle the decompressed xml dump
        wdp = WikiDumpProcessor(lang=args.lang[0],
                                dump_path=xml_dump,
                                outfile_path=outfile,
                                num_rows=args.num_rows)
        wdp.process_dump()
        # remove the decompressed file if the user has not specified the --no_decompress flag
        if not args.no_decompress:
            dh.remove_dump()

if __name__ == "__main__":
    main()