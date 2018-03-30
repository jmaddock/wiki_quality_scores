from WikiPageProcessor import WikiPageProcessor
from DumpHandler import DumpHandler
import mwxml
import argparse
import single_revision_template_extractor
import logging
from xml.etree.ElementTree import ParseError

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
    "new_quality_scores",
    "min_quality",
    "mean_quality",
    "max_quality",
    "parse_error",
    "deleted_text"
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
        # some logging stuff
        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = None
        # keep track of the number of processed pages and edits debug/logging purposes
        self.page_count = 0
        self.edit_count = 0
        self.page_error_count = 0
        self.quality_error_count = 0

    def get_extractor_name(self):
        if self.lang == 'simple':
            extractor_name = 'enwiki'
        else:
            extractor_name = '{0}wiki'.format(self.lang)
        return extractor_name

    def write_header(self,outfile):
        out = ''
        for column in self.column_list:
            out += '"{0}",'.format(column)
        out = out.rstrip(',') + '\n'
        outfile.write(out)

    def write_processed_page(self,processed_page,outfile):
        for rev in processed_page['revisions']:
            out = ''
            for column in self.column_list:
                if column in processed_page['page']:
                    out += '"{0}",'.format(processed_page['page'][column])
                elif column in processed_page['revisions'][rev]:
                    out += '"{0}",'.format(processed_page['revisions'][rev][column])
                else:
                    message = 'Column name {0} not found in page {1}'.format(column,processed_page['page']['page_id'])
                    raise KeyError(message)
            out = out.rstrip(',') + '\n'
            outfile.write(out)

    def process_dump(self):
        # create an iterator for the xml file
        self.logger.info('processing file {0}'.format(self.dump_path))
        dump = mwxml.Dump.from_file(self.dump_path)
        # create an empty .csv file for the output (processed edits)
        outfile = open(self.outfile_path, 'w')
        # write the csv file header
        self.write_header(outfile)
        # start iterating through the xml dump
        # break after n iterations if the --debug flag has been specified
        extractor_name = self.get_extractor_name()
        quality_extractor = single_revision_template_extractor.load_extractor(extractor_name)
        quality_extractor.set_logger(self.logger)
        for page in dump:
            if page.namespace == 1 or page.namespace == 0:
                try:
                    wpp = WikiPageProcessor(page=page,
                                            lang=self.lang,
                                            quality_extractor=quality_extractor,
                                            logger=self.logger)
                    quality_extractor.reset()
                    # process page level data
                    processed_page = wpp.process()
                    self.write_processed_page(processed_page,outfile)
                    self.page_count += 1
                    self.edit_count += wpp.edit_count
                    self.quality_error_count += wpp.error_count
                except ParseError as e:
                    if self.logger:
                        self.logger.warning(e)
                        self.logger.warning('parser error for page: {0}, id: {1}'.format(page.title, page.id))
                    self.page_error_count += 1

            if self.num_rows == self.page_count:
                break
        if self.logger:
            self.logger.info('processed {0} pages and {1} edits'.format(self.page_count, self.edit_count))
            if self.page_error_count > 0 or self.quality_error_count > 0:
                self.logger.warning('handled {0} page parse errors and {1} quality parse errors'.format(self.page_error_count,self.quality_error_count))

def main():
    parser = argparse.ArgumentParser(description='process wiki dumps')
    parser.add_argument('-l','--lang',
                        nargs='*',
                        help='the language of the xml dump')
    parser.add_argument('-i','--infile',
                        help='the file path of a wikipedia xml dump to process')
    parser.add_argument('-o','--outfile',
                        help='a file path for an output .csv of edits')
    parser.add_argument('--num_rows',
                        type=int,
                        help='DEBUG: limit the number of rows in the dump file to process')
    parser.add_argument('--no_decompress',
                        action='store_true',
                        help='DEBUG: do not decompress the xml dump or remove the deompressed file after processing')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='print verbose output')
    parser.add_argument('--log_file',
                        help='a file to log output')
    args = parser.parse_args()
    infile = args.infile
    outfile = args.outfile
    # create an object to handle the compressed dump if it hasn't already been decompressed
    # return a decompressed xml file
    if args.no_decompress:
        xml_dump = infile
    else:
        dh = DumpHandler(infile)
        xml_dump = dh.decompress()
    # create handler to log output to file
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
    # create an object to handle the decompressed xml dump
    wdp = WikiDumpProcessor(lang=args.lang[0],
                            dump_path=xml_dump,
                            outfile_path=outfile,
                            num_rows=args.num_rows,
                            logger=logger)
    wdp.process_dump()
    # remove the decompressed file if the user has not specified the --no_decompress flag
    if not args.no_decompress:
        dh.remove_dump()

if __name__ == "__main__":
    main()