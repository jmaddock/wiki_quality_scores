import mwreverts
import translations
import datetime
import single_revision_template_extractor

QUOTE_ESCAPE_CHAR = '&quot'

class WikiPageProcessor(object):

    def __init__(self,page,lang,**kwargs):
        self.page = page
        self.lang = lang
        self.revision_dict = {}
        self.revert_dectector = mwreverts.Detector()
        # create a new quality extractor for each page so we automatically reset its quality score dict
        if 'quality_extractor' in kwargs:
            if type(kwargs['quality_extractor']) == single_revision_template_extractor.Single_Revision_Template_Extractor:
                # if passed an extractor object, just use that object
                # use this implementation for performance reasons
                self.quality_extractor = kwargs['quality_extractor']
            elif kwargs['quality_extractor'] == 'infer':
                # otherwise try to infer the quality extractor name from the language
                extractor_name = self.get_extractor_name()
                self.quality_extractor = single_revision_template_extractor.load_extractor(extractor_name)
        else:
            self.quality_extractor = None
        self.edit_count = 0
        # some logging stuff
        if 'logger' in kwargs:
            self.logger = kwargs['logger']
        else:
            self.logger = None


    def get_extractor_name(self):
        if self.lang == 'simple':
            extractor_name = 'enwiki'
        else:
            extractor_name = '{0}wiki'.format(self.lang)
        return extractor_name

    def process_page(self):
        # results dictionary for each page
        d = {}
        # get the page id
        d['page_id'] = self.page.id
        # get the namespace (0 or 1)
        d['namespace'] = self.page.namespace
        # replace quote chars with an escape character, remove trailing spaces, and convert to lowercase
        stripped_title = self.page.title.replace('"', QUOTE_ESCAPE_CHAR).strip()  # .lower()
        # capture the full title w/ escaped quotes
        d['full_title'] = stripped_title
        # if the page is a talk page, strip "Talk:" from the title
        if self.page.namespace == 1:
            stripped_title = stripped_title.split(':', 1)[-1]
        # remove trailing "/archive" from the title
        # get the archive number or title (if any)
        if len(stripped_title.split(
                '/{0}'.format(translations.translations['archive'][self.lang]))) > 1 and self.page.namespace == 1:
            d['title'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[0]
            d['archive'] = stripped_title.split('/{0}'.format(translations.translations['archive'][self.lang]))[
                1].strip()
            # if no text follows "/archive" in the title, add a 0
            if len(d['archive']) < 1:
                d['archive'] = 0
        else:
            d['archive'] = None
            d['title'] = stripped_title
        return d

    ## process the data for each revision
    ## return a tuple in the form (revision_id,{revision_data})
    def process_edit(self,rev):
        r = {}
        # indicate if the revision was deleted or restricted by the user
        if rev.deleted:
            r['deleted'] = True
        else:
            r['deleted'] = False
        # replace quote chars in user text
        if rev.user and rev.user.text:
            r['user_text'] = rev.user.text.replace('"', '')
            r['user_id'] = rev.user.id
        elif not rev.user:
            r['user_text'] = None
            r['user_id'] = None
        # get the datetime of the edit
        r['ts'] = str(datetime.datetime.fromtimestamp(rev.timestamp))
        self.edit_count += 1
        return r

    ## identify the whether this edit is a revert and flag all reverted edits
    def process_revert(self,rev):
        revert = self.revert_dectector.process(rev.sha1, rev.id)
        if revert is not None:
            # This revision is a revert.
            for rev_id in revert.reverteds:
                self.revision_dict[rev.id]['revert'] = 'reverting'
                if rev_id in self.revision_dict:
                    self.revision_dict[rev_id]['revert'] = 'reverted'
        else:
            self.revision_dict[rev.id]['revert'] = None

    def process(self):
        # get the page level data
        p = self.process_page()
        for rev in self.page:
            # get revision level data
            self.revision_dict[rev.id] = self.process_edit(rev)
            # update reverteds if the edit is a revert
            self.process_revert(rev)
            if self.quality_extractor:
                self.revision_dict[rev.id]['quality_change'], self.revision_dict[rev.id]['new_quality_scores'] = self.quality_extractor.extract(rev)
        if self.logger:
            self.logger.debug('processed {0} edits from page {1}'.format(self.edit_count,self.page.title))
        return {'page':p,'revisions':self.revision_dict}
