from wikiclass.extractors.extractor import TemplateExtractor
from importlib import import_module
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import mwparserfromhell
from collections import namedtuple

## named tuple for storing a revision timestamp, project names, and quality scores
## quality list should be a tuple of the form (project_name,quality_score)
RevisionQuality = namedtuple('RevisionQuality',
                             ['quality_list','ts','id','error'])

ProcessedRevisionQuality = namedtuple('ProcessedRevisionQuality',
                                      ['min','max','mean','new','change','id','error'])

class Single_Revision_Template_Extractor(TemplateExtractor):
    def __init__(self,*args, from_template, possible_labels, **kwargs):
        # a list to store RevisionQuality tuples
        self.revision_list = []
        self.possible_labels = possible_labels
        # logging stuff
        self.logger = None
        super().__init__(*args, from_template=from_template, **kwargs)

    ## add a logger to the template extractor
    def set_logger(self,logger):
        self.logger = logger

    def reset(self):
        self.revision_list = []

    ## extract the wikiproject name and quality assessment
    ## append name, quality, and timestamp to self.revision_list
    def extract(self, revision):
        quality_list = None
        error = False
        # ignore all deleted revisions or revisions that are not talk pages
        if revision.page.namespace in self.namespaces and not revision.deleted:
            # initialize dict to hold project:quality labels for current revision
            # Process all of the revisions looking for new class labels
            revision_text = revision.text or ""
            try:
                project_labels = set(pl for pl in self.extract_labels(revision_text))
                # create a list of tuples of the form (project_name,quality_score)
                quality_list = [(project, wp10) for project, wp10 in project_labels]
            # catch parse errors from the template parser
            # assume new quality scores and quality change are both 0
            except mwparserfromhell.parser.ParserError as e:
                if self.logger:
                    self.logger.warning(e)
                    self.logger.warning('parser error for page: {0}, id: {1}'.format(revision.page.title,revision.id))
                error = True

        # create a new namedtuple that includes all quality scores and the revision timestamp for sorting
        rq = RevisionQuality(
            quality_list=quality_list,
            ts=revision.timestamp,
            id=revision.id,
            error=error
        )
        self.revision_list.append(rq)

    def calculate_quality_change(self):
        # sort the list of revisions by date
        sorted_revision_list = sorted(self.revision_list, key=lambda rq: rq.ts)
        # dict of project names and quality scores that we've already seen
        quality_score_dict = {}
        # iterate through the sorted list of revisions
        for rev in sorted_revision_list:
            qmin = None
            qmean = None
            qmax = None
            change = None
            new = None
            error = False
            if rev.error:
                error = True
            elif rev.quality_list:
                quality_list = []
                change = 0
                new = 0
                # iterate through all of the project ratings within a revision
                for project, current_quality_score in rev.quality_list:
                    quality_list += current_quality_score
                    if project in quality_score_dict:
                        change += np.sign(self.possible_labels.index(current_quality_score) - self.possible_labels.index(self.quality_score_dict[project]))
                    else:
                        new += 1
                qmin = min(quality_list),
                qmean = np.mean(quality_list),
                qmax = max(quality_list),

            yield ProcessedRevisionQuality(
                new=new,
                change=change,
                min=qmin,
                mean=qmean,
                max=qmax,
                id=rev.id,
                error=error
            )

def load_extractor(extractor_name,**kwargs):
    try:
        return import_module("custom_extractors." + extractor_name)
    except ImportError:
        raise RuntimeError("Could not load extractor for '{0}'"
                           .format(extractor_name))