from wikiclass.extractors.extractor import TemplateExtractor
from importlib import import_module
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils

class Single_Revision_Template_Extractor(TemplateExtractor):
    def __init__(self,*args,from_template,**kwargs):
        super().__init__(*args, from_template=from_template, **kwargs)

    def extract(self, revision):
        if revision.page.namespace in self.namespaces:
            # initialize dict to hold project:quality labels for current revision
            labelings = {}
            # initialize set to hold project:quality labels
            new_labels = set()

            # Process all of the revisions looking for new class labels
            revision_text = revision.text or ""
            project_labels = set(pl for pl in
                                 self.extract_labels(revision_text))

            if len(project_labels) > 0:
                labelings = [(project,wp10) for project, wp10 in project_labels]
                return labelings
        else:
            return None

def load_extractor(extractor_name):
    try:
        return import_module("custom_extractors." + extractor_name)
    except ImportError:
        raise RuntimeError("Could not load extractor for '{0}'"
                           .format(extractor_name))