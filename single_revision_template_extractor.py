from wikiclass.extractors.extractor import TemplateExtractor
from importlib import import_module
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils
import numpy as np

class Single_Revision_Template_Extractor(TemplateExtractor):
    def __init__(self,*args, from_template, possible_labels, **kwargs):
        self.quality_scores = {}
        self.possible_labels = possible_labels
        super().__init__(*args, from_template=from_template, **kwargs)

    def extract(self, revision):
        if revision.page.namespace in self.namespaces:
            # initialize dict to hold project:quality labels for current revision
            # Process all of the revisions looking for new class labels
            revision_text = revision.text or ""
            project_labels = set(pl for pl in
                                 self.extract_labels(revision_text))

            #if len(project_labels) > 0:
            current_quality_scores = {}
            #current_quality_scores = [{project:wp10} for project, wp10 in project_labels]
            for project, wp10 in project_labels:
                current_quality_scores[project] = wp10
            quality_change, new_quality_scores = self.calculate_quality_change(current_quality_scores)
            if quality_change != 0 or new_quality_scores != 0:
                print(revision.page.title)

        else:
            quality_change = 0
            new_quality_scores = 0
        return quality_change, new_quality_scores

    def calculate_quality_change(self,current_quality_scores):
        # the increase or decrease in page quality in the current revision
        quality_change = 0
        # the first time a project is scored
        new_quality_scores = 0
        for project in current_quality_scores:
            if project not in self.quality_scores:
                # if we haven't already seen this project rating before, increment new_quality_scores
                new_quality_scores += 1
            else:
                # if we've already seen this project
                # increment by 1 for increase in rating
                # de-increment by 1 for decrease in rating
                quality_change += np.sign(self.possible_labels.index(current_quality_scores[project]) - self.possible_labels.index(self.quality_scores[project]))
            # log the project and the quality score in the extractors dictionary of quality scores
            self.quality_scores[project] = current_quality_scores[project]
            print(quality_change, new_quality_scores)
            print(current_quality_scores)
        return quality_change, new_quality_scores

    def reset(self):
        self.quality_scores = {}

def load_extractor(extractor_name):
    try:
        return import_module("custom_extractors." + extractor_name)
    except ImportError:
        raise RuntimeError("Could not load extractor for '{0}'"
                           .format(extractor_name))