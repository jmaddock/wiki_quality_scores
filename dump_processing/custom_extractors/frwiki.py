import logging
import re
import sys
import traceback

from single_revision_template_extractor import Single_Revision_Template_Extractor

logger = logging.getLogger(__name__)

# TODO: fix this to actually return the name of the project
PROJECT_NAME = "wikiprojet"

POSSIBLE_LABELS = ("e", "bd", "b", "a", "ba", "adq")

def from_template(template):
    template_name = str(template.name).lower().strip()
    if template_name == "wikiprojet" and template.has_param('avancement'):
        try:
            label = normalize_label(template.get('avancement').value)

            if label is not None:
                return PROJECT_NAME, label
            else:
                logger.debug("Class '{0}' not in possible classes."
                             .format(template.get('avancement').value))
                pass  # not a quality assessment class

        except ValueError as e:
            logger.warning(traceback.format_exc())
            pass  # no assessment class in template


LABEL_MATCHES = [
    ("e", re.compile(r"\be|ébauche\b", re.I)),
    ("bd", re.compile(r"\bbd|bon[ _]?début\b", re.I)),
    ("b", re.compile(r"\bb\b", re.I)),
    ("a", re.compile(r"\ba\b", re.I)),
    ("ba", re.compile(r"\bba\b", re.I)),
    ("adq", re.compile(r"\badq\b", re.I))
]


def normalize_label(value):
    value = str(value.strip_code()).lower().replace("_", " ").strip()

    for label, regex in LABEL_MATCHES:
        if regex.match(value):
            return label

    return None


sys.modules[__name__] = Single_Revision_Template_Extractor(
    __name__,
    doc="""
wikiclass.extractors.frwiki
+++++++++++++++++++++++++++

This extractor looks for instances of the "wikiprojet" template on article talk
pages (namespace = 1) with a parameter called "avancement".  All `project` s
are hard-coded to "wikiprojet"
""",
    namespaces={1},
    from_template=from_template,
    possible_labels=POSSIBLE_LABELS)