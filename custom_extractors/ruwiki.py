import logging
import re
import sys
import traceback

from single_revision_template_extractor import Single_Revision_Template_Extractor

logger = logging.getLogger(__name__)

PROJECT_NAME = "wikiproject"
PROJECT_RE = re.compile(r"^статья[\s_]проекта\b", re.I)
POSSIBLE_LABELS = ("I","II", "III", "IV", "ДС", "ХС", "ИС")


def from_template(template):
    template_name = str(template.name).lower().strip()
    if PROJECT_RE.match(template_name) \
       and template.has_param("уровень"):
        try:
            label = normalize_label(template.get("уровень").value)
            if label is not None:
                return PROJECT_NAME, label
            else:
                logger.debug("Class '{0}' not in possible classes."
                             .format(template.get("уровень").value))
                pass  # not a quality assessment class

        except ValueError:
            logger.warning(traceback.format_exc())
            pass  # no assessment class in template


LABEL_MATCHES = [
    ("ИС", re.compile(r"ис", re.I)),    # featured article
    ("ХС", re.compile(r"хс", re.I)),    # good article
    ("ДС", re.compile(r"дс", re.I)),    # strong article
    ("I", re.compile(r"^i$", re.I)),    # Level I
    ("II", re.compile(r"^ii$", re.I)),  # Level II
    ("III", re.compile(r"iii", re.I)),  # Level III
    ("IV", re.compile(r"iv", re.I))     # Level IV
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
wikiclass.extractors.enwiki
+++++++++++++++++++++++++++
This extractor looks for instances of templates that contain
"class=<some class>" on article talk pages (namespace = 1) and parses the
template name to obtain a `project`.
""",
    namespaces={1},
    from_template=from_template,
    possible_labels=POSSIBLE_LABELS
)