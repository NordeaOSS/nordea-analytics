# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import sys
from pathlib import Path
from sphinx.ext.autodoc import between
import os

na_analytics_src = "\\".join(Path(__file__)._parts[:-3] +['src'])
sys.path.insert(0, na_analytics_src)


# -- Project information -----------------------------------------------------

project = 'Nordea Analytics python library'
copyright = '2022, Nordea'
author = 'Nordea Desk Quants and Markets Advisory Tools'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc', 'autoapi.extension']
autodoc_typehints = 'description'
pdf_documents = [('index', u'rst2pdf', u'Sample rst2pdf docs', u'Desk Qants'),]


autoapi_type = 'python'
cd = os.getcwd()
if 'readthedocs.org' in cd:
    autoapi_dirs = ['/home/docs/checkouts/readthedocs.org/user_builds/nordea-analytics/checkouts/latest']
else:
    autoapi_dirs = [na_analytics_src]


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']
autoapi_ignore = ["*/tests/*",
                  "*noxfile.py", "*conf.py"]
exclude_patterns = ["**nalib"]

# add_function_parentheses = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
