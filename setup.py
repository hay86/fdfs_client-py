#!/usr/bin/env python
import os
from fdfs_client import __version__

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

f = open(os.path.join(os.path.dirname(__file__), 'README.md'))
long_description = f.read()
f.close()

sdict = {
    'name': 'fdfs_client-py',
    'version': __version__,
    'description': 'Python client for Fastdfs ver 4.06',
    'long_description': long_description,
    'author': 'scott yuan',
    'author_email': 'scottzer8@gmail.com',
    'maintainer': 'scott yuan',
    'maintainer_email': 'scottzer8@gmail.com',
    'keywords': ['Fastdfs', 'Distribute File System'],
    'license': 'GPLV3',
    'packages': ['fdfs_client'],
    'classifiers': [
        'Development Status :: 1 - Production/Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: GPLV3',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
    'ext_modules': [Extension('fdfs_client.sendfile',
                              sources=['fdfs_client/sendfilemodule.c'])],
}

setup(**sdict)

