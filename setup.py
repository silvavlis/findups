#!/usr/bin/env python

import setuptools

setuptools.setup(
        name = 'findups',
        version = '15.1',
        description = 'Find duplicates',
        author = 'Silvano Cirujano Cuesta',
        author_email = 'silvanociru@gmx.net',
        url = 'https://github.com/silvavlis/findups',
        packages = setuptools.find_packages(exclude = ['ez_setup']),
        include_package_data = True,
        zip_safe = False,
        scripts = ['bin/findups'],
        install_requires = ['sqlite'],
        test_suite = 'findups.test.testcases'
        )



