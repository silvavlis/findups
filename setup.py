#!/usr/bin/env python3

import setuptools

setuptools.setup(
        name = 'findups',
        version = '15.4',
        description = 'Find duplicates across multiple devices.',
        author = 'Silvano Cirujano Cuesta',
        author_email = 'silvanociru@gmx.net',
        url = 'https://github.com/silvavlis/findups',
        packages = ['findups', 'findups.comparors'],
        include_package_data = True,
        zip_safe = False,
        scripts = ['bin/findups'],
        test_suite = 'findups.test.testcases'
        )



