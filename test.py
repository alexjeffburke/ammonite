#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
upgrade.py

Copyright (c) 2013 Alex J Burke. All rights reserved.
"""

import os
import sqlite3
import unittest

import ammonite

TEST_DATA_PATH = 'test'

def generate_test_data_path(test_name):
    return os.path.join(TEST_DATA_PATH, test_name)

class TestSimpleUpgrader(unittest.TestCase):

    upgrader = ammonite.SimpleUpgrader

    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()

    def tearDown(self):
        self.connection.close()

    def assert_uprade_attempted(self, upgrader):
        self.assertEqual(-1, upgrader.get_active_version(), 'version -1 for empty database with no version table')

        self.assertEqual(0, upgrader.perform_upgrade(), 'perform_upgrade() ran successfully')

    def assert_upgrade_performed(self, upgrader):
        target_version = upgrader.get_latest_upgrade_version()

        self.assert_uprade_attempted(upgrader)

        self.assertEqual(target_version, upgrader.get_active_version(), 'database was upgraded to version %d' % target_version)     

    def test_create_schema_changelog(self):
        upgrader = self.upgrader.for_engine('sqlite3', self.connection, generate_test_data_path('upgrades_none'))

        self.assert_uprade_attempted(upgrader)

        self.assertEqual(0, upgrader.get_active_version(), 'no upgrades applied but active version is 0 (version table added)')

    def test_perform_basic_upgrade(self):
        upgrader = self.upgrader.for_engine('sqlite3', self.connection, generate_test_data_path('upgrades_basic'))

        self.assert_upgrade_performed(upgrader)

    def test_perform_numbered_upgrade(self):
        upgrader = self.upgrader.for_engine('sqlite3', self.connection, generate_test_data_path('upgrades_numbered'))

        self.assert_upgrade_performed(upgrader)

    def test_package_missingprefix(self):
        upgrader = self.upgrader.for_engine('sqlite3', self.connection, generate_test_data_path('packages'))

        with self.assertRaises(Exception):
            upgrader.scripts_from_upgradedir('missing-prefix')


if __name__ == '__main__':
    unittest.main()
