#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ammonite.py

Copyright (c) 2013 Alex J Burke. All rights reserved.
"""

import os

FILE_MANIFEST = '_manifest'
SCRIPT_EXT = '.sql'
TABLE_NAME = 'schema_changelog'
TABLE_STRUCTURE = {
    'mysql': """
CREATE TABLE `{0}` (
  `id` int(11) unsigned NOT NULL,
  `applied` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
""",
    'sqlite3': """
CREATE TABLE {0} (
    id int,
    applied text default datetime
)"""
}

class SimpleUpgrader(object):
    """
    Applies upgrade scripts to SQL databases.
    """

    def __init__(self, db_connection, upgrades_path, table_name, table_structure):
        self.connection = db_connection
        self.cursor = self.connection.cursor()
        self.directory = upgrades_path
        self.table = (table_name, table_structure)

    @classmethod
    def for_engine(clss, db_type, db_connection, upgrades_path, table_name=TABLE_NAME):
        if db_type in TABLE_STRUCTURE:
            return clss(db_connection, upgrades_path, table_name, TABLE_STRUCTURE[db_type])
        else:
            raise Exception('unknown database requested %s' % db_type)

    def apply_upgrade(self, upgrades):
        for upgrade_tuple in upgrades:
            # execute the upgrade SQL
            self.cursor.execute(upgrade_tuple[1])

            # write name of applied update to console
            print(' '.join([' >', upgrade_tuple[0]]))

    def create_version_record(self, version):
        query = 'INSERT INTO %s (id) VALUES (%d)' % (self.get_table(), version)
        self.cursor.execute(query)

    def create_version_table(self):
        # substitute table name into table structure
        query = self.table[1].format(self.table[0])
        self.cursor.execute(query)

    def get_active_version(self):
        try:
            # query the log for the highest version (maximum id column value)
            self.cursor.execute('SELECT MAX(id) FROM %s' % self.get_table())
        except:
            # if the table does not exist we get an exception so we return
            # version -1 which will cause the table to be built.
            return -1

        version = self.cursor.fetchone()[0]

        if version is not None:
            return version
        else:
            # the query was successful because the table exists but no upgrade
            # has yet been applied, thus the database is a candidate to face the
            # first upgrade
            return 0

    def get_available_upgrades(self):
        # list out available version upgrade dirs
        versions = os.listdir(self.directory)

        # sort the upgrade version list
        versions.sort()

        return versions

    def get_latest_upgrade_version(self):
        versions = self.get_available_upgrades()

        if len(versions) > 0:
            # return last element which will correspond to the maximum version
            return int(versions[-1])
        else:
            return 0

    def get_table(self):
        return self.table[0]

    def load_upgrade(self, version):
        version_dir = os.path.join(self.directory, str(version))

        try:
            # see if we were given an explicit manifest
            return self.scripts_from_manifestfile(version_dir)
        except IOError as e:
            # if receiving a file not found error treat the directory as
            # containing numbered scripts
            if e.errno == 2:
                return self.scripts_from_upgradedir(version_dir)
            else:
                raise Exception('missing manifest file')

    def load_upgrade_components(self, upgrade_path, manifest):
        components = []
        script_name = None

        # load each upgrade script in the order listed in the manifest file
        for line in manifest:
            script_name = line.rstrip()

            upgrade_tuple = (script_name, self.load_upgrade_script(upgrade_path, script_name))

            # add the SQL string to the array of scripts to be applied
            components.append(upgrade_tuple)

        return components

    def load_upgrade_script(self, script_path, script_name):
        script = ''.join([script_name, SCRIPT_EXT])
        script = os.path.join(script_path, script)

        with open(script) as f:
            return f.read()

        # throw an error if for any reason reading the script failed
        raise Exception("error reading script %s\n" % script_name)

    def perform_upgrade(self):
        active_version = self.get_active_version()
        upgrade_version = self.get_latest_upgrade_version()
        upgrade = None
        version = None

        print("Database currently at version %d." % active_version)

        try:
            # in the case the versioning data is not yet initialised in the
            # database the active version will be -1; use this has our indicator
            # to create the table and then start trying to apply upgrades
            if active_version == -1:
                self.create_version_table()
                active_version = 0
                print("Create version table %s: version => %d" % (self.get_table(), active_version))
        except Exception as e:
            print(str(e))
            raise e

        # write out a newline
        print('')

        if active_version - upgrade_version == 0:
            print("No upgrades to be applied.")
            return 0

        try:
            # loop applying upgrades from active version to latest
            for version in xrange(active_version, upgrade_version):
                # increment version number to account for our range being from
                # active version to < the version we are upgrading to
                version += 1

                upgrade = self.load_upgrade(version)

                print("= upgrading to version %d ..." % version)

                self.apply_upgrade(upgrade)

                # mark upgrade as having been performed
                self.create_version_record(version)

                self.connection.commit()

                if self.cursor.rowcount != 1:
                    raise Exception("could not record the upgrade")

                print("= upgrade complete: version => %d\n" % version)
        except Exception as e:
            self.connection.rollback()
            print('\n'.join(["error upgrading to version %s\n" % version, str(e), ""]))
            return 1

        return 0

    @staticmethod
    def script_prefix(filename):
        fileparts = filename.split('-')

        if (len(fileparts) > 1):
            prefix = fileparts.pop(0)

            try:
                # check that string leading the first hyphen
                # is actually a number whose value is > 0
                # only then do we have a valid ordering key
                return int(prefix) > 0
            except ValueError:
                pass

        # if we've not returned by now we have a problem
        raise Exception('invalid prefix')

    def scripts_from_manifestfile(self, version_dir):
        # access manifest file and load scripts it lists
        with open(os.path.join(version_dir, FILE_MANIFEST)) as manifest:
            return self.load_upgrade_components(version_dir, manifest)

    def scripts_from_upgradedir(self, version_dir):
        # list directory contents
        scripts = os.listdir(version_dir)

        # order the scripts based on their prefix
        scripts.sort(key=SimpleUpgrader.script_prefix)

        # remove the file extensions
        scripts = map(lambda x: x[:x.rfind('.')], scripts)

        return self.load_upgrade_components(version_dir, scripts)
