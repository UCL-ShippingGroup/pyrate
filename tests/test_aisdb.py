from pyrate.repositories.aisdb import AISdb
from pyrate.repositories import file
from pyrate.algorithms.aisparser import run
import logging
from pytest import fixture

@fixture(scope='function')
def setup_database(request):
    """ Creates the required tables in the test_aisdb postgres database

    Returns
    -------
    aisdb : pyrate.repositories.aisdb.AISdb
        An instance of the AISdb class with tables created

    """
    options = {'host': 'localhost',
               'db': 'test_aisdb',
               'user': 'postgres',
               'pass': ''}
    aisdb = AISdb(options, readonly=False)
    with aisdb:
        aisdb.create()
    def teardown():
        logging.debug("Tearing down database tables")
        with aisdb:
            cursor = aisdb.conn.cursor()
            cursor.execute("drop schema public cascade;")
    request.addfinalizer(teardown)
    return aisdb

@fixture(scope='function')
def setup_input_csv_file():
    """ Returns an open input file object

    Returns
    -------
    pyrate.repositories.file.FileRepository

    """
    input_path = '/Users/will2/repository/exactEarth/tests/ais_import/data/ais'
    input_options = {'path': input_path,
                     'extensions': '.csv',
                     'unzip': False,
                     'recursive': False}
    return file.load(input_options, readonly=True)

@fixture(scope='function')
def setup_log_csv_file():
    """ Returns an open log file object

    Returns
    -------
    pyrate.repositories.file.FileRepository

    """
    log_path = '/Users/will2/repository/exactEarth/tests/ais_import/data/bad'
    log_options = {'path': log_path,
                   'extensions': '.csv',
                   'unzip': False,
                   'recursive': False}
    return file.load(log_options, readonly=False)

class TestParsing():
    """ Tests for parsing AIS data
    """

    def test_parser(self, setup_database, setup_input_csv_file, setup_log_csv_file):
        """
        """
        input_object = setup_input_csv_file
        bad_object = setup_log_csv_file

        database = setup_database
        inputs = {'aiscsv': input_object}
        outputs = {'aisdb': database,
                   'baddata': bad_object}
        with database:
            run(inp=inputs, out=outputs, dropindices=False, source=0)
        assert 0 == 1
