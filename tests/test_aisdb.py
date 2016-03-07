from pyrate.repositories.aisdb import AISdb
from pyrate.repositories import file
from pyrate.algorithms.aisparser import run, AIS_CSV_COLUMNS
import logging
import tempfile
from pytest import fixture
import csv

def make_temporary_file():
    """ Returns a temporary file name

    Returns
    =========
    openfile.name : str
        Name of the temporary file
    """
    with tempfile.NamedTemporaryFile() as openfile:
        return openfile.name

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
            cursor.execute("create schema public;")
            aisdb.conn.commit()
    request.addfinalizer(teardown)
    return aisdb

@fixture(scope='function')
def setup_input_csv_file():
    """ Returns an open input file object

    Returns
    -------
    pyrate.repositories.file.FileRepository

    """
    input_path = generate_test_input_data()
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
    log_path = make_temporary_file()
    log_options = {'path': log_path,
                   'extensions': '.csv',
                   'unzip': False,
                   'recursive': False}
    return file.load(log_options, readonly=False)

def generate_test_input_data(headers=None):
    """ Generates a temporary csv file with the ais headers

    Arguments
    ---------
    headers : optional
        A list of headers for the temp csv file

    Returns
    -------
    tempfile : tempfile.NamedTemporaryFile
        A csv file with a row of AIS headers
    """
    if headers == None:
        headers = AIS_CSV_COLUMNS
    assert isinstance(headers, list)

    tempfile = make_temporary_file()

    with open(tempfile, 'w+') as csvfile:
        writer = csv.writer(csvfile, dialect='excel')
        writer.writerow(headers)
    return tempfile


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
