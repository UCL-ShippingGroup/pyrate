from pyrate.algorithms.aisparser import run, AIS_CSV_COLUMNS, readcsv
from utilities import setup_database
from pyrate.repositories.aisdb import AISdb
from pyrate.repositories import file
from pyrate.algorithms.aisparser import run, AIS_CSV_COLUMNS
from utilities import setup_database
import logging
import os
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
    if headers is None:
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
    def test_script_runs(self, set_tmpdir_environment):
        """
        """
        input_file = os.path.join(str(set_tmpdir_environment), 'test_input.csv')
        print(input_file)
        written_rows = [{'ETA_minute': '45'}, {'IMO': 'Baked', 'Navigational_status': '4'}]
        with open(input_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=AIS_CSV_COLUMNS, dialect="excel")
            writer.writeheader()
            for row in written_rows:
                writer.writerow(row)
        with open(input_file, 'r') as csvfile:
            iterator = readcsv(csvfile)
            for actual, expected in zip(iterator, written_rows):
                for col in expected.keys():
                    assert actual[col] == expected[col]


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
