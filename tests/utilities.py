import tempfile
from pytest import fixture
from pyrate.repositories.aisdb import AISdb
import logging

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
