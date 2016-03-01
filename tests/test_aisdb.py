from pyrate.repositories.aisdb import AISdb
import logging

class TestAISdb:
    """ Tests that the AIS database is correctly generated
    """
    def test_load(self):
        options = {'host': 'localhost',
                   'db': 'test_aisdb',
                   'user': 'postgres',
                   'pass': ''}
        aisdb = AISdb(options, readonly=False)
        with aisdb:
            # logging.debug("Connections: {}".format(open_repo.conn.status)
            aisdb.create()
        assert aisdb.tables == ['']
