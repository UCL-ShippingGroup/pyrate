""" Tests the creation of tables, and the methods of the sql class
"""
from pyrate.repositories.sql import Table
from utilities import setup_database


class TestSql:
    """ Tests the Sql class
    """
    def test_get_list_of_columns(self, setup_database):
        db = setup_database
        rows = [{'unit': 'days',
                 'description': 'At berth/anchor',
                 'name': 's_berth_day'},
                 {'unit': 'SOG / kts',
                 'description': 'Average at sea',
                 'name': 's_av_sea'}]

        with db:
            actual = db.clean._get_list_of_columns(rows[0])

        assert isinstance(actual, str)
        assert actual.endswith(')')
        assert actual[0] == '('
        actual_contents = actual.strip('()').split(',')
        expected = ['description','name','unit']
        for expected_column in expected:
            assert expected_column in actual_contents

    def test_get_list_of_columns_lowerconversion(self, setup_database):
        db = setup_database
        rows = [{'uNit': 'days',
                 'Description': 'At berth/anchor',
                 'namE': 's_berth_day'},
                 {'unit': 'SOG / kts',
                 'description': 'Average at sea',
                 'name': 's_av_sea'}]

        with db:
            actual = db.clean._get_list_of_columns(rows[0])

        assert isinstance(actual, str)
        assert actual.endswith(')')
        assert actual[0] == '('
        actual_contents = actual.strip('()').split(',')
        expected = ['description','name','unit']
        for expected_column in expected:
            assert expected_column in actual_contents
