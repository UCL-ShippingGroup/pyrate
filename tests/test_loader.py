from pyrate.loader import Loader
from utilities import make_temporary_file
from pytest import fixture

class TestLoader:
    """ Tests for the pyrate.loader.Loader class
    """

    @fixture
    def setup_no_global_config(self):
        contents = """
        # Repositories listing
        [aiscsv]
        type = file
        path = /path/to/csv
        extensions = .csv,.xml
        unzip = True

        [baddata]
        type = file
        path = /path/to/log

        [aisdb]
        type = aisdb
        host = localhost
        user = postgres
        pass =
        db = test_aisdb
        """
        tempfile = make_temporary_file()
        with open(tempfile, 'w') as configfile:
            configfile.write(contents)
            self.config = tempfile

    @fixture
    def setup_empty_global_config(self):
        contents = """
        # Repositories listing
        [global]

        [aiscsv]
        type = file
        path = /path/to/csv
        extensions = .csv,.xml
        unzip = True

        [baddata]
        type = file
        path = /path/to/log

        [aisdb]
        type = aisdb
        host = localhost
        user = postgres
        pass =
        db = test_aisdb
        """
        tempfile = make_temporary_file()
        with open(tempfile, 'w') as configfile:
            configfile.write(contents)
            self.config = tempfile


    def test_loader_config_no_global(self, setup_no_global_config):
        """ Tests that repositories and algorithms are correctly loaded

        When using a config file, without a global section, check that the
        bundled repos and algorithms are loaded correctly
        """

        myloader = Loader(self.config)
        algos = list(myloader.get_algorithms())
        repos = list(myloader.get_data_repositories())

        expected_algorithms = ['vesselimporter',
                               'imolist',
                               'aisparser']
        expected_repositories = ['aiscsv',
                                 'baddata',
                                 'aisdb',]
        for expected_algorithm in expected_algorithms:
            assert expected_algorithm in algos
        for expected_repository in expected_repositories:
            assert expected_repository in repos


    def test_loader_config_empty_global(self, setup_empty_global_config):
        """ With an empty global section, check config correctly loaded

        When using a config file, with an empty global section, check that the
        bundled repos and algorithms are loaded correctly
        """

        myloader = Loader(self.config)
        algos = list(myloader.get_algorithms())
        repos = list(myloader.get_data_repositories())

        expected_algorithms = ['vesselimporter',
                               'imolist',
                               'aisparser']
        expected_repositories = ['aiscsv',
                                 'baddata',
                                 'aisdb',]
        for expected_algorithm in expected_algorithms:
            assert expected_algorithm in algos
        for expected_repository in expected_repositories:
            assert expected_repository in repos


    def test_loader_no_config_file(self):
        """ Tests that repositories and algorithms loaded without config file

        When not specifying a config file, check that the packaged default is
        used and bundled repos and algorithms are loaded correctly
        """

        myloader = Loader()
        algos = list(myloader.get_algorithms())
        repos = list(myloader.get_data_repositories())

        expected_algorithms = ['vesselimporter',
                               'imolist',
                               'aisparser']
        expected_repositories = ['aiscsv',
                                 'baddata',
                                 'aisdb',]
        for expected_algorithm in expected_algorithms:
            assert expected_algorithm in algos
        for expected_repository in expected_repositories:
            assert expected_repository in repos
