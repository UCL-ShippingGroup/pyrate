"""Generates a default config file in current folder

"""
import os
from configparser import ConfigParser

__author__ = '''Eoin O'Keeffe, Will Usher'''


def gen_default_config(*args):
    """Generates a default config file in current folder

    This command generates a default configuration file and folder structure
    in the current folder.

    The folders generated are:

    repositories
        To hold additional repository code for pyrate
    algorithms
        To hold additional algorithm code for pyrate
    aiscsv
        For AIS csv files (required by algorithms/aisparser.py)
    baddata
        For AIS import logfiles (required by algorithms/aisparser.py)
    """

    file_dir = os.path.dirname(os.path.realpath(__file__))

    default_config = ConfigParser()
    default_config.add_section('globals')

    repo_directory = os.path.join(os.getcwd(), 'repositories')
    if not os.path.exists(repo_directory):
        os.mkdir(repo_directory)
    default_config.set('globals', 'repos', repo_directory)
    algo_directory = os.path.join(os.getcwd(), 'algorithms')
    if not os.path.exists(algo_directory):
        os.mkdir(algo_directory)
    default_config.set('globals', 'algos', algo_directory)

    aiscsv_directory = os.path.join(os.getcwd(), 'aiscsv')
    if not os.path.exists(aiscsv_directory):
        os.mkdir(aiscsv_directory)
    default_config.add_section('aiscsv')
    default_config.set('aiscsv', 'type', 'file')
    default_config.set('aiscsv', 'path', aiscsv_directory)
    default_config.set('aiscsv', 'extensions', '.csv')
    default_config.set('aiscsv', 'unzip', 'True')

    baddata_directory = os.path.join(os.getcwd(), 'baddata')
    if not os.path.exists(baddata_directory):
        os.mkdir(baddata_directory)
    default_config.add_section('baddata')
    default_config.set('baddata', 'type', 'file')
    default_config.set('baddata', 'path', baddata_directory)

    default_config.add_section('aisdb')
    default_config.set('aisdb', 'type', 'aisdb')
    default_config.set('aisdb', 'host', 'localhost')
    default_config.set('aisdb', 'db', 'test_aisdb')
    default_config.set('aisdb', 'user', 'test_ais')
    default_config.set('aisdb', 'ro_user', 'test_ais')
    default_config.set('aisdb', 'pass', 'test_ais')
    default_config.set('aisdb', 'ro_pass', 'test_ais')
    default_config.set('aisdb', 'ro_user', 'test_ais')
    default_config.set('aisdb', 'ro_pass', 'test_ais')

    # now write to file
    with open('aistool.conf', 'w') as config_file:
        default_config.write(config_file)

    print("****************************************************")
    print("aistool.conf has been created in the current working folder")
    print("Please check the settings to ensure they are correct")
    print("....especially postgres database settings")
    print("----------------------------------------------------")
    print("Folder structure for logging has also been created")
    print("****************************************************")
