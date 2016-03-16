"""This module provides the Loader class which loads a pyrate session from a
configuation file. This session can then run tasks on data repositories and
algorithms."""

import logging
import imp
import pkgutil
import inspect
import contextlib
from configparser import ConfigParser
from pyrate import get_resource_filename

def load_module(name, paths):
    """Load module name using the given search paths."""
    handle, pathname, description = imp.find_module(name, paths)
    if handle != None:
        return imp.load_module(name, handle, pathname, description)
    else:
        return None

def load_all_modules(paths):
    """Load all modules on the given paths."""
    modules = {}
    for _, name, _ in pkgutil.walk_packages(paths):
        try:
            modules[name] = load_module(name, paths)
        except ImportError as error:
            logging.warn("Error importing module "+ name +": {}".format(error))
    return modules

configfilepath = get_resource_filename('config/default.conf')

DEFAULT_CONFIG = ConfigParser()
DEFAULT_CONFIG.add_section('globals')
DEFAULT_CONFIG.set('globals', 'repos', get_resource_filename('repositories'))
DEFAULT_CONFIG.set('globals', 'algos', get_resource_filename('algorithms'))

class Loader:
    """The Loader joins together data repositories and algorithms,
    and executes operations on them."""

    def __init__(self, config=None):
        # load from file if path provided
        loaded_conf = ConfigParser()
        if config is None:
            loaded_conf.read(configfilepath)
            config = loaded_conf
        else:
            if isinstance(config, str):
                loaded_conf.read(config)
                config = loaded_conf

        if 'globals' in config:
            repopaths = str(config.get('globals', 'repos'))
            repopaths = repopaths.split(',')
            repopaths.extend([get_resource_filename('repositories')])
        else:
            repopaths = [get_resource_filename('repositories')]

        logging.debug("Paths to repositories: {}".format(repopaths))

        # load repo drivers from repopaths
        repo_drivers = load_all_modules(repopaths)

        # get repo configurations from config
        if 'globals' in config:
            repo_config = set(config.sections()) - set(['globals'])
        else:
            repo_config = set(config.sections())


        # check which repos we have drivers for
        repo_conf_dict = {}
        for repo_name in repo_config:
            conf = config[repo_name]
            if not 'type' in conf:
                logging.warning("Repository "+ repo_name +" does not specify a type in the config file.")
            elif not conf['type'] in repo_drivers:
                logging.warning("Driver of type "+ conf['type'] +" for repository "+ repo_name +" not found.")
            else:
                repo_conf_dict[repo_name] = conf

        if 'globals' in config:
            algopaths = str(config.get('globals', 'algos'))
            algopaths = algopaths.split(',')
            algopaths.extend([get_resource_filename('algorithms')])
        else:
            algopaths = [get_resource_filename('algorithms')]

        # load algorithms from algopaths
        logging.debug("Paths to algorithms: {}".format(algopaths))
        algorithms = load_all_modules(algopaths)

        self.repo_drivers = repo_drivers
        self.repo_config = repo_conf_dict
        self.algorithms = algorithms

    def get_data_repositories(self):
        """Returns a set of the names of available data repositories"""
        return self.repo_config.keys()

    def get_repository_commands(self, repo_name):
        """Returns a list of available commands for the specified repository"""
        try:
            return self.repo_drivers[self.repo_config[repo_name]['type']].EXPORT_COMMANDS
        except AttributeError:
            return []

    def get_algorithm_commands(self, algname):
        """Returns a list of available commands for the specified algorithm"""
        try:
            return self.algorithms[algname].EXPORT_COMMANDS
        except AttributeError:
            return []

    def get_algorithms(self):
        """Returns a set of the names of available algorithms"""
        return self.algorithms.keys()

    def execute_repository_command(self, reponame, command, **args):
        """Execute the specified command on the specified repository."""
        if not command in [c[0] for c in self.get_repository_commands(reponame)]:
            raise ValueError("Invalid command {} for repository {}".format(command, reponame))
        # load repostory class
        repo = self.get_data_repository(reponame)
        fns = inspect.getmembers(repo, lambda x: inspect.ismethod(x) and x.__name__ == command)
        if len(fns) != 1:
            raise RuntimeError("Unable to find method {} in repository {}: {}".format(command, reponame, repo))
        with repo:
            # call command
            fns[0][1](**args)

    def execute_algorithm_command(self, algname, command, **args):
        """Execute the specified command on the specified algorithm"""
        alg = self.get_algorithm(algname)
        fns = inspect.getmembers(alg, lambda x: inspect.isfunction(x) and x.__name__ == command)
        if len(fns) != 1:
            raise RuntimeError("Unable to find function {} in algorithm {}: {}".format(command, algname, alg))

        # get inputs and outputs
        inputs = {}
        outputs = {}

        for inp in alg.INPUTS:
            inputs[inp] = self.get_data_repository(inp, readonly=True)
        for out in alg.OUTPUTS:
            outputs[out] = self.get_data_repository(out)

        with contextlib.ExitStack() as stack:
            # prepare repositories
            for i in inputs:
                stack.enter_context(inputs[i])
            for i in outputs:
                stack.enter_context(outputs[i])
            fns[0][1](inputs, outputs, **args)

    def get_data_repository(self, name, readonly=False):
        """Returns a loaded instance of the specified data repository."""
        return self.repo_drivers[self.repo_config[name]['type']].load(self.repo_config[name], readonly=readonly)

    def get_algorithm(self, name):
        """Returns the algorithm module specified."""
        return self.algorithms[name]
