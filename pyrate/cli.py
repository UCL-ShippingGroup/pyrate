"""Provides a command line interface to the pyrate library

The command line interface (CLI) expects that a configuration file named
'aistool.conf' is located in the current folder.

If the config file is not present, a runtime error is raised, and the commands
`set_default` can be used to generate a default configuration file.

"""
import logging
from configparser import ConfigParser
from pyrate import loader
import argparse
import os
from pyrate import get_resource_filename
from pyrate.config_setter import gen_default_config


def main():
    """ The command line interface

    Type `pyrate --help` for help on how to use the command line interface
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # load tool components
    config = ConfigParser()
    configfilepath = 'aistool.conf'
    l=None
    if os.path.exists(configfilepath):
        config.read(configfilepath)
        logger.debug(configfilepath)
        l = loader.Loader(config)
    else:
        logger.warn("The expected configuration file 'aistool.conf' is not present in this folder. "
                           "Please move to the correct folder, or run set_default to initialise "
                           "the current directory.")
        # return

    def list_components(args):
        print("{} repositories:".format(len(l.get_data_repositories())))
        for repository in l.get_data_repositories():
            print("\t" + repository)

        print("{} algorithms:".format(len(l.get_algorithms())))
        for algorithm in l.get_algorithms():
            print("\t" + algorithm)

    def execute_repo_command(args):
        l.execute_repository_command(args.repo, args.cmd)

    def execute_algorithm(args):
        l.execute_algorithm_command(args.alg, args.cmd)

    # set up command line parser
    parser = argparse.ArgumentParser(description="AIS Super tool")
    subparsers = parser.add_subparsers(help='available commands')

    parser_list = subparsers.add_parser('set_default',
                                        help='Setup default config file and folder structure')
    parser_list.set_defaults(func=gen_default_config)

    parser_list = subparsers.add_parser('list',
                                        help='list loaded data repositories and algorithms')
    parser_list.set_defaults(func=list_components)

    if (l != None):
        for r in l.get_data_repositories():
            repo_parser = subparsers.add_parser(r, help='commands for '+ r +' repository')
            repo_subparser = repo_parser.add_subparsers(help=r+' repository commands.')
            for cmd, desc in l.get_repository_commands(r):
                cmd_parser = repo_subparser.add_parser(cmd, help=desc)
                cmd_parser.set_defaults(func=execute_repo_command, cmd=cmd, repo=r)

        for a in l.get_algorithms():
            alg_parser = subparsers.add_parser(a, help='commands for algorithm '+ a +'')
            alg_subparser = alg_parser.add_subparsers(help=a+' algorithm commands.')
            for cmd, desc in l.get_algorithm_commands(a):
                alg_parser = alg_subparser.add_parser(cmd, help=desc)
                alg_parser.set_defaults(func=execute_algorithm, cmd=cmd, alg=a)

    args = parser.parse_args()
    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
