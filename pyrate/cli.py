"""Provides a command line interface to the pyrate library
"""
import logging
from pyrate import loader
import argparse
from pyrate import get_resource_filename

def main():
    """ The command line interface

    Type `pyrate --help` for help on how to use the command line interface
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # load tool components
    config = loader.DEFAULT_CONFIG
    configfilepath = get_resource_filename('config/default.conf')
    config.read(configfilepath)
    l = loader.Loader(config)

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

    parser_list = subparsers.add_parser('list', help='list loaded data repositories and algorithms')
    parser_list.set_defaults(func=list_components)

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
