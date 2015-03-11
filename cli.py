import logging
from pyrate import loader
import argparse

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# load tool components
config = loader.default_config
config.read(['aistool.conf'])
l = loader.Loader(config)

def listComponents(args):
	print("{} repositories:".format(len(l.getDataRepositories())))
	for r in l.getDataRepositories():
		print("\t"+r)

	print("{} algorithms:".format(len(l.getAlgorithms())))

	for a in l.getAlgorithms():
		print("\t"+a)

def executeRepoCommand(args):
	l.executeRepositoryCommand(args.repo, args.cmd)

def executeAlgorithm(args):
	l.executeAlgorithmCommand(args.alg, args.cmd)

# set up command line parser
parser = argparse.ArgumentParser(description="AIS Super tool")
subparsers = parser.add_subparsers(help='available commands')

parser_list = subparsers.add_parser('list', help='list loaded data repositories and algorithms')
parser_list.set_defaults(func=listComponents)

for r in l.getDataRepositories():
	repo_parser = subparsers.add_parser(r, help='commands for '+ r +' repository')
	repo_subparser = repo_parser.add_subparsers(help=r+' repository commands.')
	for cmd, desc in l.getRepositoryCommands(r):
		cmd_parser = repo_subparser.add_parser(cmd, help=desc)
		cmd_parser.set_defaults(func=executeRepoCommand, cmd=cmd, repo=r)

for a in l.getAlgorithms():
	alg_parser = subparsers.add_parser(a, help='commands for algorithm '+ a +'')
	alg_subparser = alg_parser.add_subparsers(help=a+' algorithm commands.')
	for cmd, desc in l.getAlgorithmCommands(a):
		alg_parser = alg_subparser.add_parser(cmd, help=desc)
		alg_parser.set_defaults(func=executeAlgorithm, cmd=cmd, alg=a)

args = parser.parse_args()
if 'func' in args:
	args.func(args)
else:
	parser.print_help()
