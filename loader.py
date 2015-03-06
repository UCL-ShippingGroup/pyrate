import logging
import imp
import pkgutil
import inspect
import contextlib

def loadModule(name, paths):
	fp, pathname, description = imp.find_module(name, paths)
	if fp != None:
		return imp.load_module(name, fp, pathname, description)
	else:
		return None

def loadAllModules(paths):
	modules = {}
	for importer, name, package in pkgutil.walk_packages(paths):
		try:
			modules[name] = loadModule(name, paths)
		except ImportError as e:
			logging.warn("Error importing module "+ name +": {}".format(e))
	return modules


class Loader:

	def __init__(self, config):
		repopaths = config.get('globals', 'repos', fallback='./repositories')
		repopaths = repopaths.split(',')

		# load repo drivers from repopaths
		repoDrivers = loadAllModules(repopaths)

		# get repo configurations from config
		repoConfig = set(config.sections()) - set(['globals'])

		# check which repos we have drivers for
		repoConfDict = {}
		for r in repoConfig:
			conf = config[r]
			if not 'type' in conf:
				logging.warning("Repository "+ r +" does not specify a type in the config file.")
			elif not conf['type'] in repoDrivers:
				logging.warning("Driver of type "+ conf['type'] +"for repository "+ r +" not found.")
			else:
				repoConfDict[r] = conf

		algopaths = config.get('globals', 'algos', fallback='./algorithms')
		algopaths = algopaths.split(',')

		# load algorithms from algopaths
		algorithms = loadAllModules(algopaths)

		self.repoDrivers = repoDrivers
		self.repoConfig = repoConfDict
		self.algorithms = algorithms

	def getDataRepositories(self):
		return self.repoConfig.keys()

	def getRepositoryCommands(self, repoName):
		try:
			return self.repoDrivers[self.repoConfig[repoName]['type']].export_commands
		except AttributeError:
			return []

	def getAlgorithmCommands(self, algname):
		try:
			return self.algorithms[algname].export_commands
		except AttributeError:
			return []

	def getAlgorithms(self):
		return self.algorithms.keys()

	def executeRepositoryCommand(self, reponame, command):
		if not command in [c[0] for c in self.getRepositoryCommands(reponame)]:
			raise ValueError("Invalid command {} for repository {}".format(command, reponame))
		# load repostory class
		repo = self.getDataRepository(reponame)
		fns = inspect.getmembers(repo, lambda x : inspect.ismethod(x) and x.__name__ == command)
		if len(fns) != 1:
			raise RuntimeError("Unable to find method {} in repository {}: ".format(command, reponame, repo))
		with repo:
			# call command
			fns[0][1]()

	def executeAlgorithmCommand(self, algname, command):

		alg = self.getAlgorithm(algname)
		fns = inspect.getmembers(alg, lambda x : inspect.isfunction(x) and x.__name__ == command)
		if len(fns) != 1:
			raise RuntimeError("Unable to find function {} in algorithm {}: ".format(command, algname, alg))

		# get inputs and outputs
		inputs = {}
		outputs = {}

		for inp in alg.inputs:
			inputs[inp] = self.getDataRepository(inp, readonly=True)
		for out in alg.outputs:
			outputs[out] = self.getDataRepository(out)

		with contextlib.ExitStack() as stack:
			# prepare repositories
			[stack.enter_context(inputs[i]) for i in inputs]
			[stack.enter_context(outputs[i]) for i in outputs]
			fns[0][1](inputs, outputs)

	def getDataRepository(self, name, readonly=False):
		return self.repoDrivers[self.repoConfig[name]['type']].load(self.repoConfig[name], readonly=readonly)

	def getAlgorithm(self, name):
		return self.algorithms[name]
