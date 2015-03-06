import os

export_commands = [('status', 'report status of this repository.')]

def load(options, readonly=False):
	assert 'path' in options

	if 'extensions' in options:
		allowedExtensions = options['extensions'].split(',')
	else:
		allowedExtensions = None

	if 'resursive' in options:
		recursive = bool(options['resursive'])
	else:
		recursive = True

	return FileRepository(options['path'], allowedExtensions = allowedExtensions, recursive = recursive)

class FileRepository:

	def __init__(self, path, allowedExtensions=None, recursive=True):
		self.root = path
		self.allowedExtensions = allowedExtensions
		self.recursive = recursive

	def __enter__(self):
		pass

	def __exit__(self, exc_type, exc_value, traceback):
		pass

	def status(self):
		print("Folder at {}".format(self.root))

	def iterFiles(self):
		"""
		Iterate files in this file repository. Returns a generator of 3-tuples, 
		containing a handle, filename and file extension of the current opened file.
		"""
		for root, dirs, files in os.walk(self.root):
			# iterate files, filtering only allowed extensions
			for f in files:
				fname, ext = os.path.splitext(f)
				if self.allowedExtensions == None or ext in self.allowedExtensions:
					with open(os.path.join(root, f), 'r') as fp:
						yield (fp, f, ext)

			# stop after first iteration if not recursive
			if not self.recursive:
				break

	def close(self):
		pass
