import os

_repo_ = True
_type_ = "file"
"""
Description of this plugin for super tool.
"""

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

	def iterFiles(self):
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