import os
import logging
import zipfile
import io

export_commands = [('status', 'report status of this repository.')]

def load(options, readonly=False):
	assert 'path' in options

	if 'extensions' in options:
		allowedExtensions = options['extensions'].split(',')
	else:
		allowedExtensions = None

	if 'recursive' in options:
		recursive = bool(options['recursive'])
	else:
		recursive = True

	if 'unzip' in options:
		unzip = bool(options['unzip'])
	else:
		unzip = False

	return FileRepository(options['path'], allowedExtensions = allowedExtensions, recursive = recursive, unzip = unzip)

class FileRepository:

	def __init__(self, path, allowedExtensions=None, recursive=True, unzip=False):
		self.root = path
		self.allowedExtensions = allowedExtensions
		self.recursive = recursive
		self.unzip = unzip

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
		logging.debug("Iterating files in "+ self.root)
		for root, dirs, files in os.walk(self.root):
			# iterate files, filtering only allowed extensions
			for f in files:
				fname, ext = os.path.splitext(f)
				if self.allowedExtensions == None or ext in self.allowedExtensions:
					with open(os.path.join(root, f), 'r') as fp:
						yield (fp, f, ext)
				# zip file auto-extract
				elif self.unzip and ext == '.zip':
					with zipfile.ZipFile(os.path.join(root, f), 'r') as z:
						for zname in z.namelist():
							zfname, ext = os.path.splitext(zname)
							if self.allowedExtensions == None or ext in self.allowedExtensions:
								with z.open(zname, 'r') as fp:
									# zipfile returns a binary file, so we require a TextIOWrapper to decode it
									yield (io.TextIOWrapper(fp, encoding='ascii'), zname, ext)

			# stop after first iteration if not recursive
			if not self.recursive:
				break

	def close(self):
		pass
