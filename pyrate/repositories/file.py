import os
import logging
import zipfile
import io

EXPORT_COMMANDS = [('status', 'report status of this repository.')]

def load(options, readonly=False):
    assert 'path' in options

    if 'extensions' in options:
        allowed_extensions = options['extensions'].split(',')
    else:
        allowed_extensions = None

    if 'recursive' in options:
        recursive = bool(options['recursive'])
    else:
        recursive = True

    if 'unzip' in options:
        unzip = bool(options['unzip'])
    else:
        unzip = False

    return FileRepository(options['path'], allowedExtensions=allowed_extensions,
                          recursive=recursive, unzip=unzip)

class FileRepository:

    def __init__(self, path, allowedExtensions=None, recursive=True, unzip=False):
        self.root = path
        self.allowed_extensions = allowedExtensions
        self.recursive = recursive
        self.unzip = unzip

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def status(self):
        print("Folder at {}".format(self.root))

    def iterfiles(self):
        """
        Iterate files in this file repository. Returns a generator of 3-tuples,
        containing a handle, filename and file extension of the current opened file.
        """
        logging.debug("Iterating files in "+ self.root)
        failed_files = []
        for root, _, files in os.walk(self.root):
            # iterate files, filtering only allowed extensions
            for filename in files:
                _, ext = os.path.splitext(filename)
                if self.allowed_extensions == None or ext in self.allowed_extensions:
                    # hitting errors with decoding the data, iso-8859-1 seems to sort it
                    with open(os.path.join(root, filename), 'r', encoding='iso-8859-1') as fp:
                        yield (fp, filename, ext)
                # zip file auto-extract
                elif self.unzip and ext == '.zip':
                    try:
                        with zipfile.ZipFile(os.path.join(root, filename), 'r') as z:
                            for zname in z.namelist():
                                _, ext = os.path.splitext(zname)
                                if self.allowed_extensions == None or ext in self.allowed_extensions:
                                    with z.open(zname, 'r') as fp:
                                        # zipfile returns a binary file, so we require a
                                        # TextIOWrapper to decode it
                                        yield (io.TextIOWrapper(fp, encoding='iso-8859-1'), zname, ext)
                    except (zipfile.BadZipFile, RuntimeError) as error:
                        logging.warning("Unable to extract zip file %s: %s ", filename, error)
                        failed_files.append(filename)
            # stop after first iteration if not recursive
            if not self.recursive:
                break
        if len(failed_files) > 0:
            logging.warning("Skipped %d files due to errors: %s", len(failed_files), repr(failed_files))

    def close(self):
        pass
