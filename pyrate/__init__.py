from pkg_resources import get_distribution, resource_filename

try:
    __version__ = get_distribution(__name__).version
except:
    __version__ = 'unknown'

def get_resource_filename(resource_name):
    """ Returns the absolute path associated with the file

    Arguments
    =========
    path : str
        The expected file path

    Returns
    =======
    path : str
        The absolute file path
    """
    return resource_filename(__name__, resource_name)
