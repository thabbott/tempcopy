import os
import tempfile

_tempcopy_dir = os.path.join(tempfile.gettempdir(), '.tempcopy')
if '_TEMPCOPY_DIR' in os.environ:
    _tempcopy_dir = os.environ['_TEMPCOPY_DIR']

config = {
    'tempcopy_dir': _tempcopy_dir
}
"""
The config dictionary contains global configuration values for tempcopy.

The config is defined in ``tempcopy/__init__.py``. Values can be overwridden in two
ways: either by manually setting dictionary keys, or by setting environment
variables.

Keys in the config dictionary:

``tempcopy_dir``
    The absolute path to a directory where `tempcopy` stores temporary
    copies of files. The default value is set to 
    `tempfile.gettempdir()/.tempcopy`, and the value can be overwritten by 
    setting the `_TEMPCOPY_DIR` environment variable.
"""
