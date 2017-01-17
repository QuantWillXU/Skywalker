from . import pos
from . import timeseries
from . import txn
from . import utils
from ._version import get_versions
from .plotting import *  # noqa
from .tears import *  # noqa

try:
    from . import bayesian
except ImportError:
    warnings.warn(
        "Could not import bayesian submodule due to missing pymc3 dependency.",
        ImportWarning)

__version__ = get_versions()['version']
del get_versions

__all__ = ['utils', 'timeseries', 'pos', 'txn', 'bayesian']
