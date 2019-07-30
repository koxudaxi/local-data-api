from pathlib import Path

from setuptools import setup
from setuptools.config import read_configuration

PROJECT_PATH = Path(__file__).parent
config = read_configuration(PROJECT_PATH.joinpath('setup.cfg'))

extras_require = {
    'setup': config['options']['setup_requires'],
    'test': config['options']['tests_require'],
    **config['options']['extras_require'],
}

extras_require['all'] = [*extras_require.values()]
kwargs = {'extras_require': extras_require}

if (PROJECT_PATH / '.git').exists():
    use_scm_version = {'write_to': Path(config['metadata']['name'].replace('-', '_'), 'version.py')}
    kwargs['use_scm_version'] = use_scm_version

setup(
    **kwargs
)
