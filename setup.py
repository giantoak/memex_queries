from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='memex_queries',
    version='0.0.1',
    author='Peter M. Landwehr',
    author_email='peter.landwehr@giantoak.com',
    description='Helper components for querying MEMEX resources',
    keywords='memex elasticsearch hbase requests',
    url='https://www.github.com/giantoak/memex_queries',
    packages=['memex_queries', 'memex_queries.helpers'],
    long_description=open('README.md', 'r').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities'],
)
