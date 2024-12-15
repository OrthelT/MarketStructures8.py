from setuptools import setup, find_packages

setup(
    name='ESIMT',
    version='2.0',
    packages=find_packages(),
    install_requires=[  # Any packages your code needs to run
        'pandas',
        'sqlalchemy',
        'requests'
        'sqlite3'
        'logging'
        'numpy'
        'requests_oauthlib'
        'python-dotenv'
        'oauthlib'
        'os'
        'datetime'
    ]

)
