from setuptools import setup, find_packages

setup(  name='database-connector',
        version='1.0.2',
        author='Sungmin Lee',
        author_email='il.sungminlee@gmail.com',
        description="Python-based database connector for MySQL, MongoDB, Azure Table, and plus",
        license="BSD License",
        url="https://github.com/sm-le/python-database-connector",
        package_dir = {'':'src'},
        packages=find_packages(where='src'),
        install_requires=["pymysql","pymongo","DBUtils","azure-data-tables",
                          "azure-identity","azure-keyvault-secrets","python-dotenv",
                          "zstandard","aiohttp","PyYAML"] )