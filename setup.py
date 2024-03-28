from setuptools import setup, find_packages

setup(  name='database-connector',
        version='1.0.0',
        author='smlee',
        author_email='smlee@seegene.com',
        description="Python-based database connector for MySQL, MongoDB, and Azure Table",
        license="MIT License",
        url="https://github.com/sm-le/python-database-connector",
        package_dir = {'':'src'},
        packages=find_packages(where='src'),
        install_requires=["pymysql","pymongo","DBUtils","azure-data-tables",
                          "azure-identity","azure-keyvault-secrets","python-dotenv",
                          "zstandard"] )