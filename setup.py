from setuptools import setup, find_packages

install_requires = ["uvicorn", "fastapi"]

setup(
    name='tableui',
    version='0.0.2',
    author='Bob Weigel',
    author_email='rweigel@gmu.edu',
    packages=find_packages(),
    license='LICENSE.txt',
    description='Serve a SQL database as a web page using DataTables.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=install_requires,
    package_data={
        'tableui': ['ui/**/*'],
    },
    entry_points={
        'console_scripts': [
            'tableui-serve=tableui.cli:main',
        ],
    },
)
