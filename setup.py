from setuptools import setup, find_packages

with open('requirements.txt') as requirements:
    setup(
        name='multiplechoice.mappings',
        version='1.0.0',
        packages=find_packages(exclude=['tests']),
        install_requires=requirements.read().splitlines(),
        url='https://github.com/multiplechoice/sqlalchemy-mappings',
        license='MIT',
        author='aodj',
        author_email='alexodonovanjones@gmail.com',
        description='Common mappings to db objects that need to be shared',
        long_description=open('README.rst').read(),
        keywords='sqlalchemy postgresql'
    )
