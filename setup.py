import os

from setuptools import setup, find_packages


setup(
    name='fakeredis',
    version='0.2.0',
    description="Fake implementation of redis API for testing purposes.",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.rst')).read(),
    license='BSD',
    url="https://github.com/jamesls/fakeredis",
    author='James Saryerwinnie',
    author_email='jlsnpi@gmail.com',
    py_modules=['fakeredis'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
    ],
    install_requires=[
        'redis',
    ]
)

