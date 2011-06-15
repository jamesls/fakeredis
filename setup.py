from setuptools import setup, find_packages

setup(
    name='fakeredis',
    version='0.1',
    description="Fake implementation of redis API for testing purposes.",
    license='BSD',
    author='James Saryerwinnie',
    author_email='jlsnpi@gmail.com',
    modules=['fakeredis'],
    entry_points={
        'console_scripts': ['lmsh = labmanager.shell:main'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha'
        'License :: OSI Approved :: BSD License',
    ],
    install_requires=[
        'redis',
    ]
)

