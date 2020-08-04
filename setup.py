import os

from setuptools import setup


setup(
    name='fakeredis',
    version='1.4.2',
    description="Fake implementation of redis API for testing purposes.",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.rst')).read(),
    license='BSD',
    url="https://github.com/jamesls/fakeredis",
    author='James Saryerwinnie',
    author_email='js@jamesls.com',
    maintainer='Bruce Merry',
    maintainer_email='bmerry@ska.ac.za',
    packages=['fakeredis'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
    python_requires='>=3.5',
    install_requires=[
        # Minor version updates to redis tend to break fakeredis. If you
        # need to use fakeredis with a newer redis, please submit a PR that
        # relaxes this restriction and adds it to the Travis tests.
        'redis<3.6.0', 'six>=1.12', 'sortedcontainers'
    ],
    extras_require={
        'lua': ['lupa'],
        'aioredis': ['aioredis']
    }
)
