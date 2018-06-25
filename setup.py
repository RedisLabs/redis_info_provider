from setuptools import setup, find_packages
import os
import sys


ROOT_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, 'src')


install_requires = [
    'gevent',
    'redis',
    'psutil',
]

if sys.version_info[:2] < (3, 5):
    install_requires.append('typing')
if sys.version_info[:2] < (3, 4):
    install_requires.append('enum')

tests_require = [
    'six',
    'mock',
]

extras = {
    'test': tests_require,
}

setup(
    name='redis-info-provider',
    version='0.9.8',
    python_requires='>=2.7',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    url='',
    license='GNU General Public License v3 (GPLv3)',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'
    ],
    author='Slava Koyfman',
    author_email='slava.koyfman@redislabs.com',
    description='Framework for serving info for an arbitrary number of redis-server instances',
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras,
    test_suite='tests'
)
