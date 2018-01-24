# -*- coding: utf-8 -*-

"""
    Example Asynchronous multiprocessing grabber
    @author Lukashov_ai
"""

from setuptools import setup, find_packages
import os

DIR_LOG = '/var/log/webloader'
DIR_TMP = '/var/tmp/webloader'
DIR_ETC = '/etc/webloader'
DIR_ETC_ENT = '/etc/webloader/entities'

if not os.path.isdir(DIR_LOG):
    os.mkdir(DIR_LOG)
    os.chmod(DIR_LOG, 0o777)

if not os.path.isdir(DIR_TMP):
    os.mkdir(DIR_TMP)
    os.chmod(DIR_TMP, 0o777)

if not os.path.isdir(DIR_ETC):
    os.mkdir(DIR_ETC)
    os.chmod(DIR_ETC, 0o777)

if not os.path.isdir(DIR_ETC_ENT):
    os.mkdir(DIR_ETC_ENT)
    os.chmod(DIR_ETC_ENT, 0o777)

setup(
    name='webloader',
    version="0.4",
    include_package_data=True,
    packages=find_packages(),
    install_requires=[
        'pika>=0.11.2',
        'asyncpg>=0.14.0',
        'aiohttp>=2.3.9',
        'aiofiles>=0.3.2',
    ],
)
