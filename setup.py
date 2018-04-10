#!/usr/bin/env python3

from setuptools import setup

setup(name='signals',
      version='0.8',
      description='signals library',
      author='David Schere',
      author_email='dave.avantgarde@gmail.com',
      py_modules=['signals'],
      install_requires=[
        'paho-mqtt>=1.3.1'
      ]
     )
