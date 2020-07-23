# -*- coding: utf-8 -*-

import codecs
import os.path
import re
import setuptools


def read(*parts):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, *parts), "r") as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


requirements = ["black>=19.10b0", "pendulum>=1.4.4"]


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="spectron",
    version=find_version("spectron", "__init__.py"),
    author="Jeremy Jacobs",
    author_email="pub@j4c0bs.net",
    description="AWS Redshift Spectrum utilities.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/j4c0bs/spectron",
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=requirements,
    tests_require=["pytest", "pytest-datadir"],
    extras_require={"json": "ujson==1.35"},
    include_package_data=False,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={"console_scripts": ["spectron=spectron.cli:create_spectrum_schema"]},
)
