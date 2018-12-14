#!/usr/bin/env python2

          
import setuptools 

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ARtWORK",
    version="1.0.14",
    author="Arnaud Felten, Kevin Durimel, Ludovic Mallet",
    author_email="arnaud.felten@anses.fr, ludovic.mallet@anses.fr",
    description="Assembly of Reads and Typing Workflow",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/afelten-Anses/ARtWORK",
    packages=setuptools.find_packages(),
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Programming Language :: Python :: 3.4",
        "Operating System :: POSIX :: Linux",
    ],
        scripts=['src/ARtWORK',
             "src/ARtWORK_Assembler",
             "src/ARtWORK_lite",
             "src/MetARtWORK",
             ],
    include_package_data=True,
    install_requires=['biopython>=1.68', 
                      'lxml',
                      'requests',
                      'pymongo>=3.7.2',
                      'bson',
                      "featmongo",
                      "pyunpack",
                      "patool",
                      "networkx",
                      "numpy",
                      ],
    zip_safe=False,







)
