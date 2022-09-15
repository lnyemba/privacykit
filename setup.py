"""
This is a build file for the 
"""
from setuptools import setup, find_packages
 
setup(
    name = "risk",
    version = "0.8.1",
    author = "Healthcare/IO - The Phi Technology LLC & Health Information Privacy Lab",
    author_email = "info@the-phi.com",
    license = "MIT",
    packages=['risk'],
    install_requires = ['numpy','pandas']
    )
