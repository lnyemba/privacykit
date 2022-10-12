"""
This is a build file for the 
"""
from setuptools import setup, find_packages
 
setup(
    name = "privacykit",
    version = "0.9.0",
    author = "Healthcare/IO - The Phi Technology LLC & Health Information Privacy Lab",
    author_email = "info@the-phi.com",
    license = "MIT",
    packages=['privacykit'],
    install_requires = ['numpy','pandas']
    )
