from setuptools import setup, find_packages


with open("README.md", "r") as readme:
    long_description = readme.read()

setup(
    name="elasticsearch-adsl",
    version="6.3.1.dev1",
    author="Slam",
    author_email="3lnc.slam@gmail.com",
    description="Async interface for elasticsearch dsl",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/3lnc/elasticsearch-adsl",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'elasticsearch-dsl',
        'elasticsearch-async'
    ],
    tests_require=[
        'pytest',
        'pytest-aiohttp'
    ]
)
