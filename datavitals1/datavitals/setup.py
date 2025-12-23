from setuptools import setup, find_packages

setup(
    name="datavitals",
    version="0.1.3",
    author="Kamaleshkumar.K",
    author_email="kamaleshkumaroffi@gmail.com",
    description="A reusable Python library for data cleaning, ETL, and SQL query building",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kamaleshkumaroffi/datavitals",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "pytest>=7.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
)

