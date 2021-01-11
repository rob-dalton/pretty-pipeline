import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pretty-pipeline",
    version="0.0.3",
    author="Rob Dalton",
    description="Package for creating ETL pipelines.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rob-dalton/pretty-pipeline",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
