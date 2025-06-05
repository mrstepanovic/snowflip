from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="snowflip",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python package for accessing Flipside Crypto datasets via Snowflake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/snowflip",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Office/Business :: Financial",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "snowflip=snowflip.cli:main",
        ],
    },
    keywords="flipside crypto snowflake blockchain data analysis",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/snowflip/issues",
        "Source": "https://github.com/yourusername/snowflip",
        "Documentation": "https://snowflip.readthedocs.io/",
    },
) 