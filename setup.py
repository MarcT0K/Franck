from setuptools import setup, find_packages

setup(
    name="franck",
    version="1.0.3",
    url="https://github.com/MarcT0K/Franck",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Crawlers for various Fediverse projects",
    packages=find_packages(),
    license="GPLv3",
    python_requires=">=3.8",
    install_requires=[
        "aiohttp[speedups]",
        "aiohttp_retry",
        "tqdm",
        "numpy<=1.26",  # Was creating an import error with pandas when importing the package
        "scipy",
        "fastparquet",
        "requests",
        "colorlog",
    ],
    entry_points={
        "console_scripts": [
            "franck = franck.cli:main",
        ],
    },
)
