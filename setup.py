from setuptools import setup, find_packages

setup(
    name="franck",
    version="0.0.1",
    url="https://github.com/MarcT0K/Franck",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Crawlers for various Fediverse projects",
    packages=find_packages(),
    license="GPLv3",
    python_requires=">=3.8",
    install_requires=[
        "aiohttp[speedups]",
        "tqdm",
        "scipy",
        "pandas",
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
