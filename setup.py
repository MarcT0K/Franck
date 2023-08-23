from setuptools import setup, find_packages

setup(
    name="franck",
    version="0.0.1",
    url="https://github.com/MarcT0K/Franck",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Description of my package",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=["aiohttp[speedups]", "tqdm", "scipy", "pandas", "fastparquet"],
    entry_points={
        "console_scripts": [
            "franck = franck.cli:main",
        ],
    },
)
