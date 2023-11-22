from setuptools import setup, find_packages

setup(
    name="lp-parser",
    version="0.0.1",
    py_modules=find_packages(),
    install_requires=[
        'Click',
        'rocrate',
    ],
    entry_points={
        'console_scripts': [
            "lp-parser = parser.cli:cli"
        ]
    }
)
