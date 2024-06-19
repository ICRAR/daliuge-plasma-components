# daliuge_plasma_components

[![codecov](https://codecov.io/gh/ICRAR/daliuge-plasma-components/branch/main/graph/badge.svg?token=daliuge-plasma-components_token_here)](https://codecov.io/gh/ICRAR/daliuge-plasma-components)
[![CI](https://github.com/ICRAR/daliuge-plasma-components/actions/workflows/main.yml/badge.svg)](https://github.com/ICRAR/daliuge-plasma-components/actions/workflows/main.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


`daliuge_plasma_components` created by ICRAR

## Installation

There are multiple options for the installation, depending on how you are intending to run the DALiuGE engine, directly in a virtual environment (host) or inside a docker container. You can also install it either from PyPI (latest released version).

## Install it from PyPI

### Engine in virtual environment
```bash
pip install daliuge_plasma_components
```

## Usage
For example the MyComponent component will be available to the engine when you specify 
```
daliuge_plasma_components.apps.MyAppDROP
```
in the AppClass field of a Python Branch component. The EAGLE palette associated with these components are also generated and can be loaded directly into EAGLE. In that case all the fields are correctly populated for the respective components.

