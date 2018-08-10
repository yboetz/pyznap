.PHONY: all install-dev test release release-test clean

all: test

install-dev:
	pip install -e .[dev]

test: clean install-dev
	pytest

release:
	pip install twine
	python setup.py sdist bdist_wheel
	twine upload dist/*
	rm -f -r build/ dist/ pyznap.egg-info/

release-test:
	pip install twine
	python setup.py sdist bdist_wheel
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*
	rm -f -r build/ dist/ pyznap.egg-info/

clean:
	rm -f -r build/
	rm -f -r dist/
	rm -f -r pyznap/__pycache__/
	rm -f -r tests/__pycache__
	rm -f -r pyznap.egg-info/
	rm -f -r .pytest_cache/
