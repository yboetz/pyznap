
.PHONY: all install-dev test release clean

all: test

install-dev:
	pip install -e .[dev]

test: clean install-dev
	pytest -v

release:
	pip install twine
	python setup.py sdist bdist_wheel
	#twine upload dist/*
	rm -f -r build/ dist/ pyznap.egg-info/

clean:
	rm -f -r build/
	rm -f -r dist/
	rm -f -r pyznap/__pycache__/
	rm -f -r tests/__pycache__
	rm -f -r pyznap.egg-info/
	rm -f -r .pytest_cache/
