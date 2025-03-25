ENVIRONMENT_NAME = harmonized-landsat-sentinel
DOCKER_IMAGE_NAME = $(ENVIRONMENT_NAME)

clean:
	rm -rf *.o *.out *.log
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +

test:
	pytest

build:
	python -m build

twine-upload:
	twine upload dist/*

dist:
	make clean
	make build
	make twine-upload

remove-environment:
	mamba env remove -y -n rasters

install:
	pip install -e .[dev]

uninstall:
	pip uninstall $(DOCKER_IMAGE_NAME)

reinstall:
	make uninstall
	make install

environment:
	mamba create -y -n $(ENVIRONMENT_NAME) -c conda-forge python=3.10

colima-start:
	colima start -m 16 -a x86_64 -d 100 

docker-build:
	docker build -t $(DOCKER_IMAGE_NAME):latest .

docker-build-environment:
	docker build --target environment -t $(DOCKER_IMAGE_NAME):latest .

docker-build-installation:
	docker build --target installation -t $(DOCKER_IMAGE_NAME):latest .

docker-interactive:
	docker run -it $(DOCKER_IMAGE_NAME) fish 

docker-remove:
	docker rmi -f $(DOCKER_IMAGE_NAME)
