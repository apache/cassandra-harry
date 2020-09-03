DOCKER_REPO   = org.apache.cassandra/harry/harry-runner

img:
	rm -fr shared/*
	mvn clean && mvn package -DskipTests && docker build -t ${DOCKER_REPO}:latest-local ./ -f docker/Dockerfile.local

run: img
	docker run -v `pwd`/shared:/shared -it ${DOCKER_REPO}:latest-local

run-last:
	docker run -v `pwd`/shared:/shared -it ${DOCKER_REPO}:latest-local
