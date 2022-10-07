build:
	echo "$(shell pwd)"
	docker build -t pollenclub:v0.4 .

run:
	docker run --rm -it -v "$(shell pwd)"/messages:/opt/app/messages  pollenclub:v0.4

run_bash:
	docker run --rm -it -v "$(shell pwd)"/messages:/opt/app/messages  pollenclub:v0.4 bash


