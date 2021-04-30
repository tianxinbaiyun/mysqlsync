DOCKER_NAME          := dbsync
PORT                 := 10001
VERSION              := v1.0.1

LOC_IP               := 127.0.0.1
DEV_IP               := 172.16.3.134


local:
	@$(info build and upload dev)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags "-s -w" -o server;
	@rm -f $(DOCKER_NAME).img;
	docker build -t $(DOCKER_NAME):$(VERSION) .;
	docker save -o $(DOCKER_NAME).img $(DOCKER_NAME);
	-docker stop $(DOCKER_NAME);
	-docker rm $(DOCKER_NAME);
	docker rmi $(DOCKER_NAME):$(VERSION);
	docker load --input $(DOCKER_NAME).img;
	docker run --restart=always -e DEBUG='$(DEV_DEBUG)' \
		-p '$(PORT)':8080 --name $(DOCKER_NAME) \
		-itd $(DOCKER_NAME):$(VERSION) ;
	rm -f $(DOCKER_NAME).img;docker logs -f $(DOCKER_NAME);

dev:
	@$(info build and upload dev)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags "-s -w" -o server
	@rm -f $(DOCKER_NAME).img
	docker build -t $(DOCKER_NAME):$(VERSION) .
	docker save -o $(DOCKER_NAME).img $(DOCKER_NAME)
	scp $(DOCKER_NAME).img root@$(DEV_IP):/data/images/
	docker rmi $(DOCKER_NAME):$(VERSION)
	rm -f server
	ssh root@$(DEV_IP) "cd /data/images;\
	docker stop $(DOCKER_NAME) && docker rm $(DOCKER_NAME);\
	docker rmi $(DOCKER_NAME):$(VERSION);\
	docker load --input $(DOCKER_NAME).img;\
	docker run --restart=always \
		-p '$(PORT)':8080 --name $(DOCKER_NAME) \
		-itd $(DOCKER_NAME):$(VERSION) ; \
	rm -f $(DOCKER_NAME).img;docker logs -f $(DOCKER_NAME)";


