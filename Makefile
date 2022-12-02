.PHONY = test
test:
	go test -race -v

.PHONY: rabbitmq-server
rabbitmq-server:
	docker run --detach --rm --name rabbitmq \
		--publish 5672:5672 --publish 15672:15672 \
        --pull always rabbitmq:3-management

.PHONY: stop-rabbitmq-server
stop-rabbitmq-server:
	docker stop $$(docker inspect --format='{{.Id}}' rabbitmq)
