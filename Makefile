.PHONY: check-env
check-env:
ifndef AWS_ACCESS_KEY_ID
	$(error AWS_ACCESS_KEY_ID is undefined)
endif
ifndef AWS_SECRET_ACCESS_KEY
	$(error AWS_SECRET_ACCESS_KEY is undefined)
endif

.PHONY: check-connection
check-connection:
	@[ "${user}" ] || ( echo ">> user is not set"; exit 1 )
	@[ "${password}" ] || ( echo ">> password is not set"; exit 1 )
	@[ "${host}" ] || ( echo ">> host is not set"; exit 1 )
	@[ "${port}" ] || ( echo ">> port is not set"; exit 1 )
	@[ "${db}" ] || ( echo ">> db is not set"; exit 1 )
	@[ "${schema}" ] || ( echo ">> schema is not set"; exit 1 )

extract: check-env
	@python -m ecom_sales extract

load: check-env check-connection
	@python -m ecom_sales load \
		--user=$(user) \
		--password=$(password) \
		--host=$(host) \
		--port=$(port) \
		--db=$(db) \
		--schema=$(schema)