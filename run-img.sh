mkdir -p ./data_test
sudo chown -R 65532:65532 ./data_test

docker run --rm -p 8866:8866 \
  -v "$(pwd)/data_test:/data/novasql" \
  --name novasql-server \
  tuannm99/novasql:v0.0.1


docker run --rm -it --network host \
  --entrypoint /app/novasql-client \
 tuannm99/novasql:v0.0.1 -addr 127.0.0.1:8866
