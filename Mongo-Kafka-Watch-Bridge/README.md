# Mongo-Kafka-Watch-Bridge
Queries data from a MongoDB given a database, collection and a filter in a given interval and sends it to a Kafka server.  
Originally used for the smartwatch application.

## Setup
1. Ensure you have the go toolchain installed
2. Rename `.sample.env` to `.env`
3. Populate `.env` with your credentials
4. Run `go build`
5. Run the executable to perform the task

*If you want to build the program for another platform, please consider the [docs](https://golang.org/doc/install/source#environment).*