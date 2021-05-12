#! /bin/bash
redis-server ./redis.conf &
java -jar ./ThriftServer.jar ./server-configure.json