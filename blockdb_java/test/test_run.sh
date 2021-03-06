#!/bin/bash

# This script starts the database server and runs a series of operation against server implementation.
# If the server is implemented correctly, the output (both return values and JSON block) will match the expected outcome.
# Note that this script does not compare the output value, nor does it compare the JSON file with the test JSON.

# Please start this script in a clean environment; i.e. the server is not running, and the data dir is empty.

if [ ! -f "config.json" ]; then
	echo "config.json not in working directory. Trying to go to parent directory..."
	cd ../
fi
if [ ! -f "config.json" ]; then
  	echo "!! Error: config.json not found. Please run this script from the project root directory (e.g. ./test/test_run.sh)."
	exit -1
fi

echo "Testrun starting..."

./start.sh &>./test/terminal_log.txt &
PID=$!
sleep 1

echo "Step 1: Initialize account"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=PUT -user=TESTAAA$I -value=10
done
echo "Check value: expecting value=10"
go run ./test/test_client.go -T=GET -user=TESTAAA5

echo "Check LogLength: expecting value=10"
go run ./test/test_client.go -T=LogLength

echo "Step 2: Try deposit"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=DEPOSIT -user=TESTAAA$I -value=5
done
echo "Check value: expecting value=15"
go run ./test/test_client.go -T=GET -user=TESTAAA5

echo "Step 3: Try transfer"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=TRANSFER -from=TESTAAA$I -to=TESTAATX  -value=10
done
echo "Check value: expecting value=100"
go run ./test/test_client.go -T=GET -user=TESTAATX

echo "Step 4: Try transfer again"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=TRANSFER -from=TESTAATX -to=TESTAAA$I  -value=5
done
echo "Check value: expecting value=50"
go run ./test/test_client.go -T=GET -user=TESTAATX

echo "Step 5: Try withdraw"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=WITHDRAW -user=TESTAAA$I  -value=5
done
echo "Check value: expecting value=5"
go run ./test/test_client.go -T=GET -user=TESTAAA2

echo "Step 5: Try overdraft"
for I in `seq 0 9`; do
	go run ./test/test_client.go -T=WITHDRAW -user=TESTAAA$I  -value=10
done
echo "Check value: expecting value=5"
go run ./test/test_client.go -T=GET -user=TESTAAA2

echo "Check LogLength: expecting value<=50"
go run ./test/test_client.go -T=LogLength

#echo "Try Killing the server and restart..."
#echo "Sleep for a while, waiting for hashmap reconstruction..."
#sleep 10;

echo "Check value again: expecting value=5"
go run ./test/test_client.go -T=GET -user=TESTAAA2


echo "Test completed. Please verify JSON block output with test_1.json ."

kill $PID
