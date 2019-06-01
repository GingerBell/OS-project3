#!/bin/bash

if [ ! -f "config.json" ]; then
	cd ../
fi
if [ ! -f "config.json" ]; then
	exit -1
fi

./start.sh >>./test/terminal_log.txt 2>&1 &
PID=$!
echo -n $PID
