#!/bin/bash
cd /app/lib

wget https://www.dropbox.com/s/ewjv4dy3mprzauv/libclntsh.so


wget https://www.dropbox.com/s/565uig6mfoj30g4/libociei.so

export LD_LIBRARY_PATH=/app/lib:$LD_LIBRARY_PATH