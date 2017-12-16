#!/bin/bash
cd /app/lib

wget https://www.dropbox.com/s/ewjv4dy3mprzauv/libclntsh.so

cp libclntsh.so libclntsh.so.12.1

wget https://www.dropbox.com/s/565uig6mfoj30g4/libociei.so

export LD_LIBRARY_PATH=/app/lib/