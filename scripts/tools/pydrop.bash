#!/bin/bash

python -c "from pymongo import MongoClient;MongoClient('mongodb://manager:toric@129.10.135.170:27017/ToricCY?authMechanism=SCRAM-SHA-1')['ToricCY']['$1'].drop();"