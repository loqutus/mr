#!/usr/bin/env bash
./mr.py &
sleep 1
pytest -v
kill -9 $(ps aux | grep mr | grep -v grep | grep python3 | awk '{print $2}')
rm data/input/*
rm data/split_input/*
rm data/output/*
rm data/map/*
rm data/reduce/*
