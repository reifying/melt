#!/bin/bash

set -e

clojure -Aoutdated -a outdated > deps.log
cat deps.log
cat deps.log | grep 'All up to date'
