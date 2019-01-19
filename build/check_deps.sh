#!/bin/bash

set -e

clojure -Aoutdated -a outdated | grep 'All up to date'
