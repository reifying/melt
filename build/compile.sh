#!/bin/bash

set -e

clojure -e "(compile 'jdbc.melt.serdes)"
