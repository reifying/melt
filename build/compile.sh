#!/bin/bash

set -e

clojure -e "(compile 'melt.serial)"
