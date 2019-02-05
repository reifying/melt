#!/bin/bash

set -e

clojure -Atest
clojure -Aintegration
