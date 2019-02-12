#!/bin/bash

set -e

clojure -Sdeps '{:deps {olical/depot {:mvn/version "1.7.0"}}}' -m depot.outdated.main --update
