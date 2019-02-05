#!/bin/bash

clj -Amidje -e "(use 'midje.repl) (midje.repl/autotest)"
