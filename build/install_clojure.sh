#!/bin/bash

set -e
SCRIPT=`curl https://clojure.org/guides/getting_started | grep 'https://download.clojure.org/install/linux-install-.*.sh' | sed 's|.*install/\(.*.sh\).*|\1|'`
curl -O https://download.clojure.org/install/$SCRIPT
chmod +x $SCRIPT
sudo ./$SCRIPT
