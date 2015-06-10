#! /bin/bash

HANDSON=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. $HANDSON/install_mvn.sh
. $HANDSON/install_flink.sh
. $HANDSON/tag_cluster.sh

