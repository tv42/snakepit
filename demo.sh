#!/bin/sh
set -e

#
# Simple demonstration on setting up a hive from shell.
#

snakepit-create-hive sqlite:///hive.db
snakepit-create-dimension sqlite:///hive.db frob
snakepit-create-node sqlite:///hive.db frob node1 sqlite:///node1.db
snakepit-create-node sqlite:///hive.db frob node2 sqlite:///node2.db
