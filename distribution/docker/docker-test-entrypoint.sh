#!/bin/bash
cd /usr/share/havenask/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/havenask/logs/console.log
