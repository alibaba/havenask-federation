#!/bin/bash
cd /usr/share/havenask/bin/
./havenask-users useradd rest_user -p test-password -r superuser || true
echo "testnode" > /tmp/password
/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/havenask/logs/console.log
