#!/bin/bash

set -e

## compatible with macos
if ! [ -x "$(command -v realpath)" ]; then
    realpath() {
        [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
    }
fi



if [ "$1" == "-h" -o "$1" == "--help" ]; then
    echo "Usage: $0 DOCKER_NAME [IMAGE]"
    echo "       then excute './DOCKER_NAME/sshme' to enter to container."
    exit 0
fi
if [ $# -lt 1 ]; then
    echo "Usage: $0 DOCKER_NAME [IMAGE]"
    echo "       then excute './DOCKER_NAME/sshme' to enter to container."
    exit 2
fi

IMAGE="havenask/ha3_runtime:0.2.2"
if [ $# -eq 2 ]; then
    IMAGE=$2
fi

USER="havenask"
USERID=`id -u`
CONTAINER_NAME=$1

SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
REPO_DIR=$(dirname "$SCRIPT_DIR")
CONTAINER_DIR=$SCRIPT_DIR/$CONTAINER_NAME

echo "Start to run scrip"
echo "Info: Repo locatation: $REPO_DIR"
echo "Info: Container entry: $CONTAINER_DIR"

mkdir -p $CONTAINER_DIR


docker run -p 9200:9200 -p 5005:5005 -p 39200:39200 -p 49200:49200 -p 5601:5601 --ulimit nofile=655350:655350 --privileged --cap-add SYS_ADMIN --device /dev/fuse --ulimit memlock=-1 --cpu-shares=15360 --cpu-quota=9600000 --cpu-period=100000 --memory=500000m -d  --name $CONTAINER_NAME -v $CONTAINER_DIR/initContainer.sh:/tmp/initContainer.sh $IMAGE /sbin/init 1> /dev/null


if [ $? -ne 0 ]; then
    echo "ERROR, run container failed, please check."
    exit 3
fi


sshme="docker exec --user $USER -it $CONTAINER_NAME sh -c 'cd ~ && /bin/bash' "
[ -e $CONTAINER_DIR/sshme ] && rm -rf $CONTAINER_DIR/sshme
echo $sshme > $CONTAINER_DIR/sshme
chmod +x $CONTAINER_DIR/sshme
dockerstop="docker stop $CONTAINER_NAME;docker rm -fv $CONTAINER_NAME"
stopSh=stopContainer.sh
[ -e $CONTAINER_DIR/$stopSh ] && rm -rf $CONTAINER_DIR/$stopSh
echo $dockerstop > $CONTAINER_DIR/$stopSh
chmod +x $CONTAINER_DIR/$stopSh

echo "INFO start container success"