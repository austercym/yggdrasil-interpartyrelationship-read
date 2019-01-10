#!/usr/bin/env bash

usage() {
	echo "Usage: $0 -v <version> [-e <env>] [-d <dest-server>] [-u <server-user>] [-f]" 1>&2;
	echo "  - env: SID / UAT; currently used ONLY for some config, so make sure you provide correct arguments to -d and -u options";
	exit 1;
}

# Default env
env=SID

# Default dest-server for SID
server=sid-hdf-g4-1

# Default server-user for SID
serverUser=centos

while getopts "v:e:d:u:f" o; do
    case "${o}" in
        v)
            version=${OPTARG}
            ;;
        e)
            env=${OPTARG}
            ;;
        d)
            server=${OPTARG}
            ;;
        u)
            serverUser=${OPTARG}
            ;;
        f)
            force=true
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${version}" ] || [ -z "${server}" ]; then
    usage
fi

if [ -z "${force}" ]; then
	read -r -p "Will deploy to $server, are you sure? [y/N] " response
	case "$response" in
	    [yY][eE][sS]|[yY])
	        echo "deploying..."
	        ;;
	    *)
	        exit 1
	        ;;
	esac
fi

# dir where this script is located
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

JAR=yggdrasil-interpartyrelationship-read-$version.jar

TOPOLOGY=yggdrasil-interpartyrelationship-read
MAIN=com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopology
PROFILE=deploy
SERVERUSER=centos
SERVERDIR=/tmp/
DEPLOYUSER=svc_core

if [ $env = "SID" ]; then
	NIMBUS=sid-hdf-g1-1.node.sid.consul
	ZOOKEEPER=sid-hdf-g1-0.node.sid.consul:2181,sid-hdf-g1-1.node.sid.consul:2181,sid-hdf-g1-2.node.sid.consul:2181
elif [ $env = "UAT" ]; then
	NIMBUS=hdf-group2-1.node.consul
	ZOOKEEPER=hdf-group2-0.node.consul:2181,hdf-group2-1.node.consul:2181,hdf-group4-2.node.consul:2181
elif [ $env = "PROD" ]; then
	NIMBUS=hdf-g1-1.node.consul
	ZOOKEEPER=hdf-g1-0.node.consul:2181,hdf-g1-1.node.consul:2181,hdf-g1-2.node.consul:2181
	env=IO
else
	usage
fi


mvn clean package -P $PROFILE

if [ ! -f target/$JAR ]; then
  echo "target/$JAR not found"
  exit 1
fi
scp $dir/target/$JAR $serverUser@$server:$SERVERDIR$JAR
ssh $serverUser@$server "chmod o+r /tmp/$JAR; sudo -H -u $DEPLOYUSER bash -c 'cd /home/$DEPLOYUSER; pwd; kinit -kt /etc/security/keytabs/$DEPLOYUSER.keytab $DEPLOYUSER@ORWELLG.$env; storm kill $TOPOLOGY -c nimbus.host=$NIMBUS; sleep 20s; storm jar /tmp/$JAR $MAIN $ZOOKEEPER -c nimbus.host=$NIMBUS;exit'"
