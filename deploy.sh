#!/usr/bin/env bash

usage() {
	echo "Usage: $0 -v <version> [-d <dest-server>] [-f]" 1>&2; exit 1;
}

# Default dest-server is SID
server=sid-hdf-g4-1

while getopts "v:d:f" o; do
    case "${o}" in
        v)
            version=${OPTARG}
            ;;
        d)
            server=${OPTARG}
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

JAR=yggdrasil-interpartyrelationship-read-$version.jar

TOPOLOGY=yggdrasil-interpartyrelationship-read
MAIN=com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopology
PROFILE=deploy
SERVERUSER=centos
SERVERDIR=/tmp/
DEPLOYUSER=svc_core
NIMBUS=sid-hdf-g1-1.node.sid.consul
ZOOKEEPER=sid-hdf-g1-0.node.sid.consul:2181,sid-hdf-g1-1.node.sid.consul:2181,sid-hdf-g1-2.node.sid.consul:2181

mvn clean package -P $PROFILE

if [ ! -f target/$JAR ]; then
  echo "target/$JAR not found"
  exit 1
fi

scp target/$JAR $SERVERUSER@$server:$SERVERDIR$JAR
ssh $SERVERUSER@$server "sudo -H -u $DEPLOYUSER bash -c 'cd /home/$DEPLOYUSER; pwd; kinit -kt /etc/security/keytabs/$DEPLOYUSER.keytab $DEPLOYUSER@ORWELLG.SID; storm kill $TOPOLOGY -c nimbus.host=$NIMBUS; sleep 20s; storm jar /tmp/$JAR $MAIN $ZOOKEEPER -c nimbus.host=$NIMBUS;exit'"
