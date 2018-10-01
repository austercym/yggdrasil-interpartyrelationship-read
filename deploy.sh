#!/usr/bin/env bash
FILE=yggdrasil-interpartyrelationship-read-0.0.1.jar
TOPOLOGY=yggdrasil-interpartyrelationship-read
MAIN=com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopology
PROFILE=deploy
SERVER=sid-hdf-g4-1
SERVERUSER=centos
SERVERDIR=/tmp/
DEPLOYUSER=svc_core
NIMBUS=sid-hdf-g1-1.node.sid.consul
ZOOKEEPER=sid-hdf-g1-0.node.sid.consul:2181,sid-hdf-g1-1.node.sid.consul:2181,sid-hdf-g1-2.node.sid.consul:2181
mvn clean package -P $PROFILE
scp target/$FILE $SERVERUSER@$SERVER:$SERVERDIR$FILE
ssh $SERVERUSER@$SERVER "sudo -H -u $DEPLOYUSER bash -c 'cd /home/$DEPLOYUSER; pwd; kinit -kt /etc/security/keytabs/$DEPLOYUSER.keytab $DEPLOYUSER@ORWELLG.SID; storm kill $TOPOLOGY -c nimbus.host=$NIMBUS; sleep 20s; storm jar /tmp/$FILE $MAIN $ZOOKEEPER -c nimbus.host=$NIMBUS;exit'"
