# yggdrasil-interpartyrelationship-read
contract Topologies to read  party contract catalog, and also to manage party contracts

## Build

mvn clean install -DskipTests -P environment (see pom.xml for defined environments), eg:

mvn clean install -DskipTests -P development
mvn clean install -DskipTests -P integration

```

## Run (topology)

Run local cluster or deploy with "storm jar ...".


## Deploy in OVH test storm cluster:

```sh
storm jar <name>-jar-with-dependencies.jar com.orwellg.yggdrasil.dsl.contract.topology.contractDSLTopology -c nimbus.host=hdf-node2
```
