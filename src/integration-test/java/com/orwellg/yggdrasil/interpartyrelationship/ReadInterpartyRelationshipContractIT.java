package com.orwellg.yggdrasil.interpartyrelationship;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.ScyllaDbHelper;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.utils.enums.InterpartyRelationshipEvents;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.umbrella.commons.utils.uniqueid.UniqueIDGenerator;
import com.orwellg.umbrella.commons.utils.zookeeper.ZooKeeperHelper;
import com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopology;
import com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopologyRequestSender;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;


public class ReadInterpartyRelationshipContractIT {

	// Kafka+zookeeper services
	@Rule
	public DockerComposeRule docker = DockerComposeRule.builder()
	.file("src/integration-test/resources/docker-compose.yml")
	.waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
	.build();


	protected static Session ses;

	protected final static String partycontractID = "3ID";
	protected final static String partyID = "1PARTYID1";

	protected String bootstrapHosts = "localhost:9092";
	private final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipContractIT.class);
	private CuratorFramework client;

	protected String scyllaNodes = "127.0.0.1:9042";
	protected String scyllaKeyspace = ScyllaParams.DEFAULT_SCYLA_KEYSPACE_CUSTOMER_PRODUCT_DB;
	protected static ScyllaManager man;

	@Before
	public void setUp() throws Exception {
		LOG.info("setup");

		// Set zookeeper properties
		LOG.info("Curator connecting to zookeeper localhost:2181...");
		client = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3));
		client.start();
		LOG.info("...Curator connected.");	

		ZooKeeperHelper zk = new ZooKeeperHelper(client);

		// Set scylla config

		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list", scyllaNodes);
		assertEquals(scyllaNodes, new String(client.getData().forPath("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.node.list")));
		zk.setZkProp("/com/orwellg/yggdrasil/scylla/yggdrassil.scylla.keyspace", scyllaKeyspace);

		String uniqueIdClusterSuffix = "IPAGOO";
		//zk.setZkProp(UniqueIDGenerator.CLUSTER_SUFFIX_ZNODE, uniqueIdClusterSuffix);

		// #bootstrap kafka servers:
		zk.setZkProp("/com/orwellg/yggdrasil/topologies-defaults/kafka-bootstrap-host", bootstrapHosts);
		assertEquals(bootstrapHosts, new String(client.getData().forPath("/com/orwellg/yggdrasil/topologies-defaults/kafka-bootstrap-host")));

	}


	@Test
	public void testLoadTopologyPartyIdInStormLocalCluster() throws Exception {
		LOG.info("testLoadTopologyInStormLocalCluster");

		String result = "KO";

		// Given topology loaded in local cluster
		LocalCluster cluster = new LocalCluster();
		ReadInterpartyRelationshipTopology.loadTopologyInStorm(cluster);
		
		Thread.sleep(10000);
		
		ScyllaManager man = ScyllaManager.getInstance(scyllaNodes);
		ses = man.getCluster().connect();
				
		ScyllaDbHelper scyllaDbHelper = new ScyllaDbHelper(ses);
		scyllaDbHelper.createDbSchema("/DataModel/ScyllaDB/scylla_obs_datamodel.cql", ";", ScyllaParams.DEFAULT_SCYLA_KEYSPACE_CUSTOMER_PRODUCT_DB , scyllaKeyspace);
		
		///////
				
		String insertQueryInsert = "INSERT INTO " + scyllaKeyspace + ".interpartyrelationshippartyid1 ( Relationship_ID, Party_ID1, Party_ID2, RelationshipType, UpdateVector) VALUES ( '1ID','1PARTYID1','1PARTYID2','EMPLOYEE', 49);";
		PreparedStatement stInsert= ses.prepare(insertQueryInsert);
		BoundStatement insert = stInsert.bind();
		ses.execute(insert);

		String insertQueryInsert2 = "INSERT INTO " + scyllaKeyspace + ".interpartyrelationshippartyid1 ( Relationship_ID, Party_ID1, Party_ID2, RelationshipType, UpdateVector) VALUES ( '2ID','1PARTYID1','2PARTYID2','EMPLOYEE', 59);";
		PreparedStatement stInsert2= ses.prepare(insertQueryInsert2);
		BoundStatement insert2 = stInsert2.bind();
		ses.execute(insert2);
		
		String insertQueryInsert3 = "INSERT INTO " + scyllaKeyspace + ".interpartyrelationship ( Relationship_ID, Party_ID1, Party_ID2, RelationshipType, UpdateVector) VALUES ( '3ID','3PARTYID1','3PARTYID2','EMPLOYEE', 69);";
		PreparedStatement stInsert3 = ses.prepare(insertQueryInsert3);
		BoundStatement insert3 = stInsert3.bind();
		ses.execute(insert3);
		
		String insertQueryInsert4 = "INSERT INTO " + scyllaKeyspace + ".interpartyrelationship ( Relationship_ID, Party_ID1, Party_ID2, RelationshipType, UpdateVector) VALUES ( '1ID','1PARTYID1','1PARTYID2','EMPLOYEE', 169);";
		PreparedStatement stInsert4 = ses.prepare(insertQueryInsert4);
		BoundStatement insert4 = stInsert4.bind();
		ses.execute(insert4);
		
		String insertQueryInsert5 = "INSERT INTO " + scyllaKeyspace + ".interpartyrelationship ( Relationship_ID, Party_ID1, Party_ID2, RelationshipType, UpdateVector) VALUES ( '2ID','2PARTYID1','2PARTYID2','EMPLOYEE', 269);";
		PreparedStatement stInsert5 = ses.prepare(insertQueryInsert5);
		BoundStatement insert5 = stInsert5.bind();
		ses.execute(insert5);
		

		ReadInterpartyRelationshipTopologyRequestSender test = new ReadInterpartyRelationshipTopologyRequestSender();
		LOG.info("Requesting balance (Read Party Contract)");
		InterPartyRelationship balance = (InterPartyRelationship) test.requestAndWaitResponseInterpartyRelationship(InterpartyRelationshipEvents.GET_INTERPARTYRELATIONSHIP ,partycontractID, TopologyConfigFactory.getTopologyConfig());

		Assert.assertNotNull(balance);
		
		// Then Print result
		LOG.info("InterpartyRelationship = {}", balance);
		
		List<InterPartyRelationship> list = (List<InterPartyRelationship>) test.requestAndWaitResponseInterpartyRelationship(InterpartyRelationshipEvents.LIST_INTERPARTYRELATIONSHIP ,partyID, TopologyConfigFactory.getTopologyConfig());
		
		Assert.assertEquals(2, list.size());
		
		LOG.info("List InterpartyRelationship = {}", list);
	}	

}
