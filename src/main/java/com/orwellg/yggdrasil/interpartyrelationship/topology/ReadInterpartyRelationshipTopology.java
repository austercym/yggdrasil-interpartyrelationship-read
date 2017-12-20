package com.orwellg.yggdrasil.interpartyrelationship.topology;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.GenericGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.yggdrasil.interpartyrelationship.topology.bolts.ReadInterpartyRelationshipBolt;
import com.orwellg.yggdrasil.interpartyrelationship.topology.bolts.ReadInterpartyRelationshipFinalProcessBolt;
import com.orwellg.yggdrasil.interpartyrelationship.topology.bolts.ReadInterpartyRelationshipKafkaEventProcessBolt;

/**
 * Storm topology to process GetTransactionlog action read from kafka topic. Topology
 * summary:
 * <li>KafkaSpoutWrapper
 * 
 * <li>GetLastTransactionlogKafkaEventProcessBolt
 * <li>GetLastTransactionlogBolt
 * <li>GetLastTransactionlogFinalProcessBolt
 * 
 * <li>KafkaEventGeneratorBolt
 * <li>KafkaBoltWrapper
 * 
 * @author c.friaszapater
 *
 */
public class ReadInterpartyRelationshipTopology {


	private static final String INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME = "interpartyrelationship-read";

	public static final String KAFKA_EVENT_PRODUCER_COMPONENT_ID = "interpartyrelationship-read-kafka-event-producer";
	public static final String KAFKA_EVENT_GENERATOR_COMPONENT_ID = "interpartyrelationship-read-kafka-event-generator";
	public static final String INTERPARTYRELATIONSHIP_READ_FINAL_PROCESS_COMPONENT_ID = "interpartyrelationship-read-final-process";

	public static final String INTERPARTYRELATIONSHIP_READ_COMPONENT_ID = INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME;

	public static final String KAFKA_ERROR_PRODUCER_COMPONENT_ID = "interpartyrelationship-read-kafka-error-producer";
	public static final String KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID = "interpartyrelationship-read-kafka-event-error-process";
	public static final String KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID = "interpartyrelationship-read-kafka-event-success-process";
	public static final String KAFKA_EVENT_READER_COMPONENT_ID = "interpartyrelationship-read-kafka-event-reader";

	private final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipTopology.class);

	/**
	 * Set up Party topology and load it into storm.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		boolean local = false;
		if (args.length >= 1 && args[0].equals("local")) {
			LOG.info("*********** Local parameter received, will work with LocalCluster ************");
			local = true;
		}

		if (local) {
			LocalCluster cluster = new LocalCluster();
			loadTopologyInStorm(cluster);
			Thread.sleep(6000000);
			cluster.shutdown();
			ZookeeperUtils.close();
		} else {
			loadTopologyInStorm();
		}

	}

	public static void loadTopologyInStorm() throws Exception {
		loadTopologyInStorm(null);
	}

	/**
	 * Set up interPartyRelationship topology and load into storm.<br/>
	 * It may take some 2min to execute synchronously, then another some 2min to
	 * completely initialize storm asynchronously.<br/>
	 * Pre: kafka+zookeeper servers up in addresses as defined in TopologyConfig.
	 * 
	 * @param localCluster
	 *            null to submit to remote cluster.
	 */
	public static void loadTopologyInStorm(LocalCluster localCluster) throws Exception {
		LOG.info("Creating {} topology...", INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME);

		// Read configuration params from topology.properties and zookeeper
		TopologyConfig config = TopologyConfigFactory.getTopologyConfig();

		// Create the spout that read the events from Kafka
		GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT_ID,
				new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

		// Parse the events and we send it to the rest of the topology
		GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID,
				new ReadInterpartyRelationshipKafkaEventProcessBolt(), config.getEventProcessHints());
		kafkaEventProcess
				.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT_ID, KafkaSpout.EVENT_SUCCESS_STREAM));

		// Action bolts:

		// GetParty bolt
		GenericGrouping interPartyRelationshipEventGroup = new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS_COMPONENT_ID);
		GBolt<?> interPartyRelationshipBolt = new GRichBolt(INTERPARTYRELATIONSHIP_READ_COMPONENT_ID, new ReadInterpartyRelationshipBolt(null), config.getEventProcessHints());
		// Link to the former bolt
		interPartyRelationshipBolt.addGrouping(interPartyRelationshipEventGroup);

		// Final process
		GBolt<?> interPartyRelationshipFinalProcessBolt = new GRichBolt(INTERPARTYRELATIONSHIP_READ_FINAL_PROCESS_COMPONENT_ID, new ReadInterpartyRelationshipFinalProcessBolt(),
				config.getActionBoltHints());
		// Link to the former bolt
		interPartyRelationshipFinalProcessBolt.addGrouping(new ShuffleGrouping(INTERPARTYRELATIONSHIP_READ_COMPONENT_ID));

		// Event generator
		GBolt<?> kafkaEventGeneratorBolt = new GRichBolt(KAFKA_EVENT_GENERATOR_COMPONENT_ID, new KafkaEventGeneratorBolt(),
				config.getEventResponseHints());
		kafkaEventGeneratorBolt.addGrouping(new ShuffleGrouping(INTERPARTYRELATIONSHIP_READ_FINAL_PROCESS_COMPONENT_ID));
		// Send a event with the result
		KafkaBoltWrapper kafkaPublisherBoltWrapper = new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class);
		GBolt<?> kafkaEventProducer = new GRichBolt(KAFKA_EVENT_PRODUCER_COMPONENT_ID,
				kafkaPublisherBoltWrapper.getKafkaBolt(), config.getEventResponseHints());
		kafkaEventProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_GENERATOR_COMPONENT_ID));

		//
		// GBolt for work with the errors
		GBolt<?> kafkaEventError = new GRichBolt(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID, new EventErrorBolt(), config.getEventErrorHints());
		kafkaEventError
				.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT_ID, KafkaSpout.EVENT_ERROR_STREAM));
		// GBolt for send errors of events to kafka
		KafkaBoltWrapper kafkaErrorBoltWrapper = new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class);
		GBolt<?> kafkaErrorProducer = new GRichBolt(KAFKA_ERROR_PRODUCER_COMPONENT_ID,
				kafkaErrorBoltWrapper.getKafkaBolt(), config.getEventErrorHints());
		kafkaErrorProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID));


		// Build the topology
		StormTopology topology = TopologyFactory.generateTopology(kafkaEventReader,
				Arrays.asList(
						new GBolt[] { kafkaEventProcess, kafkaEventError, kafkaErrorProducer, interPartyRelationshipBolt, interPartyRelationshipFinalProcessBolt,
								kafkaEventGeneratorBolt, kafkaEventProducer }));

		LOG.info("{} Topology created, submitting it to storm...", INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME);

		// Create the basic config and upload the topology
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
		conf.setNumWorkers(config.getTopologyNumWorkers());

		if (localCluster != null) {
			localCluster.submitTopology(INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME, conf, topology);
			LOG.info("{} Topology submitted to storm (LocalCluster).", INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME);
		} else {
			StormSubmitter.submitTopology(INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME, conf, topology);
			LOG.info("{} Party Topology submitted to storm (StormSubmitter).", INTERPARTYRELATIONSHIP_READ_TOPOLOGY_NAME);
		}
	}
}
