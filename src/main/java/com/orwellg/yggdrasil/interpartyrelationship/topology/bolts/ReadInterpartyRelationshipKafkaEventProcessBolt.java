package com.orwellg.yggdrasil.interpartyrelationship.topology.bolts;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.yggdrasil.interpartyrelationship.model.InterpartyRelationshipRead;

/**
 * Bolt that starts storm processing of Party actions received by topic.
 * 
 * @author c.friaszapater
 *
 */
public class ReadInterpartyRelationshipKafkaEventProcessBolt extends KafkaEventProcessBoltRead {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipKafkaEventProcessBolt.class);
	
	protected Gson gson;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
	}

	@Override
	public void sendNextStep(Tuple input, Event event) {

		String key = event.getEvent().getKey().toString();
		String processId = event.getProcessIdentifier().getUuid().toString();
		String eventName = event.getEvent().getName().toString();
		String eventDataStr = event.getEvent().getData().toString();

		LOG.info("[Key: {}][ProcessId: {}]: Receiving InterpartyRelationship Read action {} for processing in storm, eventData = {}.", key,
				processId, eventName, eventDataStr);

		InterpartyRelationshipRead eventData = gson.fromJson(eventDataStr, InterpartyRelationshipRead.class);

		// Pass the Board Director
		getCollector().emit(input, new Values(key, processId, eventName, eventData));
		getCollector().ack(input);

		LOG.info("[Key: {}][ProcessId: {}]: InterpartyRelationship Read action sent for processing in storm, eventData = {}.", key, processId, eventData);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "eventName", "eventData"));
	}

}
