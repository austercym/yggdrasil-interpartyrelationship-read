package com.orwellg.yggdrasil.interpartyrelationship.topology.bolts;

import java.util.List;
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
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;

/**
 * 
 * @author c.friaszapater
 *
 */
public class ReadInterpartyRelationshipFinalProcessBolt extends BasicRichBolt {

	private Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipFinalProcessBolt.class);

	private String logPreffix;

	protected Gson gson;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		gson = new Gson();
	}
	
	@Override
	public void execute(Tuple input) {

		LOG.info("Tuple input = {}", input);

		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		String eventName = (String) input.getValueByField("eventName");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		LOG.info("{}Read InterpartyRelationship action Finished. Processing the results", logPreffix);

		try {
			List<InterPartyRelationship> product = (List<InterPartyRelationship>) input.getValueByField("result");

			// Return Product Read
			String result = gson.toJson(product);

			LOG.info("{}Creating event {} to send to Kafka topic.", logPreffix, eventName);

			Values tuple = new Values(key, processId, eventName, result);
			getCollector().emit(input, tuple);
			getCollector().ack(input);

			LOG.info("{}Read InterpartyRelationship Complete event sent to Kafka topic, tuple = {}.", logPreffix, tuple);
		} catch (Exception e) {
			LOG.error("{}Error trying to send Read InterpartyRelationship Complete event. Message: {},", logPreffix, e.getMessage(), e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "eventName", "result"));
	}

	@Override
	public void declareFieldsDefinition() {
		// TODO Auto-generated method stub
		
	}

}
