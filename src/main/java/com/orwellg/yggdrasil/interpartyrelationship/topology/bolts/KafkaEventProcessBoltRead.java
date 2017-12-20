package com.orwellg.yggdrasil.interpartyrelationship.topology.bolts;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;

/**
 * = Kafka Event Process Bolt superclass
 * 
 * That class define a base class for event receptors. 
 * 
 * The tuple with the event is received from a KafkaSpout.
 * 
 * This class decode the Kafka message, it uses Event Avro Scheme for that, and send a new tuple with
 * the datas to the next step in the topology.
 *
 * @author f.deborja
 *
 */
public abstract class KafkaEventProcessBoltRead extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LogManager.getLogger(KafkaEventProcessBoltRead.class);
	
	protected Gson gson;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		gson = new Gson();
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Method that decode the receive event coming from Kafka and send it to the next step in the topology
	 */
	@Override
	public void execute(Tuple input) {		
		
		LOG.info("Event received: {}. Starting the decode process.", input);
				
		try {
			Event event = null;
			if (input.getValues().get(4) != null && (input.getValues().get(4) instanceof ByteBuffer)) {
				event = RawMessageUtils.decode(Event.SCHEMA$, (ByteBuffer) input.getValues().get(4));
			} else if (input.getValues().get(4) instanceof String) {
				event = RawMessageUtils.decodeFromString(Event.SCHEMA$, (String) input.getValues().get(4));
			}
			
			LOG.info("[Key: {}][ProcessId: {}]: The event was decoded. Send the tuple to the next step in the Topology.", event.getEvent().getKey().toString(), event.getProcessIdentifier().getUuid());
			sendNextStep(input, event);
		} catch (Exception e) {
			LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
			error(e, input);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * [source,java]
	 * addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData"}));
	 */
	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "eventData", "eventName"}));
	}
	
	/**
	 * Send the tuple to the next step in the topology.
	 * 
	 * Subclasses can make additional modifications over the tuple and sent it.
	 * 
	 * @param input The original tuple that we have received
	 * @param event The event that we want send to the next step
	 */
	public abstract void sendNextStep(Tuple input, Event event);
}
