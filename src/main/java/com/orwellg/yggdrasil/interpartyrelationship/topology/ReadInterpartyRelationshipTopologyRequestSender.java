package com.orwellg.yggdrasil.interpartyrelationship.topology;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.InterpartyRelationshipEvents;
import com.orwellg.umbrella.commons.utils.kafka.SimpleKafkaConsumerProducerFactory;

public class ReadInterpartyRelationshipTopologyRequestSender {

	public final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipTopologyRequestSender.class);

	protected Gson gson = new Gson();

	public static void main(String[] args) {
		String id = null;
		String zookeeperhost = "";
		
		if(args == null)
			throw new IllegalArgumentException("Incorrect arguments. Use: zookeeperhost <InterpartyRelationshipId>");
		
		switch(args.length) {
		case 1:{
			id = "ID1";
			zookeeperhost = args[0];
			break;
		}
		case 2:{
			id = args[1];
			zookeeperhost = args[0];
			break;
		}			
		default:
			throw new IllegalArgumentException("Incorrect arguments. Use: zookeeperhost <InterpartyRelationshipId>");
		}

		// Send request and wait response in specific topics
		ReadInterpartyRelationshipTopologyRequestSender test = new ReadInterpartyRelationshipTopologyRequestSender();
		
		LOG.info("Requesting Party with ID = {} to topology...", id);
		InterPartyRelationship c = (InterPartyRelationship) test.requestAndWaitResponseInterpartyRelationship(InterpartyRelationshipEvents.GET_INTERPARTYRELATIONSHIP, id, TopologyConfigFactory.getTopologyConfig(null, zookeeperhost));
		
		// Print result
		LOG.info("Retrieved balance = {}", c);
	}


	public Object requestAndWaitResponseInterpartyRelationship(InterpartyRelationshipEvents eventType, String id, TopologyConfig config) {
		
		KafkaConsumer<String, String> consumer = SimpleKafkaConsumerProducerFactory.createConsumer(config.getKafkaConfig());
		Producer<String, String> producer = SimpleKafkaConsumerProducerFactory.createProducer(config.getKafkaConfig());

		// Generate event
		String parentKey = this.getClass().getName();
		String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
		String uuid = UUID.randomUUID().toString();
		String eventKeyId = "EVENT-" + uuid;
		LOG.info("eventKeyId = {}", eventKeyId);
		String eventName = eventType.getEventName();
		
		// Send Id as eventData
		String serializedId = "{\"id\": " + id + " }";
		Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedId);
		
		// Serialize event
		String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
		LOG.info("Encoded message: " + base64Event);
		
		// Write event to kafka topic.
		String topic = config.getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
		LOG.info("Sending event {} to topic {}...", event, topic);
		producer.send(new ProducerRecord<String, String>(topic, base64Event));
		LOG.info("Event {} sent to topic {}.", event, topic);

		//
		// Wait results
		int maxRetries = 120;
		int retries = 0;
		int interval = 1;
		
		// Check if topology returns kafka result event
		LOG.info("Checking if topology has returned to kafka topic...");
		String responseTopic = config.getKafkaPublisherBoltConfig().getTopic().getName().get(0);
		consumer.subscribe(Arrays.asList(responseTopic));
		
		Object result = waitAndConsume(eventKeyId, maxRetries, retries, interval, consumer);
		if (result != null) {
			LOG.info("CORRECT Response for eventKeyId = {} found, result = {}.", eventKeyId, result);
		} else {
			throw new RuntimeException("No response.");
		}
		
		consumer.close();
		return result;
	}
	
	/**
	 * Based on KafkaEventGeneratorBolt.generateEvent(), modified for this test.
	 * 
	 * @param parentKey
	 * @param processId
	 * @param eventKeyId
	 * @param eventName
	 * @param data
	 * @return
	 */
	private Event generateEvent(String parentKey, String processId, String eventKeyId, String eventName, String serializedData) {

		String logPreffix = String.format("[Key: %s][ProcessId: %s]: ", parentKey, processId);

		LOG.info("{} Generating event {} for topology", logPreffix, eventName);

		// Create the event type
		EventType eventType = new EventType();
		eventType.setName(eventName);
		eventType.setVersion(Constants.getDefaultEventVersion());
		eventType.setParentKey(parentKey);
		eventType.setKey(eventKeyId);
		eventType.setSource(this.getClass().getSimpleName());
		SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
		eventType.setTimestamp(format.format(new Date()));

		eventType.setData(serializedData);

		ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
		processIdentifier.setUuid(processId);

		EntityIdentifierType entityIdentifier = new EntityIdentifierType();
		entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
		entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

		// Create the corresponden event
		Event event = new Event();
		event.setEvent(eventType);
		event.setProcessIdentifier(processIdentifier);
		event.setEntityIdentifier(entityIdentifier);

		LOG.info("{}Event generated correctly: {}", logPreffix, event);

		return event;
	}

	protected Object waitAndConsume(String eventKeyId, int maxRetries, int retries,
			int interval, KafkaConsumer<String, String> consumer) {
		
		Object result = null;
		
		while (result == null && retries < maxRetries) {
			
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent Wait and Consume = {}", responseEvent);
				String eventDataStr = responseEvent.getEvent().getData().toString();
				// Check if eventKeyId of the request matches the parent of the response

				if (eventKeyId.equals(responseEvent.getEvent().getParentKey().toString())) {
					InterpartyRelationshipEvents event = InterpartyRelationshipEvents.fromString(responseEvent.getEvent().getName());
					switch(event) {
					case GET_INTERPARTYRELATIONSHIP:{
						result = gson.fromJson(eventDataStr, new TypeToken<InterPartyRelationship>(){}.getType());
						break;
					}
					case LIST_INTERPARTYRELATIONSHIP:{
						result = gson.fromJson(eventDataStr, new TypeToken<List<InterPartyRelationship>>(){}.getType());
						break;
					}
					}
					
				}
				
			}
			if (result == null) {
				retries++;
				LOG.info(
						"Response for eventKeyId = {} not found yet, retry {} after sleeping {}s...",
						eventKeyId, retries, interval);
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
}
