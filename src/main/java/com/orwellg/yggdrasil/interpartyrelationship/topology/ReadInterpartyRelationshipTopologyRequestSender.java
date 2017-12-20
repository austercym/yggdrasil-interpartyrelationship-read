package com.orwellg.yggdrasil.interpartyrelationship.topology;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.types.scylla.entities.PartyContract;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.InterpartyRelationshipEvents;

public class ReadInterpartyRelationshipTopologyRequestSender {

	public final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipTopologyRequestSender.class);

	protected Gson gson = new Gson();

	public static void main(String[] args) {
		String id = null;
		if (args.length >= 1) {
			id = args[0];
			LOG.info("Element ID parameter received = {}", id);
		}

		// Send request and wait response in specific topics
		ReadInterpartyRelationshipTopologyRequestSender test = new ReadInterpartyRelationshipTopologyRequestSender();
		
		LOG.info("Requesting Party with ID = {} to topology...", id);
		List<InterPartyRelationship> c = test.requestAndWaitResponseInterpartyRelationship(id);
		
		// Print result
		LOG.info("Retrieved balance = {}", c);
	}


	public List<InterPartyRelationship> requestAndWaitResponseInterpartyRelationship(String id) {
		String bootstrapServer = TopologyConfigFactory.getTopologyConfig().getKafkaBootstrapHosts();
		KafkaConsumer<String, String> consumer = makeConsumer(bootstrapServer);
		Producer<String, String> producer = makeProducer(bootstrapServer);

		// Generate event
		String parentKey = this.getClass().getName();
		String processId = "" + ThreadLocalRandom.current().nextLong(1000000);
		String uuid = UUID.randomUUID().toString();
		String eventKeyId = "EVENT-" + uuid;
		LOG.info("eventKeyId = {}", eventKeyId);
		String eventName = InterpartyRelationshipEvents.GET_INTERPARTYRELATIONSHIP.getEventName();
		
		// Send Id as eventData
		String serializedId = "{\"id\": " + id + " }";
		Event event = generateEvent(parentKey, processId, eventKeyId, eventName, serializedId);
		
		// Serialize event
		String base64Event = Base64.encodeBase64String(RawMessageUtils.encode(Event.SCHEMA$, event).array());
		LOG.info("Encoded message: " + base64Event);
		
		// Write event to kafka topic.
		String topic = TopologyConfigFactory.getTopologyConfig().getKafkaSubscriberSpoutConfig().getTopic().getName().get(0);
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
		String responseTopic = TopologyConfigFactory.getTopologyConfig().getKafkaPublisherBoltConfig().getTopic().getName().get(0);
		consumer.subscribe(Arrays.asList(responseTopic));
		
		List<InterPartyRelationship> result = waitAndConsume(eventKeyId, maxRetries, retries, interval, consumer);
		if (result != null) {
			LOG.info("CORRECT Response for eventKeyId = {} found, result = {}.", eventKeyId, result);
		} else {
			throw new RuntimeException("No response.");
		}
		
		consumer.close();
		return result;
	}
	

	protected KafkaConsumer<String, String> makeConsumer(String bootstrapServer) {
		Properties propsC = new Properties();
		propsC.put("bootstrap.servers", bootstrapServer);
		propsC.put("acks", "all");
		propsC.put("retries", 0);
		propsC.put("batch.size", 16384);
		propsC.put("linger.ms", 1);
		propsC.put("buffer.memory", 33554432);
		propsC.put("group.id", "test");
		propsC.put("enable.auto.commit", "true");
		propsC.put("auto.commit.interval.ms", "1000");
		propsC.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propsC);
		return consumer;
	}

	protected Producer<String, String> makeProducer(String bootstrapServer) {
		// Using kafka-clients library:
		// https://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
		Properties propsP = new Properties();
		propsP.put("bootstrap.servers", bootstrapServer);
		// props.put("bootstrap.servers", "172.31.17.121:6667");
		propsP.put("acks", "all");
		propsP.put("retries", 0);
		propsP.put("batch.size", 16384);
		propsP.put("linger.ms", 1);
		propsP.put("buffer.memory", 33554432);
		propsP.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsP.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(propsP);
		return producer;
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

	protected List<InterPartyRelationship> waitAndConsume(String eventKeyId, int maxRetries, int retries,
			int interval, KafkaConsumer<String, String> consumer) {
		
		List<InterPartyRelationship> result = null;
		
		while (result == null && retries < maxRetries) {
			
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				Event responseEvent = RawMessageUtils.decode(Event.SCHEMA$, Base64.decodeBase64(record.value()));
				LOG.info("responseEvent Wait and Consume = {}", responseEvent);
				String eventDataStr = responseEvent.getEvent().getData().toString();
				// Check if eventKeyId of the request matches the parent of the response

				if (eventKeyId.equals(responseEvent.getEvent().getParentKey().toString())) {
					result = gson.fromJson(eventDataStr, new TypeToken<List<InterPartyRelationship>>(){}.getType());
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
