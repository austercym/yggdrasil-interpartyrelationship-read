package com.orwellg.yggdrasil.interpartyrelationship.topology.bolts;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.utils.enums.InterpartyRelationshipEvents;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.interpartyrelationship.model.InterpartyRelationshipRead;
import com.orwellg.yggdrasil.interpartyrelationship.nosqldao.InterPartyRelationshipNoSqlDao;

/**
 * 
 * @author c.friaszapater
 *
 */
public class ReadInterpartyRelationshipBolt extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(ReadInterpartyRelationshipBolt.class);

	private String logPreffix;

	private String propertiesFile;

	/**
	 * Usual constructor unless specific topologyPropertiesFile is needed.
	 * TopologyConfig.DEFAULT_PROPERTIES_FILE properties file will be used.
	 */
	public ReadInterpartyRelationshipBolt() {
		this.propertiesFile = null;
	}

	/**
	 * Constructor with specific topologyPropertiesFile (eg: for tests).
	 * @param propertiesFile TopologyConfig properties file. Can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
	 */
	public ReadInterpartyRelationshipBolt(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

	protected String getLogPreffix() {
		return logPreffix;
	}

	@Override
	public void execute(Tuple input) {

		LOG.info("Tuple input = {}", input);

		// Received tuple is: key, processId, eventName, data

		String key = (String) input.getValueByField("key");
		String processId = (String) input.getValueByField("processId");
		String eventName = (String) input.getValueByField("eventName");

		logPreffix = String.format("[Key: %s][ProcessId: %s]: ", key, processId);

		InterpartyRelationshipRead p = null;

		try {
			p = (InterpartyRelationshipRead) input.getValueByField("eventData");

			LOG.info("{}Action {} starting for Read PartyContract {}.", getLogPreffix(), eventName, p);

			if (p == null) {
				throw new Exception("InterpartyRelationship Id null.");
			}

			InterpartyRelationshipRead partyContractRead = p;
			List<InterPartyRelationship> result = new ArrayList<InterPartyRelationship>();

			if (StringUtils.equals(InterpartyRelationshipEvents.GET_INTERPARTYRELATIONSHIP.getEventName(), eventName)) {

				result = retrieveInterpartyRelationship(partyContractRead.getId());
			} else {
				if (StringUtils.equals(InterpartyRelationshipEvents.LIST_INTERPARTYRELATIONSHIP.getEventName(), eventName)) {

					result = retrieveInterpartyRelationshipParty(partyContractRead.getId());
				}

			}


			LOG.info("{}Action {} for Read InterpartyRelationship {} finished. Sending the results {}.", getLogPreffix(), eventName, p,
					result);

			getCollector().emit(input, new Values(key, processId, result, eventName));
			getCollector().ack(input);

			LOG.info("{}Action {} for Read InterpartyRelationship {} finished. Results sent successfully.", getLogPreffix(), eventName, p);
		} catch (Exception e) {
			LOG.error("{}Error in Action {} for Read InterpartyRelationship {}. Message: {}", getLogPreffix(), eventName, p, e.getMessage(),
					e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}


	protected List<InterPartyRelationship> retrieveInterpartyRelationship(String interpartyRelationshipId) {
		// Get scylladb connection
		ScyllaManager man = ScyllaManager.getInstance(TopologyConfigFactory.getTopologyConfig(propertiesFile).getScyllaConfig().getScyllaParams().getNodeList());
				
		String keyspace = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams().getKeyspace();
		
		// Read InterpartyRelationship from noSql database
		InterPartyRelationshipNoSqlDao ineterpartyRelationshipDAO = new InterPartyRelationshipNoSqlDao(man, keyspace);
		// Result for next bolt: InterpartyRelationship
		List<InterPartyRelationship> result = ineterpartyRelationshipDAO.getById(interpartyRelationshipId);

		return result;
	}



	private List<InterPartyRelationship> retrieveInterpartyRelationshipParty(String partyId) {
		// Get scylladb connection
		ScyllaManager man = ScyllaManager.getInstance(TopologyConfigFactory.getTopologyConfig(propertiesFile).getScyllaConfig().getScyllaParams().getNodeList());
		// Read InterpartyRelationship from noSql database
		
		String keyspace = TopologyConfigFactory.getTopologyConfig().getScyllaConfig().getScyllaParams().getKeyspace();
		
		InterPartyRelationshipNoSqlDao ineterpartyRelationshipDAO = new InterPartyRelationshipNoSqlDao(man, keyspace);
		// Result for next bolt: InterpartyRelationship
		List<InterPartyRelationship> result = ineterpartyRelationshipDAO.getByPartyId(partyId);

		return result;
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "result", "eventName"));
	}

	@Override
	public void declareFieldsDefinition() {
	}

}
