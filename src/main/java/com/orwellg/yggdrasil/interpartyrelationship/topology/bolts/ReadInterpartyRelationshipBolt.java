package com.orwellg.yggdrasil.interpartyrelationship.topology.bolts;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.orwellg.umbrella.commons.repositories.scylla.InterPartyRelationshipPartyId1Repository;
import com.orwellg.umbrella.commons.repositories.scylla.InterPartyRelationshipRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.InterPartyRelationshipPartyId1RepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.InterPartyRelationshipRepositoryImpl;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationshipPartyId1;
import com.orwellg.umbrella.commons.utils.enums.InterpartyRelationshipEvents;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.interpartyrelationship.model.InterpartyRelationshipRead;

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

	private String propertiesFile = null;
	
	private String zookeeperhost = null;

	/**
	 * Usual constructor unless specific topologyPropertiesFile is needed.
	 * TopologyConfig.DEFAULT_PROPERTIES_FILE properties file will be used.
	 */
	public ReadInterpartyRelationshipBolt() {
		this.propertiesFile = null;
		this.zookeeperhost = null;
	}

	/**
	 * Constructor with specific topologyPropertiesFile (eg: for tests).
	 * @param propertiesFile TopologyConfig properties file. Can be null, in which case DEFAULT_PROPERTIES_FILE will be used.
	 */
	@Deprecated
	public ReadInterpartyRelationshipBolt(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

	/**
	 * @param propertiesFile
	 * @param zookeeperhost
	 */
	public ReadInterpartyRelationshipBolt(String propertiesFile, String zookeeperhost) {
		super();
		this.propertiesFile = propertiesFile;
		this.zookeeperhost = zookeeperhost;
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
			
			InterpartyRelationshipEvents event = InterpartyRelationshipEvents.fromString(eventName);

			switch(event) {
			case GET_INTERPARTYRELATIONSHIP:{
				InterPartyRelationship result = getInterPartyRelationshipImpl(p.getId());
				LOG.info("{}Action {} for Read InterpartyRelationship {} finished. Sending the results {}.", getLogPreffix(), eventName, p,
						result);

				getCollector().emit(input, new Values(key, processId, result, eventName));
				break;
			}
			case LIST_INTERPARTYRELATIONSHIP:{
				List<InterPartyRelationship> result = listInterPartyRelationshipImpl(p.getId());
				LOG.info("{}Action {} for Read InterpartyRelationship {} finished. Sending the results {}.", getLogPreffix(), eventName, p,
						result);

				getCollector().emit(input, new Values(key, processId, result, eventName));
				break;
			}
			default:
				LOG.warn("{}Unrecognized action {} for this topology, silentlly discarding the tuple.", getLogPreffix(), eventName);
			}
			
			getCollector().ack(input);

			LOG.info("{}Action {} for Read InterpartyRelationship {} finished. Results sent successfully.", getLogPreffix(), eventName, p);
		} catch (Exception e) {
			LOG.error("{}Error in Action {} for Read InterpartyRelationship {}. Message: {}", getLogPreffix(), eventName, p, e.getMessage(),
					e);
			getCollector().reportError(e);
			getCollector().fail(input);
		}
	}

	private InterPartyRelationshipRepository getInterPartyRelationshipRepository() {
		ScyllaManager man = ScyllaManager.getInstance(TopologyConfigFactory.getTopologyConfig(propertiesFile, zookeeperhost).getScyllaConfig().getScyllaParams());
		return new InterPartyRelationshipRepositoryImpl(man.getSession(TopologyConfigFactory.getTopologyConfig(propertiesFile, zookeeperhost).getScyllaConfig().getScyllaParams().getKeyspace()));
	} 
	
	private InterPartyRelationship getInterPartyRelationshipImpl(String id) {
		return getInterPartyRelationshipRepository().getInterPartyRelationship(id);		
	}
	
	private InterPartyRelationshipPartyId1Repository getInterPartyRelationshipPartyId1Repository() {
		ScyllaManager man = ScyllaManager.getInstance(TopologyConfigFactory.getTopologyConfig(propertiesFile, zookeeperhost).getScyllaConfig().getScyllaParams());
		return new InterPartyRelationshipPartyId1RepositoryImpl(man.getSession(TopologyConfigFactory.getTopologyConfig(propertiesFile, zookeeperhost).getScyllaConfig().getScyllaParams().getKeyspace()));
	}
	
	private List<InterPartyRelationship> listInterPartyRelationshipImpl(String id) {
		List<InterPartyRelationship> ret = new LinkedList<InterPartyRelationship>();
		List<InterPartyRelationshipPartyId1> aux = getInterPartyRelationshipPartyId1Repository().getInterPartyRelationshipPartyId1(id);
		
		if(aux != null) {
			Iterator<InterPartyRelationshipPartyId1> auxIt = aux.iterator();
			while(auxIt.hasNext()) {
				InterPartyRelationshipPartyId1 ipr1 = auxIt.next();
				LOG.info("Chema: {}", ipr1);
				ret.add(getInterPartyRelationshipImpl(ipr1.getRelationshipId()));
			}
		}
				
		return ret; 		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "processId", "result", "eventName"));
	}

	@Override
	public void declareFieldsDefinition() {
	}

}
