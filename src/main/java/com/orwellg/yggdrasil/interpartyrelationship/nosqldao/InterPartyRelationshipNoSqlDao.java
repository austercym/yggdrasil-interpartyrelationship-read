package com.orwellg.yggdrasil.interpartyrelationship.nosqldao;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orwellg.umbrella.avro.types.party.RelationshipType;
import com.orwellg.umbrella.commons.types.scylla.entities.InterPartyRelationship;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;

/**
 * DAO for scylladb access for Party.
 * TODO Replace this by a Repository, for instance see "yggdrasil-lasttransaction-get" project.
 * 
 * @author c.friaszapater
 *
 */
public class InterPartyRelationshipNoSqlDao {

	private final static Logger LOG = LogManager.getLogger(InterPartyRelationshipNoSqlDao.class);

	protected ScyllaManager man;

	protected Session ses;


	public InterPartyRelationshipNoSqlDao() {
	}

	public InterPartyRelationshipNoSqlDao(ScyllaManager man, String keyspace) {
		this.man = man;
		this.ses = man.getSession(keyspace);
	}

	public List<Row> getRowByPartyId(String partyId) {

		String selectQuery = "select * from interpartyrelationshippartyid1 where Party_Id1 = ?;";
		PreparedStatement st = ses.prepare(selectQuery);
		BoundStatement select = st.bind(partyId);
		ResultSet rs = ses.execute(select);
		List<Row> results = rs.all();

		return results;
	}

	public List<InterPartyRelationship> getByPartyId(String interPartyRelationshipId) {
		List<Row> listRow = getRowByPartyId(interPartyRelationshipId);
		List<InterPartyRelationship> listRowMapped = new ArrayList<InterPartyRelationship>();
		for (Row row : listRow) {
			if (row != null) {
				
				InterPartyRelationship p = new InterPartyRelationship();
				
				p.setRelationshipId(row.getString(1));
				p.setPartyId1(row.getString(0));
				p.setPartyId2(row.getString(2));
				p.setRelationshipType(RelationshipType.valueOf(row.getString(3)).toString());
				
				listRowMapped.add(p);

				LOG.info("InterPartyRelationship retrieved from nosql db: {}", p.toString());
			}
		}

		return listRowMapped;
	}
	
	public List<Row> getRowById(String partyId) {

		String selectQuery = "select * from interpartyrelationship where Relationship_ID = ?;";
		PreparedStatement st = ses.prepare(selectQuery);
		BoundStatement select = st.bind(partyId);
		ResultSet rs = ses.execute(select);
		List<Row> results = rs.all();

		return results;
	}

	public List<InterPartyRelationship> getById(String interPartyRelationshipId) {
		
		List<Row> listRow = getRowById(interPartyRelationshipId);
		
		List<InterPartyRelationship> listRowMapped = new ArrayList<InterPartyRelationship>();
		for (Row row : listRow) {
			if (row != null) {
				
				InterPartyRelationship p = new InterPartyRelationship();
				
				p.setRelationshipId(row.getString(0));
				p.setPartyId1(row.getString(1));
				p.setPartyId2(row.getString(2));
				p.setRelationshipType(RelationshipType.valueOf(row.getString(3)).toString());
				
				listRowMapped.add(p);

				LOG.info("InterPartyRelationship retrieved from nosql db: {}", p.toString());
			}
		}

		return listRowMapped;
	}

}
