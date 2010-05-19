package org.archive.io.cassandra;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class CassandraWriterFactory extends BasePoolableObjectFactory {
	
	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private LinkedList<String> _endPoints;
	private CassandraParameters _parameters;
	
	public CassandraWriterFactory(String[] seeds, CassandraParameters parameters) {
		_parameters = parameters;
		Set<String> endPoints = new HashSet<String>();
		for (TokenRange range : getRanges(seeds)) {
			endPoints.addAll(range.getEndpoints());
		}
		
		for (String endPoint : endPoints.toArray(new String[0])) {
			try {
				_endPoints.add(endPoint);
			} catch (Exception e) {
				LOG.warn("Error adding client for EndPoint: " + endPoint, e);
			}
		}
	}
	
    private List<TokenRange> getRanges(String[] seeds) {
    	for (String seed : seeds) {
			try {
				Connection seedConnection = new Connection(seed);
				return seedConnection.client().describe_ring(_parameters.getKeyspace());
			} catch (TException e) {
				// Seed is not accessible.
			}
    	}
    	throw new RuntimeException("Cannot get token ranges from any of the seeds: " + Arrays.deepToString(seeds));
    }
	
	@Override
    public Object makeObject() throws Exception {
		String head = _endPoints.removeFirst();
		_endPoints.addLast(head); // Move to the end of the list
	    return new CassandraWriter(new Connection(head), _parameters);
    }
	
	@Override
	public void destroyObject(Object obj) throws Exception {
		if (obj instanceof Connection) {
			((CassandraWriter)obj).close();
		}
	}

}
