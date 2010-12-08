/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.archive.io.cassandra;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;


/**
 * @author greglu
 */
public class CassandraWriterPool extends WriterPool {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private LinkedList<String> _endPoints = null;
	private CassandraParameters _parameters;
	
	/**
	 * Create a pool of CassandraWriter objects.
	 *
	 * @param cassandraSeeds
	 * @param cassandraPort
	 * @param parameters the {@link org.archive.io.cassandra.CassandraParameters} object containing your settings
	 * @param poolMaximumActive the maximum number of writers in the writer pool.
	 * @param poolMaximumWait the maximum waittime for all writers in the pool.
	 */
	public CassandraWriterPool(final CassandraParameters parameters, final WriterPoolSettings settings,
			final int poolMaximumActive, final int poolMaximumWait) {
		super(new AtomicInteger(), settings, poolMaximumActive, poolMaximumWait);
		_parameters = parameters;
	}
	
	private LinkedList<String> getEndPoints() throws InterruptedException {
		if (_endPoints == null) {
			Set<String> endPoints = new HashSet<String>();
			for (TokenRange range : getRanges()) {
				endPoints.addAll(range.getEndpoints());
			}
			_endPoints = new LinkedList<String>(endPoints);
		}
		return _endPoints;
	}

	private List<TokenRange> getRanges() throws InterruptedException {
		for (String seed : _parameters.getSeedsArray()) {
			try {
				Connection seedConnection = new Connection(seed, _parameters.getPort());
				return seedConnection.getClient().describe_ring(_parameters.getKeyspace());
			} catch (TException e) {
				LOG.error("The following error occurred while trying to access the seed: " + seed + "\n" +
						e.getMessage());
			} catch (InvalidRequestException e) {
				throw new RuntimeException(e);
			}
		}
		throw new RuntimeException("Cannot get token ranges from any of the seeds: " +
				Arrays.deepToString(_parameters.getSeedsArray()));
	}

	@Override
	protected WriterPoolMember makeWriter() {
		try {
			String head = getEndPoints().removeFirst();
			getEndPoints().addLast(head); // Move to the end of the list
			return (WriterPoolMember)new CassandraWriter(new Connection(head, _parameters.getPort()), _parameters);
		} catch (TTransportException e) {
		} catch (IOException e) {
		} catch (TException e) {
		} catch (InterruptedException e) {
		}
		return null;
	}

}
