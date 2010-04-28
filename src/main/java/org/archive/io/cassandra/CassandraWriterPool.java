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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.archive.io.DefaultWriterPoolSettings;
import org.archive.io.WriterPool;


/**
 * @author greglu
 */
public class CassandraWriterPool extends WriterPool {

    /**
     * Create a pool of CassandraWriter objects.
     * 
     * @param cassandraServers
     * @param cassandraPort
     * @param parameters the {@link org.archive.io.cassandra.CassandraParameters} object containing your settings
     * @param poolMaximumActive the maximum number of writers in the writer pool.
     * @param poolMaximumWait the maximum waittime for all writers in the pool.
     */
    public CassandraWriterPool(final String cassandraServers, final int cassandraPort, final CassandraParameters parameters,
    		final int poolMaximumActive, final int poolMaximumWait) {
        super(
            new AtomicInteger(), 
            new BasePoolableObjectFactory() {
                public Object makeObject() throws Exception {
                    return new CassandraWriter(cassandraServers, cassandraPort, parameters);
                }

                public void destroyObject(Object cassandraWriter) throws Exception {
                    ((CassandraWriter) cassandraWriter).close();
                    super.destroyObject(cassandraWriter);
                }
            },
            new DefaultWriterPoolSettings(), 
            poolMaximumActive,
            poolMaximumWait);
    }
}
