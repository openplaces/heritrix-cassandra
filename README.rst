Heritrix-Cassandra
==================

A library for writing Heritrix 3 output directly to Cassandra as records.


Getting Started
---------------

1) Visit http://github.com/openplaces/heritrix-cassandra/tree/master/releases/ and obtain a release of heritrix-cassandra that corresponds to the versions of Heritrix and Cassandra you are running. Consult the "Releases" section for more information.

2) Copy the heritrix-cassandra-{version}.jar file into your Heritrix install's **lib** folder.

3) Copy the following list of files from your Cassandra **lib** folder into your Heritrix install's **lib** folder:
    - apache-cassandra-*.jar
    - libthrift-*.jar
    - log4j-*.jar
    - slf4j-api-*.jar
    - slf4j-log4j*.jar

4) Modify your Heritrix job configuration to use the heritrix-cassandra writer

  crawler-beans.cxml::

    <!-- DISPOSITION CHAIN -->
    <bean id="cassandraParameters" class="org.archive.io.cassandra.CassandraParameters">
      <!-- At a minimum, you need to define a keyspace value -->
      <property name="keyspace" value="MyApplication" />

      <!-- Change the crawlColumnFamily from its default value of 'crawl' -->
      <property name="crawlColumnFamily" value="crawled_pages" />

      <!-- Other parameters are overridden similarly and a full list is provided below -->
    </bean>

    <bean id="cassandraWriterProcessor" class="org.archive.modules.writer.CassandraWriterProcessor">
      <!-- Pass a comma-separated list of servers to Cassandra here -->
      <property name="cassandraServers" value="localhost,127.0.0.1" />
      <!-- This is the thrift port -->
      <property name="cassandraPort" value="9160" />
      <property name="cassandraParameters">
        <!-- Referencing the named bean we defined above -->
        <bean ref="cassandraParameters" />
      </property>
    </bean>

    [...]

    <bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
      <property name="processors">
        <list>
          <!-- write to aggregate archival files... -->
          <ref bean="cassandraWriterProcessor"/>
          <!-- other references -->
        </list>
      </property>
    </bean>


org.archive.io.cassandra.CassandraParameters
--------------------------------------------

======================  =============== =========
Parameter           	Default Value	Description
======================  =============== =========
keyspace                (none)		The name of your Cassandra keyspace.

crawlColumnFamily       crawl 		Name of the column family to use.

encodingScheme 		UTF-8 		Encoding scheme you're using.

framedTransport		false		Whether to used Thrift's Framed Transport

contentPrefix		content 	Name of the logical prefix used to save the raw content to. If contentColumnName is redefined, then this prefix will be overridden and no longer used.

contentColumnName 	raw_data 	Name of the column used to save the raw content to.

curiPrefix		curi 		Name of the logical prefix used to store the metadata related to the crawl. If any of the following parameters are redefined, then this prefix will be overridden and no longer used with it.

ipColumnName 		ip 		Name of the column used to save the resolved ip to.

pathFromSeedColumnName 	path-from-seed 	Name of the column used to save the path from the seed to.

isSeedColumnName 	is-seed 	Name of the column used to store the boolean of whether the current entry is a seed.

viaColumnName 		via 		Name of the column used to store the via information.

urlColumnName 		url		Name of the column used to store the url.

requestColumnName 	request		Name of the column used to store the request header.
======================  =============== =========


Building
--------
If you can't find a release that corresponds to your combination of Heritrix and Cassandra versions, then you can build your own version of heritrix-cassandra (granted that the APIs of each application haven't changed dramatically).

1) Obtain the heritrix-cassandra source by visiting http://github.com/openplaces/heritrix-cassandra

2) Create a new folder in **lib** (e.g. cassandra-0.*.* or heritrix-3.*.*) containing all the necessary dependencies. Check the existing folders for the required jars.

3) Edit build.xml and change the properties "version", "cassandra-version", "heritrix-version" accordingly.

4) Run "ant" in the command line, and your new jar should be in the **target** folder.


Releases
--------
Each release of heritrix-cassandra is compiled against different version combinations of Heritrix and Cassandra. The following table summarizes them.

==================  ========  =========
heritrix-cassandra  Heritrix  Cassandra
==================  ========  =========
       0.4           3.0.0      0.6.1
       0.3           3.0.0      0.6.0
       0.2           3.0.0      0.6.1
       0.1           3.0.0      0.6.0
==================  ========  =========

