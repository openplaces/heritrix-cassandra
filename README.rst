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

 * keyspace [required]: The name of your Cassandra keyspace.

 * crawlColumnFamily (defaults to 'crawl'): Name of the column family to use.

 * encodingScheme (defaults to 'UTF-8'): Encoding scheme you're using.

 * contentSuperColumn (defaults to 'content'): Name of the super column used to save the raw content to.

 * contentSubColumn (defaults to 'raw_data'): Name of the sub column used to save the raw content to.

 * curiSuperColumn (defaults to 'curi'): Name of the super column used to store the metadata related to the crawl.

 * ipSubColumn (defaults to 'ip'): Name of the sub column used to save the resolved ip to.

 * pathFromSeedSubColumn (defaults to 'path-from-seed'): Name of the sub column used to save the path from the seed to.

 * isSeedSubColumn (defaults to 'is-seed'): Name of the sub column used to store the boolean of whether the current entry is a seed.

 * viaSubColumn (defaults to 'via'): Name of the sub column used to store the via information.

 * urlSubColumn (defaults to 'url'): Name of the sub column used to store the url.

 * requestSubColumn (defaults to 'request'): Name of the sub column used to store the request header.


Building
--------
If you can't find a release that corresponds to your combination of Heritrix and Cassandra versions, then you can build your own version of heritrix-cassandra (granted that the APIs of each application haven't changed dramatically).

1) Obtain the heritrix-cassandra source by visiting http://github.com/openplaces/heritrix-cassandra

2) Create a new folder in **lib** (e.g. cassandra-0.*.* or heritrix-3.*.*) containing all the necessary dependencies. Check the existing folders for the required jars.

3) Edit build.xml and change the properties "version", "cassandra-version", "heritrix-version" accordingly.

4) Run "ant" in the command line, and your new jar should be in the **target** folder.


Releases
--------

Version 0.2: Compiled and intended to run with Heritrix 3.0.0 and Cassandra 0.6.1

Version 0.1: Compiled and intended to run with Heritrix 3.0.0 and Cassandra 0.6.0

