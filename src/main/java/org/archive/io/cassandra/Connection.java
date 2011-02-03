package org.archive.io.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class Connection {

	private Cassandra.Client _client;
	private TTransport _socket;
	private String _host;
	private int _port;
	private String _keyspace;
	
	public Connection(String host, int port, String keyspace) throws TException, InvalidRequestException {
		_host = host;
		_port = port;
		_keyspace = keyspace;
		connect();
	}
	
	public Cassandra.Client getClient() {
		return _client;
	}
	
	public String getHost() {
		return _host;
	}
	
	public void close() {
		_socket.close();
	}
	
	public boolean isClosed() {
		return (_socket == null || !_socket.isOpen());
	}
	
	public void connect() throws InvalidRequestException, TException {
		if (isClosed()) {
			_socket = new TFramedTransport(new TSocket(_host, _port));
			_socket.open();
			_client = new Cassandra.Client(new TBinaryProtocol(_socket));
			_client.set_keyspace(_keyspace);
		}
	}

}
