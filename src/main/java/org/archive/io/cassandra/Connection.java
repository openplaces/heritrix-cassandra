package org.archive.io.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class Connection {

	private Cassandra.Client _client;
	private TSocket _socket;
	private String _host;
	private int _port;
	
	public Connection(String host, int port) throws TException {
		_host = host;
		_port = port;
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
	
	public void connect() throws TTransportException {
		if (isClosed()) {
			_socket = new TSocket(_host, _port);
			_client = new Cassandra.Client(new TBinaryProtocol(new TFramedTransport(_socket), false, false));
			_socket.open();
		}
	}

}
