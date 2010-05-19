package org.archive.io.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

public class Connection {

	public static final int MAX_CONNECTION_ATTEMPTS = 3;
	
	Cassandra.Client _client;
	TSocket _socket;
	String _host;
	int _port = 9160;
	
	public Connection(String host, int port) throws TException {
		_client = connectToCassandra(host, port);
	}
	
	public Cassandra.Client client() {
		return _client;
	}
	
	public String getHost() {
		return _host;
	}
	
	public void close() {
		if (_socket != null) _socket.close();
	}
	
	public boolean isClosed() {
		return (_socket == null || !_socket.isOpen());
	}

	private Cassandra.Client connectToCassandra(String host, int port) throws TException {
		// Try to connect (up to maximum attempts)
		_host = host;
		_port = port;
		for (int i=1 ; i < MAX_CONNECTION_ATTEMPTS ; i++) {
			try {
				if (_socket != null) _socket.close(); // close any previously open sockets
				_socket = new TSocket(_host, _port);
				TBinaryProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(_socket), false, false);
				Cassandra.Client client = new Cassandra.Client(binaryProtocol);
				_socket.open();
				return client;
			} catch (TException e) {
				try {
					// sleep before trying again.
	                Thread.sleep(1000);
                } catch (InterruptedException e1) {
                } 
				continue;
			}
		}
		throw new TException("Reached Maximum Number of connection attempts; couldn't get connection to host: " + _host);
	}
	
}
