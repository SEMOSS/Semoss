package prerna.engine.impl.r;

public class RserveConnectionMeta {

	private final String host;
	private final int port;
	
	public RserveConnectionMeta(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
		
}
