package prerna.engine.impl.r;

import org.rosuda.REngine.Rserve.RConnection;

public class RserveConnectionMeta {

	private final String host;
	private final int port;
	private RConnection rcon;
	
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
	
	public RConnection getRcon() {
		return rcon;
	}

	public void setRcon(RConnection rcon) {
		this.rcon = rcon;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!RserveConnectionMeta.class.isAssignableFrom(obj.getClass())) {
			return false;
		}
		RserveConnectionMeta other = (RserveConnectionMeta) obj;
		if (other.getHost().equals(this.host) && other.getPort() == this.port) {
			return true;
		}
		return false;
	}
		
}
