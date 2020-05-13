package prerna.engine.impl.r;

import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

public class RRemoteRserve {

	private RConnection rcon = null;

	public RConnection getConnection() {

		if(Boolean.parseBoolean(System.getenv("REMOTE_RSERVE"))){
			String server = System.getenv("REMOTE_RSERVE_IP");
			if(server.contains(":")){
				String[] hostInfo = server.split(":", 2);
				try {
					String host = hostInfo[0];
					int port = Integer.parseInt(hostInfo[1]);
					rcon = new RConnection(host, port);
				} catch (RserveException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else{
				try {
					rcon = new RConnection(server);
				} catch (RserveException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return rcon;

	}
}