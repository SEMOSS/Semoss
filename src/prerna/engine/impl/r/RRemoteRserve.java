package prerna.engine.impl.r;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.DIHelper;

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