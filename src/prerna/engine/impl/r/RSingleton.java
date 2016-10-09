package prerna.engine.impl.r;

import java.util.Hashtable;

import org.rosuda.REngine.Rserve.RConnection;

public class RSingleton {
	
	private static RConnection rcon = null;
	static int port = 6311;
	static Hashtable <Integer, RConnection> portToCon = new Hashtable<Integer, RConnection>(); // RServe is not allowing me to inspect the port so I have to do this.. sorry
	
	
	private RSingleton()
	{
		
	}
	
	public static RConnection getConnection()
	{
		if(rcon == null)
			return getConnection("127.0.0.1", 6311);
		return rcon;
	}
	
	public static RConnection getConnection(int port)
	{
		return getConnection("127.0.0.1", port);		
	}
	
	public static RConnection getConnection(String host, int port)
	{
		// this basically tries to do the same as get connection with a port
		if(portToCon.containsKey(port))
			rcon = portToCon.get(port);
		
		else
		{
			System.out.println("Making a master connection now on port" + port);;
			try {
				
				rcon = new RConnection(host, port);
				portToCon.put(port, rcon);
				
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
			
		}
		
		return rcon;
	}
	

}
