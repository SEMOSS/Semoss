package prerna.engine.impl.r;

import org.rosuda.REngine.Rserve.RConnection;

public class RSingleton {
	
	private static RConnection rcon = null;
	
	private RSingleton()
	{
		
	}
	
	public static RConnection getConnection()
	{
		if(rcon == null || (rcon != null && !rcon.isConnected())) 
		{
			try {
				
				rcon = new RConnection();
				
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		return rcon;
	}
	

}
