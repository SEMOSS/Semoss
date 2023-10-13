package prerna.reactor;

import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;


public class ReconnectServer extends AbstractReactor
{
	
	public ReconnectServer()
	{
		this.keysToGet = new String[] {"force", "port"};
	}
	// reconnects the server
	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		organizeKeys();
		String force = keyValue.get(keysToGet[0]);
		
		int forcePort = -1;
		
		if(keyValue.containsKey(keysToGet[1]))
			forcePort = Integer.parseInt(keyValue.get(keysToGet[1]));
		
		User user = this.insight.getUser();
		
		if(user == null)
			return NounMetadata.getErrorNounMessage("Cannot restart server. User not valid");
		
		boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
				&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");

		if(!useNettyPy)
			return NounMetadata.getErrorNounMessage("TCP Server is not available on this server");

		if(user.getTCPServer(false) != null && user.getTCPServer(false).isConnected())
		{
			if(force != null && force.equalsIgnoreCase("true"))
			{
				user.getTCPServer(false).stopPyServe(user.tupleSpace);
				user.setTCPServer(null);
			}
			else
				return NounMetadata.getErrorNounMessage("TCP Server is already available");
		}
		//|| (force != null && force.equalsIgnoreCase("true")) - this should already work
		if( ( (user.getTCPServer(false) != null && !user.getTCPServer(false).isConnected()) ) || user.getTCPServer(false) == null ) // it was there previously
			user.getTCPServer(true, forcePort);
		
		if(user.getTCPServer(false) != null && user.getTCPServer(false).isConnected())
			return new NounMetadata("TCP Server available and connected", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		
		return null;
	}
	
	
}