package prerna.sablecc2.reactor;

import prerna.auth.User;
import prerna.ds.py.PyUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;


public class ReconnectServer extends AbstractReactor
{
	// reconnects the server
	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		User user = this.insight.getUser();
		
		if(user == null)
			return NounMetadata.getErrorNounMessage("Cannot restart server. User not valid");
		
		boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
		&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");

		if(!useNettyPy)
			return NounMetadata.getErrorNounMessage("TCP Server is not available on this server");

		
		if(user.getTCPServer(false) != null && user.getTCPServer(false).isConnected())
			return NounMetadata.getErrorNounMessage("TCP Server is already available");

		if(user.getTCPServer(false) != null && !user.getTCPServer(false).isConnected()) // it was there previously
			user.getTCPServer(true);
		
		if(user.getTCPServer(false) != null && user.getTCPServer(false).isConnected())
			return new NounMetadata("TCP Server available and connected", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		
		return null;
	}
	
	
}