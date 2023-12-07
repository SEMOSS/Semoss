package prerna.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;

public class ReconnectServer extends AbstractReactor {

	private static Logger classLogger = LogManager.getLogger(ReconnectServer.class);

	public ReconnectServer() {
		this.keysToGet = new String[] {};
	}

	public NounMetadata execute() {
		User user = this.insight.getUser();
		if(user == null) {
			return NounMetadata.getErrorNounMessage("Cannot restart server. User not valid");
		}
		// sadly, the logic right now requires we have a made cpw
		// otherwise the reconnect method does nto 
		ClientProcessWrapper cpw = user.getClientProcessWrapper();
		if(cpw == null || cpw.getSocketClient() == null) {
			user.getSocketClient(true);
			return new NounMetadata("TCP Server was not initialized but is now started and connected", PixelDataType.CONST_STRING);
		}
		cpw.shutdown(false);
		try {
			cpw.reconnect();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata("Unable to restart TCP Server", PixelDataType.CONST_STRING);
		}
		SocketClient client = user.getSocketClient(false);
		if(client == null || !client.isConnected()) {
			return new NounMetadata("Unable to restart TCP Server", PixelDataType.CONST_STRING);
		}

		return new NounMetadata("TCP Server available and connected", PixelDataType.CONST_STRING);
	}

}