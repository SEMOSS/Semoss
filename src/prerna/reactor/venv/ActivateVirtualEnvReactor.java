package prerna.reactor.venv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;

public class ActivateVirtualEnvReactor extends AbstractReactor {

	private static Logger classLogger = LogManager.getLogger(ActivateVirtualEnvReactor.class);

	public ActivateVirtualEnvReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	public NounMetadata execute() {
		this.organizeKeys();
		
		String venvName = this.keyValue.get(this.keysToGet[0]);

		User user = this.insight.getUser();
		if(user == null) {
			return NounMetadata.getErrorNounMessage("Cannot restart server. User not valid");
		}
		
		// sadly, the logic right now requires we have a made cpw
		// otherwise the reconnect method does nto 
		ClientProcessWrapper cpw = user.getClientProcessWrapper();
		if(cpw == null || cpw.getSocketClient() == null) {
			user.getSocketClient(true, venvName);
			return new NounMetadata("TCP Server was not initialized but is now started and connected", PixelDataType.CONST_STRING);
		}
		cpw.shutdown();
		try {
			cpw.reconnect(venvName);
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