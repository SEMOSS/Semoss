package prerna.zookeeper;

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class ZKEngine implements IEngine {

	private static final String ZOOKEEPER_ADDRESS_KEY = "ZOOKEEPER_ADDRESS";
	private static final String SESSION_TIMEOUT_KEY = "SESSION_TIMEOUT";
	private static final String CONNECTION_TIMEOUT_KEY = "SESSION_TIMEOUT";
	private static final String NAMESPACE_KEY = "ELECTION_NAMESPACE";

	protected String engineId = null;
	protected String engineName = null;

	protected String smssFilePath = null;
	private Properties smssProp;

	private CuratorFramework curator;

	private String address = "localhost:2181";
	// ms values
	private int sessionTimeout = -1;
	private int connectionTimeout = -1;
	private String namespace = "";


	@Override
	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	@Override
	public String getEngineId() {
		return this.engineId;
	}

	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return this.engineName;
	}

	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);

		this.address = smssProp.getProperty(ZOOKEEPER_ADDRESS_KEY);
		this.namespace = smssProp.getProperty(NAMESPACE_KEY);

		String sessionTStr = smssProp.getProperty(SESSION_TIMEOUT_KEY);
		String connectionTStr = smssProp.getProperty(CONNECTION_TIMEOUT_KEY);

		if(sessionTStr != null && connectionTStr != null
				&& (sessionTStr=sessionTStr.trim()).isEmpty()
				&& (connectionTStr=connectionTStr.trim()).isEmpty()
				) {
			this.sessionTimeout = Integer.parseInt(sessionTStr);
			this.connectionTimeout = Integer.parseInt(connectionTStr);

		}

		if(sessionTimeout > 0 && connectionTimeout > 0) {
			this.curator = CuratorFrameworkFactory.newClient(this.address, sessionTimeout, connectionTimeout, new ExponentialBackoffRetry(1000, 3));
		} else {
			this.curator = CuratorFrameworkFactory.newClient(this.address, new ExponentialBackoffRetry(1000, 3));
		}

		// Start the curator client
		this.curator.start();
	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}

	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
	}

	@Override
	public Properties getSmssProp() {
		return this.smssProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		return this.smssProp;
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		if(this.curator != null) {
			this.curator.close();
		}
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

	public ZKCuratorUtility getCuratorUtility() {
		return new ZKCuratorUtility(this.curator);
	}

	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////


	public static void main(String[] args) throws Exception {
		ZKEngine engine = new ZKEngine();
		try {
			Properties prop = new Properties();
			prop.put(ZOOKEEPER_ADDRESS_KEY, "localhost:2181");
	
			engine.open(prop);
			
			ZKCuratorUtility utility = engine.getCuratorUtility();
			//utility.createPathIfNotExists("/VISN01/STATIONABC/2023-10-14");
			String newNode = utility.createSequentialNode("/VISN01/STATIONABC/2023-10-14/");
			System.out.println(newNode);
			
		} finally {
			engine.close();
		}
	}
}
