package prerna.zookeeper;

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class ZKEngine implements IEngine {

	public static final String ZOOKEEPER_ADDRESS_KEY = "ZOOKEEPER_ADDRESS";
	public static final String SESSION_TIMEOUT_KEY = "SESSION_TIMEOUT";
	public static final String CONNECTION_TIMEOUT_KEY = "SESSION_TIMEOUT";
	public static final String NAMESPACE_KEY = "ELECTION_NAMESPACE";

	protected String engineId = null;
	protected String engineName = null;

	protected String smssFilePath = null;
	private Properties smssProp;

	private CuratorFramework curator;

	private String address;
	private int sessionTimeout = -1;
	private int connectionTimeout = -1;
	private String namespace;

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
		if(this.address == null || (this.address=this.address.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the address for the zookeeper");
		}
		this.namespace = smssProp.getProperty(NAMESPACE_KEY);

		String sessionTStr = smssProp.getProperty(SESSION_TIMEOUT_KEY);
		if(sessionTStr != null && (sessionTStr=sessionTStr.trim()).isEmpty()) {
			this.sessionTimeout = Integer.parseInt(sessionTStr);
		}
		
		String connectionTStr = smssProp.getProperty(CONNECTION_TIMEOUT_KEY);
		if(connectionTStr != null && (connectionTStr=connectionTStr.trim()).isEmpty()) {
			this.connectionTimeout = Integer.parseInt(connectionTStr);
		}
		
		Builder builder = CuratorFrameworkFactory.builder();
		builder.connectString(address);
		if(this.sessionTimeout > 0) {
			builder.sessionTimeoutMs(this.sessionTimeout);
		}
		if(this.connectionTimeout > 0) {
			builder.connectionTimeoutMs(this.connectionTimeout);
		}
		// optional namespace (base path added to all paths using this connection)
		if(this.namespace != null && !this.namespace.isEmpty()) {
			builder.namespace(this.namespace);
		}
		builder.retryPolicy(new ExponentialBackoffRetry(1000, 3));
		
		this.curator = builder.build();
		// start the curator client
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

	public CuratorFramework getCurator() {
		return this.curator;
	}

	public ZooKeeper getZookeeper() throws Exception {
		return this.curator.getZookeeperClient().getZooKeeper();
	}
	
}
