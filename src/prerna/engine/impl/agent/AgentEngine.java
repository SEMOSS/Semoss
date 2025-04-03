package prerna.engine.impl.agent;

import java.io.IOException;
import java.util.Properties;

import prerna.engine.api.IAgentEngine;
import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class AgentEngine implements IAgentEngine {
    private Properties smssProp = null;
    private String smssFilePath = null;
    private String agentId = null;
    private String agentName = null;

    @Override
	public Properties getSmssProp() {
		return this.smssProp;
	}
	
	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
    }

    @Override
    public void setEngineId(String engineId) {
        this.agentId = engineId;
    }

    @Override
    public String getEngineId() {
        return this.agentId;
    }

    @Override
    public void setEngineName(String engineName) {
        this.agentName = engineName;
    }

    @Override
    public String getEngineName() {
        return this.agentName;
    }

    @Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

    @Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.agentId = this.smssProp.getProperty("AGENT");
		this.agentName = this.smssProp.getProperty("AGENT_NAME");
	}


    @Override
    public Properties getOrigSmssProp() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrigSmssProp'");
    }

    @Override
    public CATALOG_TYPE getCatalogType() {
        return IEngine.CATALOG_TYPE.AGENT;
    }

    @Override
    public String getCatalogSubType(Properties smssProp) {
        return "agent_sub_type";
    }

    @Override
    public void delete() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    @Override
    public boolean holdsFileLocks() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'holdsFileLocks'");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public void setSmssFilePath(String smssFilePath) {
        this.smssFilePath = smssFilePath;
    }

    @Override
    public String getSmssFilePath() {
        return this.smssFilePath;
    }
    
}
