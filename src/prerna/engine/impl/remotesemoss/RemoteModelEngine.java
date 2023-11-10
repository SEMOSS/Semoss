package prerna.engine.impl.remotesemoss;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class RemoteModelEngine implements IModelEngine {

	String smssFilePath = null;
	Properties smssProp = null;
	
	@Override
	public void setEngineId(String engineId) {
		// TODO Auto-generated method stub
		smssProp.put(Constants.ENGINE, engineId);

	}

	@Override
	public String getEngineId() {
		// TODO Auto-generated method stub
		return smssProp.getProperty(Constants.ENGINE);
	}

	@Override
	public void setEngineName(String engineName) {
		// TODO Auto-generated method stub
		smssProp.put(Constants.ENGINE_ALIAS, engineName);
	}

	@Override
	public String getEngineName() {
		// TODO Auto-generated method stub
		return smssProp.getProperty(Constants.ENGINE_ALIAS);
	}

	@Override
	public void open(String smssFilePath) throws Exception {
		// TODO Auto-generated method stub
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		// TODO Auto-generated method stub
		this.smssProp = smssProp;

	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		// TODO Auto-generated method stub
		this.smssFilePath = smssFilePath;

	}

	@Override
	public String getSmssFilePath() {
		// TODO Auto-generated method stub
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		// TODO Auto-generated method stub
		this.smssProp = smssProp;
		
	}

	@Override
	public Properties getSmssProp() {
		// TODO Auto-generated method stub
		return smssProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		// TODO Auto-generated method stub
		return IEngine.CATALOG_TYPE.MODEL;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		// TODO Auto-generated method stub
		return "remote";
	}

	@Override
	public void delete() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean holdsFileLocks() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		// for remote engine.. I dont have to do anything here
	}

	@Override
	public ModelTypeEnum getModelType() {
		// TODO Auto-generated method stub
		return ModelTypeEnum.REMOTE;
	}

	@Override
	public void startServer() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> ask(String question, String context, Insight insight, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object embeddings(List<String> stringsToEncode, Insight insight, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object model(String question, Insight insight, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

}
