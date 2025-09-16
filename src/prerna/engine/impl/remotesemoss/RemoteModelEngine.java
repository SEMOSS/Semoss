package prerna.engine.impl.remotesemoss;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
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
	public boolean isBasic() {
		return false;
	}

	@Override
	public void setBasic(boolean isBasic) {
		// always false
	}

	@Override
	public ModelTypeEnum getModelType() {
		// TODO Auto-generated method stub
		return ModelTypeEnum.REMOTE;
	}

	@Override
	public AskModelEngineResponse ask(String question, String context, Insight insight,
			Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InstructModelEngineResponse instruct(String task, String context, List<Map<String, Object>> projectData,
			Insight insight, Map<String, Object> parameters) {
		return null;
	}

	@Override
	public EmbeddingsModelEngineResponse embeddings(List<String> stringsToEncode, Insight insight,
			Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EmbeddingsModelEngineResponse imageEmbeddings(List<String> imagesToEmbed, Insight insight,
			Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AskModelEngineResponse askRoom(String question, String context, Room room, AbstractMessage inputMessage,  Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
		throw new NotImplementedException("This method has not been implemented yet...");
	}

	@Override
	public Map<String, Object> buildBedrockToolSpec() {
		throw new NotImplementedException("This method has not been implemented yet...");
	}

	@Override
	public Logger getEngineLogger(String loggerName) {
		// TODO Auto-generated method stub
		return null;
	}

}
