package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public abstract class AbstractModelEngine implements IModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractModelEngine.class);
	
	private String engineId = null;
	private String engineName = null;
	private String engineDirectoryPath = null;

	protected Properties smssProp = null;
	private String smssFilePath = null;
	
	private boolean keepConversationHistory = false;
	private boolean keepInputOutput = false;
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
				
		this.engineDirectoryPath = EngineUtility.getSpecificEngineBaseFolder(this.getCatalogType(), this.getEngineId(), this.getEngineName());

		this.keepConversationHistory = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_CONVERSATION_HISTORY));
		this.keepInputOutput = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_INPUT_OUTPUT));
				
		if (this.smssProp.containsKey(ModelEngineConstants.KEEP_CONTEXT)) {
			boolean keepContext = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_CONTEXT));
			this.keepConversationHistory = keepContext;
			this.keepInputOutput = keepContext;
		}
	}

	@Override
	public AskModelEngineResponse ask(String question, String context, Insight insight, Map<String, Object> parameters) {		
		if(parameters == null) {
			parameters = new HashMap<String, Object>();
		}
		
		LocalDateTime inputTime = LocalDateTime.now();
		AskModelEngineResponse askModelResponse = askCall(question, context, insight, parameters);
		LocalDateTime outputTime = LocalDateTime.now();
				
		askModelResponse.setMessageId(UUID.randomUUID().toString());
		askModelResponse.setRoomId(insight.getInsightId());
		
		if (Utility.isModelInferenceLogsEnabled()) {
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					askModelResponse.getMessageId(), 
					"ask", 
					this, 
					insight,
					context, 
					question,
					askModelResponse.getNumberOfTokensInPrompt(),
					inputTime, 
					askModelResponse.getResponse(),
					askModelResponse.getNumberOfTokensInResponse(),
					outputTime
			));
			inferenceRecorder.start();
		}

		return askModelResponse;
	}
	
	/**
	 * This is an abstract method that all subclasses should implement to handle text generation inference with
	 * the model engine.
	 * 
	 * @param question
	 * @param context
	 * @param insight
	 * @param parameters
	 * @return
	 */
	protected abstract AskModelEngineResponse askCall(String question, String context, Insight insight, Map<String, Object> parameters);
	

	@Override
	public EmbeddingsModelEngineResponse embeddings(List<String> stringsToEmbed, Insight insight, Map <String, Object> parameters) {		
		classLogger.info("Making embeddings call on engine " + this.engineId);

		LocalDateTime inputTime = LocalDateTime.now();
		EmbeddingsModelEngineResponse embeddingsResponse = embeddingsCall(stringsToEmbed, insight, parameters);
		LocalDateTime outputTime = LocalDateTime.now();

		classLogger.info("Embeddings Received from engine " + this.engineId);
	
		if (Utility.isModelInferenceLogsEnabled()) {
			String messageId = UUID.randomUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId, 
					"embeddings", 
					this, 
					insight, 
					null,
					"",
					embeddingsResponse.getNumberOfTokensInPrompt(),
					inputTime, 
					"",
					embeddingsResponse.getNumberOfTokensInResponse(),
					outputTime
			));
			inferenceRecorder.start();
		}
 		
		return embeddingsResponse;
	}
	
	/**
	 * This is an abstract handles how a given class will the create embeddings.
	 * 
	 * @param stringsToEmbed
	 * @param insight
	 * @param parameters
	 * @return
	 */
	protected abstract EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight, Map <String, Object> parameters);
	
	@Override
	public Object model(Object input, Insight insight, Map <String, Object> parameters) {		
		LocalDateTime inputTime = LocalDateTime.now();
		Object modelCallResponse = modelCall(input, insight, parameters);
		LocalDateTime outputTime = LocalDateTime.now();
	
		if (Utility.isModelInferenceLogsEnabled()) {
			String messageId = UUID.randomUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId,
					"model", 
					this,
					insight,
					null,
					input + "",
					null,
					inputTime, 
					PyUtils.determineStringType(modelCallResponse),
					null,
					outputTime
			));
			inferenceRecorder.start();
		}
 				
		return modelCallResponse;
	}
	
	/**
	 * This is method handles a given classes model implementation.
	 * The model method implemented could perform any unique operation associated to the given model engine.
	 * 
	 * @param input
	 * @param insight
	 * @param parameters
	 * @return
	 */
	protected abstract Object modelCall(Object input, Insight insight, Map <String, Object> parameters);
	
	protected String getModelEngineBaseFolder() {
		return this.engineDirectoryPath;
	}
	
	public boolean keepsConversationHistory() {
		return this.keepConversationHistory;
	}
	
	public boolean keepInputOutput() {
		return this.keepInputOutput;
	}
	
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
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.MODEL;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getModelType().toString();
	}
	
	@Override
	public void delete() {
		classLogger.debug("Delete model engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(
				EngineUtility.getSpecificEngineBaseFolder
					(IEngine.CATALOG_TYPE.FUNCTION, this.engineId, this.engineName)
				);
		if(engineFolder.exists()) {
			classLogger.info("Delete model engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info("Model engine folder " + engineFolder + " does not exist");
		}
		
		classLogger.info("Deleting model engine smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		String engineIds = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		engineIds = engineIds.replace(";" + this.engineId, "");
		// in case we are at the start
		engineIds = engineIds.replace(this.engineId + ";", "");
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
		DIHelper.getInstance().removeEngineProperty(this.engineId);
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
