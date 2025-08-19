package prerna.engine.impl.vector;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import prerna.auth.User;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.vector.interceptor.AbstractInterceptor;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.Utility;

public class ProxyVectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(ProxyVectorDatabaseEngine.class);
	
	private static final String TARGET_ENGINE_ID = "TARGET_ENGINE_ID";
	private static final String TARGET_INTERCEPTOR = "TARGET_INTERCEPTOR";
	private static final String TARGET_PARAMETERS = "TARGET_PARAMETERS";
	
	private String targetEngineId;
	private String targetInterceptorType;
	private MethodInterceptor targetInterceptor;
	private Object[] targetParameters;
	private IVectorDatabaseEngine proxy;
	
	/*
	TARGET_ENGINE_ID	e16b5d90-1901-46a1-828a-e32e1ce244e2
	TARGET_INTERCEPTOR	prerna.engine.impl.vector.facade.DocumentSubsetInterceptor
	TARGET_INTERCEPTOR_ARGS	["tomcat_performance_tuning.pdf"]
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);

		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if(secretStore != null) {
			Map<String, Object> engineSecrets = secretStore.getEngineSecrets(getCatalogType(), this.engineId, this.engineName);
			if(engineSecrets == null || engineSecrets.isEmpty()) {
				classLogger.info("No secrets found for " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
			} else {
				classLogger.info("Successfully pulled secrets for " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
				this.smssProp.putAll(engineSecrets);
			}
		}
		
		this.targetEngineId = this.smssProp.getProperty(TARGET_ENGINE_ID);
		if (targetEngineId == null || (targetEngineId=targetEngineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the target engine id for this vector database using " + TARGET_ENGINE_ID);
		}
		
		IVectorDatabaseEngine targetEngine = Utility.getVectorDatabase(targetEngineId);
		if (targetEngine == null) {
			throw new IllegalArgumentException("Could not find the defined target engine id for this vector database with value = " + this.targetEngineId);
		}
		
		targetInterceptorType = this.smssProp.getProperty(TARGET_INTERCEPTOR);
		if(targetInterceptorType == null || (targetInterceptorType=targetInterceptorType.trim()).isEmpty()) {
			throw new IllegalArgumentException("TARGET_INTERCEPTOR property is required");
		}
		
		String targetParametersStr = this.smssProp.getProperty(TARGET_PARAMETERS);
		if(targetParametersStr != null && !(targetParametersStr=targetParametersStr.trim()).isEmpty()) {
			this.targetParameters = new Gson().fromJson(targetParametersStr, Object[].class);
		}
		
		try {
			targetInterceptor = AbstractInterceptor.buildInterceptor(targetInterceptorType, this, targetEngine, targetParameters);
		} catch(ClassNotFoundException | ClassCastException e) {
			throw new IllegalArgumentException("Unable to create interceptor for proxy vector database engine", e);
		}
		
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(targetEngine.getClass());
        enhancer.setInterfaces(new Class[] {IVectorDatabaseEngine.class});
        enhancer.setCallback(targetInterceptor);
        
        try {
        	proxy = (IVectorDatabaseEngine) enhancer.create();
        } catch(Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
        	throw new IllegalArgumentException("Unable to create proxy for vector database", e);
        }
	}
	@Override
	protected void verifyModelProps() {
		this.modelPropsLoaded = true;
	}
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PROXY;
	}
	
	// call the proxy for the rest. let the interceptor/s decide how to handle
	@Override
	public List<FileEmbeddingStatus> addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception {
		return proxy.addDocument(filePaths, parameters);
	}
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
		proxy.removeDocument(fileNames, parameters);
	}
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight, Map<String, Object> parameters)
			throws Exception {
		return proxy.addEmbeddings(vectorCsvFiles, insight, parameters);
	}
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFilePath, Insight insight, Map<String, Object> parameters)
			throws Exception {
		return proxy.addEmbeddings(vectorCsvFilePath, insight, parameters);
	}
	@Override
	public List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight, Map<String, Object> parameters)
			throws Exception {
		return proxy.addEmbeddingFiles(vectorCsvFiles, insight, parameters);
	}
	@Override
	public List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		return proxy.addEmbeddingFile(vectorCsvFile, insight, parameters);
	}
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters)
			throws Exception {
		return proxy.addEmbeddings(vectorCsvTable, insight, parameters);
	}
	@Override
	public void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider,
			String part, int tokens, String content, Map<String, Object> additionalMetadata) throws Exception {
		proxy.addEmbedding(embedding, source, modality, divider, part, tokens, content, additionalMetadata);
	}
	@Override
	public List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		return proxy.nearestNeighbor(insight, searchStatement, limit, parameters);
	}
	@Override
	protected List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		if(proxy instanceof AbstractVectorDatabaseEngine) {
			return ((AbstractVectorDatabaseEngine) proxy).nearestNeighborCall(insight, searchStatement, limit, parameters);
		} else {
			throw new NotImplementedException("Proxy type not compatible with this operation");
		}
	}
	@Override
	public void addMetadata(VectorDatabaseMetadataCSVTable metadataTable) {
		try {
			proxy.addMetadata(metadataTable);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e);
		}
	}
	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		return proxy.listDocuments(parameters);
	}
	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		return proxy.listAllRecords(parameters);
	}
	@Override
	public boolean userCanAccessEmbeddingModels(User user) {
		return proxy.userCanAccessEmbeddingModels(user);
	}
	@Override
	protected String getDefaultDistanceMethod() {
		if(proxy instanceof AbstractVectorDatabaseEngine) {
			return ((AbstractVectorDatabaseEngine) proxy).getDefaultDistanceMethod();
		} else {
			throw new NotImplementedException("Proxy type not compatible with this operation");
		}
	}
	@Override
	public String getIndexFilesPath(String indexClass) {
		return proxy.getIndexFilesPath(indexClass);
	}
	@Override
	public String getDocumentsFilesPath(String indexClass) {
		return proxy.getDocumentsFilesPath(indexClass);
	}
	
}
