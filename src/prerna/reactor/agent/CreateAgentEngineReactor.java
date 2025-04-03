package prerna.reactor.agent;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.agent.AgentEngine;
import prerna.engine.api.IAgentEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateAgentEngineReactor extends AbstractReactor {
    private static final Logger classLogger = LogManager.getLogger(CreateAgentEngineReactor.class);
	
	public CreateAgentEngineReactor() {
		this.keysToGet = new String[] {"agent", "agent_details", ReactorKeysEnum.GLOBAL.getKey()};
		this.keyRequired = new int[] {1, 1};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create an agent engine", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		organizeKeys();
		
		String agentName = getAgentName();
		//if agent db name is not valid throw error
		if (!Utility.validateName(agentName)) {
			//error and redirect to try again
			throw new IllegalArgumentException("Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}
		
		Map<String, Object> agentDetails = getAgentDetails();
				
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
		
		String agentId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IAgentEngine agent = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.AGENT, user, agentName, agentId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.AGENT, agentId, agentName);

            agent = new AgentEngine();
			tempSmss = UploadUtilities.createTemporaryEngineSmss(IEngine.CATALOG_TYPE.AGENT, agentId, agentName, AgentEngine.class.getName(), agentDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(agentId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			agent.open(tempSmss.getAbsolutePath());			
			
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			agent.setSmssFilePath(smssFile.getAbsolutePath());

			UploadUtilities.updateDIHelper(agentId, agentName, agent, smssFile);
			SecurityEngineUtils.addEngine(agentId, global, user);
			
			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(agentId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.pushEngine(agentId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(agent, agentId, tempSmss, smssFile, specificEngineFolder);
			throw new IllegalArgumentException("Failed to create agent engine. Error: " + e.getMessage());
		}
		
		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), agentId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError(IAgentEngine agentEngine, String modelId, File tempSmss, File smssFile, File specificEngineFolder) {
		try {
			// close the DB so we can delete it
			if (agentEngine != null) {
				agentEngine.close();
			}
			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			// delete the engine folder
			if (specificEngineFolder != null && specificEngineFolder.exists()) {
				FileUtils.forceDelete(specificEngineFolder);
			}
			
			UploadUtilities.removeEngineFromDIHelper(modelId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String getAgentName() {
		GenRowStruct grs = this.store.getNoun("agent");
		if(grs != null && !grs.isEmpty()) {
			List<String> strValues = grs.getAllStrValues();
			if(strValues != null && !strValues.isEmpty()) {
				return strValues.get(0).trim();
			}
		}
		
		List<String> strValues = this.curRow.getAllStrValues();
		if(strValues != null && !strValues.isEmpty()) {
			return strValues.get(0).trim();
		}
		
		throw new NullPointerException("Must define the name of the new agent engine");
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getAgentDetails() {
		GenRowStruct grs = this.store.getNoun("agent_details");
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}
		
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}
		
		throw new NullPointerException("Must define the properties for the new agent engine");
	}
}
