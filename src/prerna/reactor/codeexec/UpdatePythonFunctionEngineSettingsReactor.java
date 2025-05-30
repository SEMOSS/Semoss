package prerna.reactor.codeexec;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UpdatePythonFunctionEngineSettingsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(UpdatePythonFunctionEngineSettingsReactor.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

    public UpdatePythonFunctionEngineSettingsReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.ENGINE.getKey(),
            ReactorKeysEnum.FUNCTION_DETAILS.getKey(),
           
        };
    }

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a function engine", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// throw error is user doesn't have rights to publish new databases
		if (SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		 String engineId = this.keyValue.get(this.keysToGet[0]);
		 Map<String, Object> functionDetails = getFunctionDetails();
			if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
			}
			String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
			Properties prop = Utility.loadProperties(smssFile);

			//Update required fields in SMSS
			updateProperty(prop, "FUNCTION_NAME", functionDetails.get("FUNCTION_NAME"));
			updateProperty(prop, "FUNCTION_DESCRIPTION", functionDetails.get("FUNCTION_DESCRIPTION"));
			updateProperty(prop, "PYTHON_FILE_NAME", functionDetails.get("PYTHON_FILE_NAME"));

			//Handle FUNCTION_REQUIRED_PARAMETERS (List<String>)
			if (functionDetails.containsKey("FUNCTION_REQUIRED_PARAMETERS")) {
				List<String> requiredParams = objectMapper.convertValue(
					functionDetails.get("FUNCTION_REQUIRED_PARAMETERS"),
					new TypeReference<List<String>>() {});
				String jsonRequiredParams = null;
				try {
					jsonRequiredParams = objectMapper.writeValueAsString(requiredParams);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				updateProperty(prop, "FUNCTION_REQUIRED_PARAMETERS", jsonRequiredParams);
			}

			//Handle FUNCTION_PARAMETERS (List<Map<String, String>>)
			if (functionDetails.containsKey("FUNCTION_PARAMETERS")) {
				String jsonParams = null;
				try {
					jsonParams = objectMapper.writeValueAsString(functionDetails.get("FUNCTION_PARAMETERS"));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				updateProperty(prop, "FUNCTION_PARAMETERS", jsonParams);
			}
			try {
				final String tab = "\t";
			Path smssPath = Paths.get(smssFile);
			List<String> originalLines = Files.readAllLines(smssPath, StandardCharsets.UTF_8);
			List<String> updatedLines = new ArrayList<>();

			// Track which keys were updated (so we avoid re-appending them)
			Set<String> updatedKeys = new HashSet<>();

			for (String line : originalLines) {
			    if (line.trim().isEmpty() || line.trim().startsWith("#")) {
			        updatedLines.add(line); // preserve empty or comment lines
			        continue;
			    }

			    String[] parts = line.split(tab, 2); // split by TAB
			    if (parts.length != 2) {
			        updatedLines.add(line);
			        continue;
			    }

			    String key = parts[0].trim();

			    if (prop.containsKey(key)) {
			        String newValue = prop.getProperty(key);
			        updatedLines.add(key + tab + newValue);
			        updatedKeys.add(key);
			    } else {
			        updatedLines.add(line); // keep the original line
			    }
			}

			// Write all lines back to file
			Files.write(smssPath, updatedLines, StandardCharsets.UTF_8);
			classLogger.info("Successfully updated function engine settings for engine: " + engineId);
			} catch (Exception e) {
				classLogger.error("Failed to write to SMSS file: " + smssFile, e);
				throw new RuntimeException("Unable to update engine settings.", e);
			}

			return new NounMetadata("Function Engine settings updated successfully", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
			
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getFunctionDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FUNCTION_DETAILS.getKey());
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
		
		throw new NullPointerException("Must define the properties for the new function engine");
	}
	
	/**
	 * Utility method to update a property only if the value is not null
	 */
	private void updateProperty(Properties prop, String key, Object value) {
		if (value != null) {
			prop.setProperty(key, value.toString());
		}
	}

}
