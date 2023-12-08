package prerna.reactor.venv.upload;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.api.VenvTypeEnum;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.upload.UploadUtilities;

public class CreateVenvEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateVenvEngineReactor.class);

	public CreateVenvEngineReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.VENV.getKey(), 
			ReactorKeysEnum.VENV_DETAILS.getKey(),
			ReactorKeysEnum.GLOBAL.getKey()
		};
		this.keyRequired = new int[] {1, 1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a venv engine", PixelDataType.CONST_STRING,
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
		if (AbstractSecurityUtils.adminSetPublisher()
				&& !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		
		String venvName = getVenvName();
		Map<String, String> venvDetails = getVenvDetails();
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");

		String venvTypeStr = venvDetails.get(IVenvEngine.VENV_TYPE);
		if(venvTypeStr == null || (venvTypeStr=venvTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the virtual environment type");
		}
		
		
		VenvTypeEnum venvType = null;
		try {
			venvType = VenvTypeEnum.getEnumFromName(venvTypeStr);
		} catch(Exception e) {
			throw new IllegalArgumentException("Invalid virtual environment type " + venvTypeStr);
		}
		
		String venvId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IVenvEngine venv = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.VENV, user, venvName, venvId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.VENV, venvId, venvName);
			
			String venvClass = venvType.getVenvClass();
			venv = (IVenvEngine) Class.forName(venvClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryVenvSmss(venvId, venvName, venvClass, venvDetails);
			
			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(venvId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			venv.open(tempSmss.getAbsolutePath());			
			
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			venv.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(venvId, venvName, venv, smssFile);
			SecurityEngineUtils.addEngine(venvId, global, user);
			
			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(venvId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.pushEngine(venvId);
			
			venv.pullRequirementsFile();
			venv.createVirtualEnv();
			
			if (ClusterUtil.IS_CLUSTER) {
				File [] engineSubFiles = specificEngineFolder.listFiles();
				
				String[] subFilesAbsolutePaths = Arrays.stream(engineSubFiles)
		                .map(File::getAbsolutePath)
		                .toArray(String[]::new);
				
				Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(venvId, venv.getCatalogType(), subFilesAbsolutePaths));
				copyFilesToCloudThread.start();
			}
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(venv, venvId, tempSmss, smssFile, specificEngineFolder);
		}
		
		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), venvId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError(IVenvEngine venv, String venvId, File tempSmss, File smssFile, File specificEngineFolder) {
		try {
			// close the DB so we can delete it
			if (venv != null) {
				venv.close();
			}

			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			if (specificEngineFolder != null && specificEngineFolder.exists()) {
				FileUtils.forceDelete(specificEngineFolder);
			}
			
			UploadUtilities.removeEngineFromDIHelper(venvId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String getVenvName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VENV.getKey());
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
		
		throw new NullPointerException("Must define the name of the new virtual environment engine");
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, String> getVenvDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VENV_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, String>) mapNouns.get(0).getValue();
			}
		}
		
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, String>) mapNouns.get(0).getValue();
		}
		
		throw new NullPointerException("Must define the properties for the new virtual environment engine");
	}

}
