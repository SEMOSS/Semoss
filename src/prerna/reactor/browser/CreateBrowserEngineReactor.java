package prerna.reactor.browser;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
import prerna.engine.api.IBrowserEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.browser.BrowserEngine;
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

public class CreateBrowserEngineReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(CreateBrowserEngineReactor.class);
	
	private String browserFile = null;

	public CreateBrowserEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.FILE_NAME.getKey()};
		this.keyRequired = new int [] {1, 1};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a browser engine", PixelDataType.CONST_STRING,
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

		if (AbstractSecurityUtils.adminOnlyBrowserAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		
		String browserName = getBrowserName();
		//if browser name is not valid, throw error
		if (!Utility.validateName(browserName)) {
			//error and redirect to try again
			throw new IllegalArgumentException("Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		String browserId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IBrowserEngine browser = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.BROWSER, user, browserName, browserId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.BROWSER, browserId, browserName);
			
			moveJsonToEngineFolder(specificEngineFolder);
			
			browser = new BrowserEngine();
			String browserClass = BrowserEngine.class.getName();
			Map<String, Object> browserDetails = new HashMap<>();
			browserDetails.put(Constants.BROWSER_FILE, browserFile);
			tempSmss = UploadUtilities.createTemporaryBrowserSmss(browserId, browserName, browserClass, browserDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(browserId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			browser.open(tempSmss.getAbsolutePath());			
			
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			browser.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(browserId, browserName, browser, smssFile);
			SecurityEngineUtils.addEngine(browserId, false, user);
			
			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(browserId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.pushEngine(browserId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(browser, browserId, tempSmss, smssFile, specificEngineFolder);
			return new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}
		
		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), browserId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError(IBrowserEngine browser, String storageId, File tempSmss, File smssFile, File specificEngineFolder) {
		try {
			// close the browser so we can delete it
			if (browser != null) {
				browser.close();
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
			
			UploadUtilities.removeEngineFromDIHelper(storageId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String getBrowserName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.NAME.getKey());
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
		
		throw new NullPointerException("Must define the name of the new browser engine");
	}
	
	private void moveJsonToEngineFolder(File specificEngineFolder) throws IOException {
		String insightFolder = this.insight.getInsightFolder();
	
		// see if added as key
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FILE_NAME.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				browserFile = grs.get(i).toString();
				File file = new File(insightFolder + File.separator + browserFile);
				if (file.exists()) {
					FileUtils.moveFileToDirectory(file, specificEngineFolder, false);
				} else {
					throw new IllegalArgumentException("No Browser file was found");
				}
			}
		}
	}
}
