package prerna.reactor.database.upload;
//package prerna.sablecc2.reactor.app.upload;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//import java.util.UUID;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.logging.log4j.Logger;
//
//import prerna.auth.AuthProvider;
//import prerna.auth.User;
//import prerna.auth.utils.AbstractSecurityUtils;
//import prerna.auth.utils.SecurityQueryUtils;
//import prerna.auth.utils.SecurityUpdateUtils;
//import prerna.cluster.util.ClusterUtil;
//import prerna.engine.impl.SmssUtilities;
//import prerna.engine.impl.app.AppEngine;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.execptions.SemossPixelException;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.AbstractReactor;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//
//public class GenerateEmptyAppReactor extends AbstractReactor {
//
//	private static final String CLASS_NAME = GenerateEmptyAppReactor.class.getName();
//
//	/*
//	 * This class is used to construct an empty app
//	 * This app contains no data (no data file or OWL)
//	 * This app only contains insights
//	 * The idea being that the insights are parameterized and can be applied to various data sources
//	 */
//
//	public GenerateEmptyAppReactor() {
//		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
//	}
//
//	@Override
//	public NounMetadata execute() {
//		Logger logger = getLogger(CLASS_NAME);
//		this.organizeKeys();
//		String appName = this.keyValue.get(this.keysToGet[0]);
//		if(appName == null || appName.isEmpty()) {
//			throw new IllegalArgumentException("Need to provide a name for the app");
//		}
//		
//		User user = null;
//		boolean security = AbstractSecurityUtils.securityEnabled();
//		if(security) {
//			user = this.insight.getUser();
//			if(user == null) {
//				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a database", PixelDataType.CONST_STRING, 
//						PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
//				SemossPixelException err = new SemossPixelException(noun);
//				err.setContinueThreadOfExecution(false);
//				throw err;
//			}
//			
//			// throw error if user is anonymous
//			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
//				throwAnonymousUserError();
//			}
//			
//			// throw error is user doesn't have rights to publish new apps
//			if(AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
//				throwUserNotPublisherError();
//			}
//		}
//		
//		String appId = UUID.randomUUID().toString();
//
//		// need to make sure we are not overriding something that already exists in the file system
//		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
//		// need to make sure app name doesn't already exist
//		String appLocation = baseFolder + DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId);
//		File appFolder = new File(appLocation);
//		if(appFolder.exists()) {
//			throw new IllegalArgumentException("Database folder already contains an app directory with the same name. Please delete the existing app folder or provide a unique app name");
//		}
//
//		logger.info("Done validating app");
//		logger.info("Starting app creation");
//
//		/*
//		 * Things we need to do
//		 * 1) make directory
//		 * 2) make insights database
//		 * 3) make special smss
//		 * 4) load into solr
//		 */
//
//		logger.info("Start generating app folder");
//		appFolder.mkdirs();
//		logger.info("Done generating app folder");
//
//
//		// add to DIHelper so we dont auto load with the file watcher
//		File tempSmss = null;
//		logger.info("Start generating temp smss");
//		try {
//			tempSmss = UploadUtilities.createTemporaryAppSmss(appId, appName, false);
//			DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
//		} catch (IOException e) {
//			e.printStackTrace();
//			throw new IllegalArgumentException(e.getMessage());
//		}
//		logger.info("Done generating temp smss");
//
//		logger.info("Add app security defaults");
//		SecurityUpdateUtils.addApp(appId, !AbstractSecurityUtils.securityEnabled());
//		logger.info("Done adding security defaults");
//
//		AppEngine appEng = new AppEngine();
//		appEng.setEngineId(appId);
//		appEng.setEngineName(appName);
//		// only at end do we add to DIHelper
//		DIHelper.getInstance().setLocalProperty(appId, appEng);
//		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
//		appNames = appNames + ";" + appId;
//		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);
//		
//		// and rename .temp to .smss
//		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
//		try {
//			FileUtils.copyFile(tempSmss, smssFile);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		tempSmss.delete();
//		
//		// update engine smss file location
//		appEng.setPropFile(smssFile.getAbsolutePath());
//		DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, smssFile.getAbsolutePath());
//		DIHelper.getInstance().setLocalProperty(appId, appEng);
//		
//		// even if no security, just add user as engine owner
//		if(user != null) {
//			List<AuthProvider> logins = user.getLogins();
//			for(AuthProvider ap : logins) {
//				SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
//			}
//		}
//		
//		// need to push this onto the cloud
//		ClusterUtil.reactorPushApp(appId);
//		
//		return new NounMetadata(appId, PixelDataType.CONST_STRING);
//	}
//}
