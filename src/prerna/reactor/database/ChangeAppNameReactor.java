package prerna.reactor.database;
//package prerna.sablecc2.reactor.app;
//
//import java.io.File;
//import java.io.IOException;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.logging.log4j.Logger;
//
//import prerna.auth.User;
//import prerna.auth.utils.AbstractSecurityUtils;
//import prerna.auth.utils.SecurityAdminUtils;
//import prerna.auth.utils.SecurityAppUtils;
//import prerna.auth.utils.SecurityQueryUtils;
//import prerna.cluster.util.ClusterUtil;
//import prerna.engine.api.IEngine;
//import prerna.engine.impl.SmssUtilities;
//import prerna.engine.impl.rdbms.RDBMSNativeEngine;
//import prerna.nameserver.AddToMasterDB;
//import prerna.nameserver.utility.MasterDatabaseUtility;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.app.upload.UploadUtilities;
//import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
//public class ChangeAppNameReactor extends AbstractInsightReactor {
//	private static final String CLASS_NAME = ChangeAppNameReactor.class.getName();
//
//	public ChangeAppNameReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.NAME.getKey() };
//	}
//
//	@Override
//	public NounMetadata execute() {
//		organizeKeys();
//		Logger logger = this.getLogger(CLASS_NAME);
//		String appId = this.keyValue.get(this.keysToGet[0]);
//		String newAppName = this.keyValue.get(this.keysToGet[1]);
//		User user = this.insight.getUser();
//		boolean security = AbstractSecurityUtils.securityEnabled();
//		// we may have the alias
//		if (security) {
//			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
//			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
//			if (!isAdmin) {
//				boolean isOwner = SecurityAppUtils.userIsOwner(user, appId);
//				if (!isOwner) {
//					throw new IllegalArgumentException("User must be the owner to perform this function.");
//				}
//			}
//
//		} else {
//			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
//			if (!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
//				throw new IllegalArgumentException("App " + appId + " does not exist");
//			}
//		}
//		String smssDbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db";
//		IEngine engine = Utility.getEngine(appId);
//		String oldAppName = engine.getEngineName();
//		
//		// smss paths
//		String oldSmssFile = smssDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(oldAppName, appId)
//				+ Constants.SEMOSS_EXTENSION;
//		String newSmssFile = smssDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(newAppName, appId)
//				+ Constants.SEMOSS_EXTENSION;
//
//		// app folder paths
//		String oldAppFolderPath = smssDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(oldAppName, appId);
//		String newAppFolderPath = smssDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(newAppName, appId);
//		File oldAppFolder = new File(oldAppFolderPath);
//		File newAppFolder = new File(newAppFolderPath);
//		File oldOwlFile = new File(engine.getOWL());
//
//		try {
//			// create new smss
//			SmssUtilities.changeAppName(oldSmssFile, newSmssFile, newAppName);
//			// create new app folder
//			engine.close();
//			FileUtils.copyDirectory(oldAppFolder, newAppFolder);
//			// create new owl
//			File newOwlFile = SmssUtilities.getOwlFile(Utility.loadProperties(newSmssFile));
//			FileUtils.copyFile(oldOwlFile, newOwlFile);
//
//			// update engine with new paths
//			DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, newSmssFile);
//			// do I need to do all this?
//
//			engine.setEngineName(newAppName);
//			engine.setOWL(newOwlFile.getAbsolutePath());
//			engine.setPropFile(newSmssFile);
//			engine.open(newSmssFile);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			// need to delete new files
//			try {
//				FileUtils.forceDelete(newAppFolder);
//				new File(newSmssFile).delete();
//			} catch (IOException e1) {
//				logger.info("Unable to delete app rename files");
//			}
//
//			// need to reset old app
//			try {
//				DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, oldSmssFile);
//				engine.setEngineName(oldAppName);
//				engine.setOWL(oldOwlFile.getAbsolutePath());
//				engine.setPropFile(oldSmssFile);
//				engine.open(oldSmssFile);
//			} catch (Exception e1) {
//				e1.printStackTrace();
//			}
//
//			throw new IllegalArgumentException("Unable to rename app");
//		}
//
//		// update name in local master
//		AddToMasterDB adder = new AddToMasterDB();
//		adder.setAppName(appId, newAppName);
//		if (security) {
//			// update name in security
//			SecurityAppUtils.setAppName(user, appId, newAppName);
//		}
//
//		// now delete old stuff
//		try {
//			FileUtils.forceDelete(oldAppFolder);
//			new File(oldSmssFile).delete();
//		} catch (IOException e1) {
//			logger.info("Unable to delete " + oldAppName + "files");
//		}
//
//		DIHelper.getInstance().setLocalProperty(appId, engine);
//		ClusterUtil.reactorPushApp(appId);
//		NounMetadata ret = new NounMetadata(newAppName, PixelDataType.CONST_STRING);
//		ret.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfuly changed app name!"));
//		return ret;
//	}
//}
