package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.InsightFile;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.export.AbstractExportTxtReactor;
import prerna.reactor.export.ToExcelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Constants;
import prerna.util.Utility;

public class AdminExportUserDatabasePermissionsReactor extends ToExcelReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminExportUserDatabasePermissionsReactor.class);

	private static final String CLASS_NAME = AdminExportUserDatabasePermissionsReactor.class.getName();

	public AdminExportUserDatabasePermissionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), 
				ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		this.includeLogo = false;
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		String engineid = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
		if(password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Must provide a password to encrypt the file");
		}
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID", "USER_ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "LOGIN_METHOD"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "ROLE"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "DATABASE_ID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "DATABASE_NAME"));
		
		qs.addRelation("ENGINEPERMISSION__USERID", "SMSS_USER__ID", "left.join");
		qs.addRelation("ENGINEPERMISSION__PERMISSION", "PERMISSION__ID", "left.join");
		qs.addRelation("ENGINE__ENGINEID", "ENGINEPERMISSION__ENGINEID", "left.join");
		
		if(engineid != null && !engineid.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID","==", engineid));
		}
		
		RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		IRawSelectWrapper iterator = null;;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
			this.task = new BasicIteratorTask(qs, iterator);
			
			// get a random file name
			String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
			if(prefixName == null || prefixName.isEmpty()) {
				prefixName = "Database_Users";
			}
			String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "xlsx");
			// grab file path to write the file
			this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
			// if the file location is not defined generate a random path and set
			// location so that the front end will download
			if (this.fileLocation == null) {
				String insightFolder = this.insight.getInsightFolder();
				{
					File f = new File(insightFolder);
					if(!f.exists()) {
						f.mkdirs();
					}
				}
				this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			}
			
			// store the insight file 
			// in the insight so the FE can download it
			// only from the given insight
			String downloadKey = UUID.randomUUID().toString();
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(this.fileLocation);
			insightFile.setDeleteOnInsightClose(true);
			insightFile.setFileKey(downloadKey);
			this.insight.addExportFile(downloadKey, insightFile);
			NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
			
			buildTask();
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
			return retNoun;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred retrieving the users. Message is : " + e.getMessage());
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
