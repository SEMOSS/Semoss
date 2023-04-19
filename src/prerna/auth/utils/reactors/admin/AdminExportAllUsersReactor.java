package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.InsightFile;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.export.AbstractExportTxtReactor;
import prerna.sablecc2.reactor.export.ToExcelReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class AdminExportAllUsersReactor extends ToExcelReactor {

	private static final String CLASS_NAME = AdminExportAllUsersReactor.class.getName();

	public AdminExportAllUsersReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
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
		// must have a password for the file
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if(password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Must provide a password to encrypt the file");
		}
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__SALT"));

		RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		IRawSelectWrapper iterator = null;;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
			this.task = new BasicIteratorTask(qs, iterator);
			
			// get a random file name
			String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
			if(prefixName == null || prefixName.isEmpty()) {
				prefixName = "All_Users";
			}
			String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "xlsx");
			// grab file path to write the file
			this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
			// if the file location is not defined generate a random path and set
			// location so that the front end will download
			if (this.fileLocation == null) {
				String insightFolder = this.insight.getInsightFolder();
				{
					File f = new File(Utility.normalizePath(insightFolder));
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
			e.printStackTrace();
			throw new IllegalArgumentException("An error occurred retrieving the users. Message is : " + e.getMessage());
		} finally {
			if(iterator != null) {
				iterator.cleanUp();
			}
		}
	}

}
