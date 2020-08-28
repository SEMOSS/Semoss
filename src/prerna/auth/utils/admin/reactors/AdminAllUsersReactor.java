package prerna.auth.utils.admin.reactors;

import java.io.File;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
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

public class AdminAllUsersReactor extends ToExcelReactor {

	private static final String CLASS_NAME = AdminAllUsersReactor.class.getName();

	public AdminAllUsersReactor() {
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
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("USER__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("USER__SALT"));

		RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		IRawSelectWrapper iterator = null;;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
			this.task = new BasicIteratorTask(qs, iterator);
			
			NounMetadata retNoun = null;
			// get a random file name
			String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
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
				// store it in the insight so the FE can download it
				// only from the given insight
				this.insight.addExportFile(exportName, this.fileLocation);
				retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
			} else {
				retNoun = new NounMetadata(this.fileLocation, PixelDataType.CONST_STRING);
			}
			buildTask();
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
			return retNoun;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured retrieving the users. Message is : " + e.getMessage());
		} finally {
			if(iterator != null) {
				iterator.cleanUp();
			}
		}
	}

}
