package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.upload.UploadInputUtility;

public class AdminUploadDatabasePermissionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminUploadDatabasePermissionsReactor.class);

	private static final String CLASS_NAME = AdminUploadDatabasePermissionsReactor.class.getName();

	static final String ENGINE_ID_KEY = "ENGINEID";
	static final String USER_ID_KEY = "USERID";
	static final String PERMISSION_KEY = "PERMISSION";

	private static String insertQuery = null;
	private static Map<String, Integer> psIndex = new HashMap<>();
	static {
		String[] headers = new String[] {ENGINE_ID_KEY, USER_ID_KEY, PERMISSION_KEY};
		StringBuilder builder = new StringBuilder("INSERT INTO ENGINEPERMISSION (");
		for(int i = 0; i < headers.length; i++) {
			if(i > 0) {
				builder.append(", ");
			}
			builder.append(headers[i]);
		}
		builder.append(") VALUES (");
		for(int i = 0; i < headers.length; i++) {
			if(i > 0) {
				builder.append(", ");
			}
			builder.append("?");

			// also keep track of header to index for the file uploading
			psIndex.put(headers[i], (i+1));
		}
		insertQuery = builder.append(")").toString();
	}

	private Logger logger = null;

	public AdminUploadDatabasePermissionsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File uploadFile = new File(Utility.normalizePath(filePath));
		if(!uploadFile.exists() || !uploadFile.isFile()) {
			throw new IllegalArgumentException("Could not find the specified file");
		}

		this.logger = getLogger(CLASS_NAME);

		RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
		Connection conn = null;
		try {
			conn = database.getConnection();
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}

		long start = System.currentTimeMillis();
		{
			ExcelSheetFileIterator it = null;
			try {
				it = getExcelIterator(filePath);
				loadExcelFile(conn, database.getQueryUtil(), it);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error loading admin users : " + e.getMessage());
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		try {
			conn.commit();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		long end = System.currentTimeMillis();
		return new NounMetadata("Time to finish = " + (end - start) + "ms", PixelDataType.CONST_STRING);
	}

	private ExcelSheetFileIterator getExcelIterator(String fileLocation) {
		// get range
		ExcelWorkbookFilePreProcessor processor = new ExcelWorkbookFilePreProcessor();
		processor.parse(fileLocation);
		processor.determineTableRanges();
		Map<String, ExcelSheetPreProcessor> sheetProcessors = processor.getSheetProcessors();
		// get sheetName and headers
		String sheetName = processor.getSheetNames().get(0);
		String range = null;
		ExcelSheetPreProcessor sProcessor = sheetProcessors.get(sheetName);
		{
			List<ExcelBlock> blocks = sProcessor.getAllBlocks();
			// for(int i = 0; i < blocks.size(); i++) {
			ExcelBlock block = blocks.get(0);
			List<ExcelRange> blockRanges = block.getRanges();
			for (int j = 0; j < 1; j++) {
				ExcelRange r = blockRanges.get(j);
				logger.info("Found range = " + r.getRangeSyntax());
				range = r.getRangeSyntax();
			}
		}
		processor.clear();

		ExcelQueryStruct qs = new ExcelQueryStruct();
		qs.setSheetName(sheetName);
		qs.setSheetRange(range);
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(fileLocation);
		ExcelSheetFileIterator it = helper.getSheetIterator(qs);

		return it;
	}

	private void loadExcelFile(Connection conn, 
			AbstractSqlQueryUtil queryUtil, 
			ExcelSheetFileIterator helper) throws Exception {

		boolean hasInsert = false;
		boolean hasUpdate = false;
		PreparedStatement insertPs = conn.prepareStatement(insertQuery);
		try {
			String[] excelHeaders = helper.getHeaders();
			List<String> excelHeadersList = Arrays.asList(excelHeaders);

			int idxEngine = excelHeadersList.indexOf(ENGINE_ID_KEY);
			int idxUser = excelHeadersList.indexOf(USER_ID_KEY);
			int idxRole = excelHeadersList.indexOf(PERMISSION_KEY);

			if(idxEngine < 0 
					|| idxUser < 0
					|| idxRole < 0
					) {
				throw new IllegalArgumentException("One or more headers are missing from the excel");
			}

			int counter = 0;
			Object[] row = null;
			while ((helper.hasNext())) {
				row = helper.next().getRawValues();

				String engineId = (String) row[idxEngine];
				String userId = (String) row[idxUser];
				String role = (String) row[idxRole];

				if(engineId == null || engineId.isEmpty()) {
					throw new IllegalArgumentException("Must have the engine id for the user defined - check row " + counter);
				}
				if(userId == null || userId.isEmpty()) {
					throw new IllegalArgumentException("Must have the user id for the user defined - check row " + counter);
				}
				if(role == null || role.isEmpty()) {
					throw new IllegalArgumentException("Must have the role for the user defined - check row " + counter);
				}

				AccessPermissionEnum permission = AccessPermissionEnum.valueOf(role);
				if(permission == null) {
					throw new IllegalArgumentException("Must have a valid permission role - check row " + counter);
				}


				// check if the ID already exists
				if(SecurityEngineUtils.checkUserHasAccessToDatabase(engineId, userId)) {
					//TODO: update based on user id instead of continue?
					logger.info("User id = " + userId + " alraedy exists for app = " + engineId + " - skipping record for upload");
					continue;
				} else {
					hasInsert = true;
					// add to insert ps
					insertPs.setString(psIndex.get(ENGINE_ID_KEY), engineId);
					insertPs.setString(psIndex.get(USER_ID_KEY), userId);
					insertPs.setInt(psIndex.get(PERMISSION_KEY), permission.getId());

					insertPs.addBatch();
				}

				counter++;
			}
			// we execute for insert and updates
			if(hasInsert) {
				insertPs.executeBatch();
			}
			logger.info("Done with item type updates , total rows = " + counter);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);	
		} finally {
			if(insertPs!=null) {
				insertPs.close();
			}
		}
	}
}
