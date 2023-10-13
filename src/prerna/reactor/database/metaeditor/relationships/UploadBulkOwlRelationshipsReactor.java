package prerna.reactor.database.metaeditor.relationships;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.impl.util.Owler;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class UploadBulkOwlRelationshipsReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadBulkOwlRelationshipsReactor.class);

	private static final String CLASS_NAME = UploadBulkOwlRelationshipsReactor.class.getName();
	private static final String SYNC_WITH_LOCALMASTER = "sync";

	static final String START_TABLE = "START_TABLE";
	static final String START_COLUMN = "START_COLUMN";
	static final String TARGET_TABLE = "TARGET_TABLE";
	static final String TARGET_COLUMN = "TARGET_COLUMN";
	
	
	/*
	 * This class assumes that the start table, start column, end table, and end column have already been defined
	 */
	
	private Logger logger = null;
	
	public UploadBulkOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), SYNC_WITH_LOCALMASTER};
	}
	
	@Override
	public NounMetadata execute() {
		logger = getLogger(CLASS_NAME);

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		databaseId = testDatabaseId(databaseId, true);
		boolean sync = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File uploadFile = new File(filePath);
		if(!uploadFile.exists() || !uploadFile.isFile()) {
			throw new IllegalArgumentException("Could not find the specified file");
		}
		
		ClusterUtil.pullOwl(databaseId);
		
		Owler owler = getOWLER(databaseId);
		// set all the existing values into the OWLER
		// so that its state is updated
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		setOwlerValues(database, owler);
		
		long start = System.currentTimeMillis();
		ExcelSheetFileIterator it = null;
		try {
			it = getExcelIterator(filePath);
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
		
		String[] excelHeaders = it.getHeaders();
		List<String> excelHeadersList = Arrays.asList(excelHeaders);

		int idxStartT = excelHeadersList.indexOf(START_TABLE);
		int idxStartC = excelHeadersList.indexOf(START_COLUMN);
		int idxTargetT = excelHeadersList.indexOf(TARGET_TABLE);
		int idxTargetC = excelHeadersList.indexOf(TARGET_COLUMN);

		if(idxStartT < 0 
				|| idxStartC < 0
				|| idxTargetT < 0
				|| idxTargetC < 0
				) {
			throw new IllegalArgumentException("One or more headers are missing from the excel");
		}
		
		int counter = 0;
		logger.info("Retrieving values to insert");
		try {
			while(it.hasNext()) {
				if(counter % 100 == 0) {
					logger.info("Adding relationship : #" + (counter+1));
				}
				IHeadersDataRow row = it.next();
				Object[] values = row.getValues();
				
				String startT = values[idxStartT].toString();
				String startC = values[idxStartC].toString();
				String endT = values[idxTargetT].toString();
				String endC = values[idxTargetC].toString();
				
				// generate the relationship
				String rel = startT + "." + startC + "." + endT + "." + endC;
				
				// add the relationship
				owler.addRelation(startT, endT, rel);
				counter++;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// commit the changes
		owler.commit();
		try {
			owler.export();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to add the relationships", 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);
		
		if(sync) {
			logger.info("Starting to remove exisitng metadata");
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(databaseId);
			logger.info("Finished removing exisitng metadata");

			logger.info("Starting to add metadata");
			String smssFile = (String) DIHelper.getInstance().getEngineProperty(databaseId + "_" + Constants.STORE);
			Properties prop = Utility.loadProperties(smssFile);
			AddToMasterDB adder = new AddToMasterDB();
			adder.registerEngineLocal(prop);
			logger.info("Done adding new metadata");

			logger.info("Synchronization complete");
		}
		
		long end = System.currentTimeMillis();
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Time to finish = " + (end - start) + "ms", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////


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
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SYNC_WITH_LOCALMASTER)) {
			return "Synchronize the OWL changes with the local master database";
		}
		return super.getDescriptionForKey(key);
	}

}
