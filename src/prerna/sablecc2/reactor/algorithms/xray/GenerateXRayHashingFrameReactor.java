package prerna.sablecc2.reactor.algorithms.xray;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateXRayHashingFrameReactor extends AbstractRFrameReactor {

	public static final String CLASS_NAME = GenerateXRayHashingFrameReactor.class.getName();
	public static final Logger classLogger = LogManager.getLogger(CLASS_NAME);

	public static final String FILES_KEY = "files";
	public static final String STATUS_KEY = "status";
	public static final String FRAME_NAMES_KEY = "frameNames";

	public static final String ROW_MATCHING = "rowComparison";

	private String folderPath;
	private Map<String, Object> configMap;
	
	public GenerateXRayHashingFrameReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), 
				ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.OVERRIDE.getKey(), ReactorKeysEnum.CONFIG.getKey(),
				GenerateXRayMatchingReactor.ROW_MATCHING};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		String[] reqPackages = new String[] {"data.table", "textreuse"};
		this.rJavaTranslator.checkPackages(reqPackages);

		organizeKeys();
		
		// check if user is logged in
		String space = this.keyValue.get(this.keysToGet[1]);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String baseFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		File baseF = new File(baseFolder);
		if(!baseF.exists() || !baseF.isDirectory()) {
			baseF.mkdirs();
		}

		// specify the folder from the base
		String folderName = keyValue.get(keysToGet[0]);
		if(folderName == null || folderName.isEmpty()) {
			folderName = "xray_corpus";
		}
		this.folderPath = (baseFolder + "/" + folderName).replace('\\', '/');
		File xrayFolder = new File(this.folderPath);
		if(!xrayFolder.exists() || !xrayFolder.isDirectory()) {
			xrayFolder.mkdirs();
		}
		
		// see if we are overriding the existing hash or not
		boolean override = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.OVERRIDE.getKey()) + "");
		boolean rowComparison = Boolean.parseBoolean(this.keyValue.get(GenerateXRayMatchingReactor.ROW_MATCHING) + "");

		List<Object> frames = getFrames();

		// get the config map
		this.configMap = getConfig();
		
		// source the packages and R script
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.loadPackages(reqPackages) + "; source(\"" 
				+ DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace('\\', '/') 
				+ "/R/XRay/encode_instances.R\", local=TRUE);");
		
		Map<String, Object> returnMap = new HashMap<>();
		returnMap.put(this.keysToGet[0], folderName);
		returnMap.put(this.keysToGet[1], space);
		List<String> fileNames = new ArrayList<>();
		List<String> status = new ArrayList<>();
		List<String> frameNames = new ArrayList<>();
		returnMap.put(FILES_KEY, fileNames);
		returnMap.put(STATUS_KEY, status);
		returnMap.put(FRAME_NAMES_KEY, frameNames);

		// go through and write the database
		for(Object frameObj : frames) {
			ITableDataFrame frame = (ITableDataFrame) frameObj;
			frameNames.add(frame.getName());
			
			List<String> selectorFilters = null;
			if(this.configMap != null && this.configMap.containsKey(frame)) {
				selectorFilters = (List<String>) this.configMap.get(frame);
			}
			
			List<String> allSelectors = frame.getMetaData().getFrameSelectors();
			if(selectorFilters != null && !selectorFilters.isEmpty()) {
				allSelectors.retainAll(selectorFilters);
			}
			
			if(rowComparison) {
				row(allSelectors, override, frame, status, fileNames);
			} else {
				nonRow(allSelectors, override, frame, status, fileNames);
			}
		}
		
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.MAP);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully stored hash files for databases"));
		return noun;
	}
		
	/**
	 * 
	 * @param selectors
	 * @param override
	 * @param frame
	 * @param status
	 * @param fileNames
	 */
	private void row(List<String> selectors, boolean override, ITableDataFrame frame, List<String> status, List<String> fileNames) {
		// need to group each table into a single query
		Map<String, SelectQueryStruct> tableToQs = new HashMap<>();
		for(String selector : selectors) {
			// see if the file already exists
			// so if we are not overriding, we can skip this selector
			String tableName = null;
			if(selector.contains("__")) {
				String[] split = selector.split("__");
				tableName = split[0];
			} else {
				tableName = selector;
			}
			
			SelectQueryStruct qs = null;
			if(tableToQs.containsKey(tableName)) {
				qs = tableToQs.get(tableName);
			} else {
				qs = new SelectQueryStruct();
				tableToQs.put(tableName, qs);
			}
			qs.addSelector(new QueryColumnSelector(selector));
		}
		
		// now loop through all the QS we have aggregated and write the tables out
		for(String table : tableToQs.keySet()) {
			SelectQueryStruct qs = tableToQs.get(table);
			String outputFileName = frame.getName() + ";" + table + ";row_comparison.tsv";
			
			String outputFile = this.folderPath + "/" + Utility.normalizePath(outputFileName);
			if(!override && new File(outputFile).exists()) {
				classLogger.info("Hash already exists for " + Utility.cleanLogString(table));
				
				// add to list of files used
				fileNames.add(outputFileName);
				status.add("existing");
				continue;
			}
			// add to list of files used
			fileNames.add(outputFileName);
			status.add("new");

			IRawSelectWrapper wrapper = null;
			try {
				qs.mergeImplicitFilters(frame.getFrameFilters());
				wrapper = frame.query(qs); 
				File f = new File(this.folderPath + "/" + Utility.normalizePath(table) + "_base.tsv");
				try {
					// write file
					// no separator so its all concat together
					Utility.writeResultToFile(f.getAbsolutePath(), wrapper, "");
					// read into R
					String randomFrame = Utility.getRandomString(6);
					this.rJavaTranslator.executeEmptyR(RSyntaxHelper.getFReadSyntax(randomFrame, f.getAbsolutePath(), "\t"));
					// run the script which also outputs the file
					// we care about the file name since we use that to split to know the source
					classLogger.info("Generating hash for " + Utility.cleanLogString(table));
					this.rJavaTranslator.executeEmptyR("encode_instances(" + randomFrame + ", \"" + outputFile + "\")");
					classLogger.info("Done generating hash");
				} finally {
					if(f.exists()) {
						f.delete();
					}
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param selectors
	 * @param override
	 * @param frame
	 * @param status
	 * @param fileNames
	 */
	private void nonRow(List<String> selectors, boolean override, ITableDataFrame frame, List<String> status, List<String> fileNames) {
		for(String selector : selectors) {
			// see if the file already exists
			// so if we are not overriding, we can skip this selector
			String outputFileName = frame.getName() + ";";
			if(selector.contains("__")) {
				String[] split = selector.split("__");
				outputFileName += split[0] + ";" + split[1];
			} else {
				outputFileName += selector + ";default_node_value";
			}
			outputFileName += ".tsv";
			String outputFile = this.folderPath + "/" + Utility.normalizePath(outputFileName);
			if(!override && new File(outputFile).exists()) {
				classLogger.info("Hash already exists for " + Utility.cleanLogString(selector));
				
				// add to list of files used
				fileNames.add(outputFileName);
				status.add("existing");
				continue;
			}
			
			// add to list of files used
			fileNames.add(outputFileName);
			status.add("new");
			
			classLogger.info("Querying data for " + Utility.cleanLogString(selector));
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.mergeImplicitFilters(frame.getFrameFilters());
			qs.addSelector(new QueryColumnSelector(selector));
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = frame.query(qs);
				File f = new File(this.folderPath + "/" + Utility.normalizePath(selector) + "_base.tsv");
				try {
					// write file
					Utility.writeResultToFile(f.getAbsolutePath(), wrapper, "/t");
					// read into R
					String randomFrame = Utility.getRandomString(6);
					this.rJavaTranslator.executeEmptyR(RSyntaxHelper.getFReadSyntax(randomFrame, f.getAbsolutePath(), "\t"));
					// run the script which also outputs the file
					// we care about the file name since we use that to split to know the source
					classLogger.info("Generating hash for " + Utility.cleanLogString(selector));
					this.rJavaTranslator.executeEmptyR("encode_instances(" + randomFrame + ", \"" + outputFile + "\")");
					classLogger.info("Done generating hash");
				} finally {
					if(f.exists()) {
						f.delete();
					}
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}	
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Grab values from store
	 */
	
	private List<Object> getFrames() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FRAME.getKey());
		return grs.getValuesOfType(PixelDataType.FRAME);
	}

	private Map<String, Object> getConfig() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONFIG.getKey());
		if(grs != null && !grs.isEmpty()) {
			NounMetadata value = grs.getNoun(0);
			if(value.getNounType() == PixelDataType.MAP) {
				return (Map<String, Object>) value.getValue();
			}
		}
		return null;
	}
	
	/*
	 * Getters for other reactors
	 */
	
	String getFolderPath() {
		return this.folderPath;
	}
	
	Map<String, Object> getConfigMap() {
		return this.configMap;
	}

}
