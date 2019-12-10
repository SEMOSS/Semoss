package prerna.sablecc2.reactor.algorithms.xray;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateXRayHashingReactor extends AbstractRFrameReactor {

	public static final String CLASS_NAME = GenerateXRayHashingReactor.class.getName();

	public static final String FILES_KEY = "files";
	public static final String STATUS_KEY = "status";
	
	private String folderPath;
	private List<String> appIds;
	private boolean override;
	private Map<String, List<String>> configMap;
	
	public GenerateXRayHashingReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), 
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.OVERRIDE.getKey(), ReactorKeysEnum.CONFIG.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// init 
		init();
		String[] reqPackages = new String[] {"data.table", "textreuse"};
		this.rJavaTranslator.checkPackages(reqPackages);

		// logger + orgnaize keys
		Logger logger = this.getLogger(CLASS_NAME);
		organizeKeys();
		
		// check if user is logged in
		String space = this.keyValue.get(this.keysToGet[1]);
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
		
		// now we want to go through and save all the file details 
		this.appIds = getApps();
		for(String appId : appIds) {
			if(AbstractSecurityUtils.securityEnabled()) {
				if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), appId)) {
					throw new IllegalArgumentException("User does not have permission to view this engine or engine does not exist");
				}
			}
		}
		
		// see if we are overriding the existing hash or not
		this.override = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]) + "");
		
		// get the config map
		this.configMap = getConfig();
		
		// source the packages and R script
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.loadPackages(reqPackages) + "; source(\"" 
				+ DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace('\\', '/') 
				+ "/R/XRay/encode_instances.R\", local=TRUE);");
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put(this.keysToGet[0], folderName);
		returnMap.put(this.keysToGet[1], space);
		List<String> fileNames = new Vector<String>();
		List<String> status = new Vector<String>();
		returnMap.put(FILES_KEY, fileNames);
		returnMap.put(STATUS_KEY, status);
		
		// go through and write the app
		for(String appId : appIds) {
			IEngine engine = Utility.getEngine(appId);
			Collection<String> pixelSelectors = MasterDatabaseUtility.getSelectorsWithinEngineRDBMS(appId);
			
			List<String> selectorFilters = null;
			if(this.configMap != null) {
				if(this.configMap.containsKey(appId)) {
					selectorFilters = this.configMap.get(appId);
				}
			}
			
			for(String selector : pixelSelectors) {
				// see if its part of the filters
				// but if the selectors is empty, it means we include them all
				if(selectorFilters != null && !selectorFilters.isEmpty() && !selectorFilters.contains(selector)) {
					// ignore the selector
					continue;
				}
				
				// see if the file already exists
				// so if we are not overriding, we can skip this selector
				String outputFileName = appId + ";";
				if(selector.contains("__")) {
					String[] split = selector.split("__");
					outputFileName += split[0] + ";" + split[1];
				} else {
					outputFileName += selector + ";default_node_value";
				}
				outputFileName += ".tsv";
				String outputFile = this.folderPath + "/" + outputFileName;
				if(!this.override && new File(outputFile).exists()) {
					logger.info("Hash already exists for " + selector);
					
					// add to list of files used
					fileNames.add(outputFileName);
					status.add("existing");
					continue;
				}
				
				// add to list of files used
				fileNames.add(outputFileName);
				status.add("new");
				
				logger.info("Querying data for " + selector);
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.addSelector(new QueryColumnSelector(selector));
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
				File f = new File(this.folderPath + "/" + selector + "_base.tsv");
				try {
					// write file
					Utility.writeResultToFile(f.getAbsolutePath(), wrapper, "/t");
					// read into R
					String randomFrame = Utility.getRandomString(6);
					this.rJavaTranslator.executeEmptyR(RSyntaxHelper.getFReadSyntax(randomFrame, f.getAbsolutePath(), "\t"));
					// run the script which also outputs the file
					// we care about the file name since we use that to split to know the source
					logger.info("Generating hash for " + selector);
					this.rJavaTranslator.executeEmptyR("encode_instances(" + randomFrame + ", \"" + outputFile + "\")");
					logger.info("Done generating hash");
				} finally {
					if(f.exists()) {
						f.delete();
					}
				}
			}
		}
		
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.MAP);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully stored hash files for databases"));
		return noun;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Grab values from store
	 */
	
	private List<String> getApps() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		return grs.getAllStrValues();
	}

	private Map<String, List<String>> getConfig() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		if(grs != null && !grs.isEmpty()) {
			NounMetadata value = grs.getNoun(0);
			if(value.getNounType() == PixelDataType.MAP) {
				return (Map<String, List<String>>) value.getValue();
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
	
	List<String> getAppIds() {
		return this.appIds;
	}
	
	boolean isOverride() {
		return this.override;
	}
	
	Map<String, List<String>> getConfigMap() {
		return this.configMap;
	}
}
