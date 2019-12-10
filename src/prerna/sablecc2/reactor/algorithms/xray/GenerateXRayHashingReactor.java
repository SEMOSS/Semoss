package prerna.sablecc2.reactor.algorithms.xray;

import java.io.File;
import java.util.Collection;
import java.util.List;

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

	private String folderPath;
	private List<String> appIds;
	private boolean override;
	
	public GenerateXRayHashingReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), 
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONFIG.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
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
		if(folderName != null) {
			this.folderPath = baseFolder + "/" + folderName;
		} else {
			this.folderPath = baseFolder + "/xray_corpus";
			File defaultXrayFolder = new File(this.folderPath);
			if(!defaultXrayFolder.exists() || !defaultXrayFolder.isDirectory()) {
				defaultXrayFolder.mkdirs();
			}
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
		this.override = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]) + "");
		
		// source the packages and R script
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.loadPackages(reqPackages) + "; source(\"" 
				+ DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace('\\', '/') 
				+ "/R/XRay/encode_instances.R\", local=TRUE);");
		
		// go through and write the app
		for(String appId : appIds) {
			IEngine engine = Utility.getEngine(appId);
			Collection<String> pixelSelectors = MasterDatabaseUtility.getSelectorsWithinEngineRDBMS(appId);
			for(String selector : pixelSelectors) {
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
				String outputFile = folderPath + "/" + outputFileName;
				outputFile = outputFile.replace('\\', '/');
				if(!this.override && new File(outputFile).exists()) {
					logger.info("Hash already exists for " + selector);
					continue;
				}
				
				logger.info("Querying data for " + selector);
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.addSelector(new QueryColumnSelector(selector));
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
				File f = new File(folderPath + "/" + selector + "_base.tsv");
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
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
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
}
