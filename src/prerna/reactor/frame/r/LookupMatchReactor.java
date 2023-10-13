package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class LookupMatchReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = LookupMatchReactor.class.getName();
	private Logger logger = null;

	public static final String COUNT = "count";

	public LookupMatchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.INSTANCE_KEY.getKey(), COUNT };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// initialize the r connection
		init();
		// check packages
		String[] packages = new String[]{"qs"};
		this.rJavaTranslator.checkPackages(packages);
		this.logger = getLogger(CLASS_NAME);

		// get inputs
		List<String> instances = getInstances();
		String count = getCount();

		// get the location to save
		String space = this.keyValue.get(this.keysToGet[1]);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String fileName =  Utility.normalizePath(this.keyValue.get(keysToGet[0]));
		String filePath = assetFolder + "/" + fileName;

		// get the output frame
		String rand =  Utility.getRandomString(6);
		String matchFrame = "LookupMatch" + rand;
		String catalog = "LookupCatalog" + rand;
		
		// get the current working directory
		String currentWd = this.rJavaTranslator.getString(RSyntaxHelper.getWorkingDirectory());

		// load the script
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");

		StringBuilder script = new StringBuilder();
		script.append(RSyntaxHelper.loadPackages(packages));
		script.append("source(\"" + base + "/R/Lookup/fuzzy_lookup.r" + "\");");
		// change working directory as new files will be generated
		script.append(RSyntaxHelper.setWorkingDirectory(assetFolder));
		// load catalog
		//TODO check if the file is qs
		script.append(RSyntaxHelper.qread(catalog, filePath + ".qs"));
		script.append(matchFrame + " <- fuzzy_lookup(catalog="+ catalog + ",");
		script.append("catalog_fn=" + "\"" + fileName + "\"" + ", ");
		script.append("request=" + RSyntaxHelper.createStringRColVec(instances) + ", ");
		script.append("topMatches=" + count);
		script.append(")");

		logger.info("Finding matches for " + instances.toString() + "in the lookup table.");

		// run it
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// get the data
		String[] matchCols = this.rJavaTranslator.getColumns(matchFrame);
		List<Object[]> matchData = new ArrayList<Object[]>();
		
		Boolean matchFrameExists = this.rJavaTranslator.getBoolean("exists(\"" + matchFrame + "\")");
		if(matchFrameExists) {
			matchData =	this.rJavaTranslator.getBulkDataRow(matchFrame, matchCols);
		}

		// clean up r temp variables
		// change the working directory to the original
		this.rJavaTranslator.runR("rm(" + matchFrame + ", " + catalog + ");gc();" + RSyntaxHelper.setWorkingDirectory(currentWd));
		
		// convert to {instance:[{}]} (easier processing)
		Map<String, List<Map<String, Object>>> returnData = new HashMap<String, List<Map<String, Object>>>();

		int instanceIdx = 0;
		int instanceLen = instances.size();	
		for (; instanceIdx < instanceLen; instanceIdx++) {
			String instance = instances.get(instanceIdx);
			// create the instance if it isn't there
			if (!returnData.containsKey(instance)) {
				returnData.put(instance, new ArrayList<Map<String, Object>>());
			}

			for (int matchDataIdx = matchData.size() - 1; matchDataIdx >= 0; matchDataIdx--){
				Object[] row = matchData.get(matchDataIdx);

				// check if the request column is the same as the instance column
				if (row[0].equals(instance)) {
					Map<String, Object> holder = new HashMap<String, Object>();
					for (int colIdx = 0; colIdx < row.length; colIdx++) {
						holder.put(matchCols[colIdx], row[colIdx]);
					}

					returnData.get(instance).add(holder);
					
					// remove the instance
					matchData.remove(matchDataIdx);
				}
			}
			// it comes back sorted
		}

		return new NounMetadata(returnData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private String getCount() {
		GenRowStruct grs = this.store.getNoun(COUNT);
		if (grs != null && !grs.isEmpty()) {
			try {
				int value = ((Number) grs.get(0)).intValue();

				return String.valueOf(value);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("Count is not a valid number");
			}
		}
		return "5";
	}

	private List<String> getInstances() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.INSTANCE_KEY.getKey());

		if (grs != null && !grs.isEmpty()) {
			try {
				List<String> values = new Vector<String>();
				for(NounMetadata n : grs.vector) {
					values.add(n.getValue().toString());
				}
				
				return values;
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("Instances need to be defined");
			}
		}

		throw new IllegalArgumentException("Instances need to be defined");
	}
}
