package prerna.sablecc2.reactor.frame.r;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.r.RDataTable;
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
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// initialize the reactor
		init();
		this.logger = getLogger(CLASS_NAME);

		// get inputs
		String instance = this.keyValue.get(this.keysToGet[2]);
		String count = getCount();


		// get the location to save
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String fileName = this.keyValue.get(keysToGet[0]);
		String filePath = assetFolder + "/" + fileName;

		// get the output frame
		String matchFrame = "LookupMatch" + Utility.getRandomString(6);

		// load the script
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");

		StringBuilder script = new StringBuilder();
		script.append("source(\"" + base + "/R/Lookup/fuzzy_lookup.r" + "\");");
		script.append(matchFrame + " <- fuzzy_lookup(");
		script.append("catalog_fn=" + "\"" + filePath + "\"" + ", ");
		script.append("request=" + "\"" + instance + "\"" + ", ");
		script.append("topMatches=" + count);
		script.append(")");

		logger.info("Finding matches for " + instance + " in the lookup table.");

		// run it
		this.rJavaTranslator.runR(script.toString());

		// get the data
		String[] matchCols = this.rJavaTranslator.getColumns(matchFrame);
		List<Object[]> matchData = this.rJavaTranslator.getBulkDataRow(matchFrame, matchCols);

		// clean up r temp variables
		this.rJavaTranslator.runR("rm(" + matchFrame + ");gc();");

		// convert to [{}] (easier processing)
		List<Map<String, Object>> returnData = new Vector<Map<String, Object>>();
		for (int rowIdx = 0; rowIdx < matchData.size(); rowIdx++) {
			Object[] row = matchData.get(rowIdx);

			Map<String, Object> holder = new HashMap<String, Object>();
			for (int colIdx = 0; colIdx < row.length; colIdx++) {
				holder.put(matchCols[colIdx], row[colIdx]);
			}

			returnData.add(holder);
		}

		return new NounMetadata(returnData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
	
	private String getCount() {
		GenRowStruct grs = this.store.getNoun(COUNT);
		if(grs != null && !grs.isEmpty()) {
			try {
				int value = ((Number) grs.get(0)).intValue();
				
				return String.valueOf(value);
			} catch(ClassCastException e) {
				throw new IllegalArgumentException("Count is not a valid number");
			}
		}
		return "5";
	}
}
