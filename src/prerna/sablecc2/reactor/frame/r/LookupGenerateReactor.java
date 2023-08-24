package prerna.sablecc2.reactor.frame.r;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class LookupGenerateReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = LookupGenerateReactor.class.getName();
	private Logger logger = null;

	public LookupGenerateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// initialize the reactor
		init();
		this.logger = getLogger(CLASS_NAME);

		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String frameName = frame.getName();

		// get inputs
		String column = getColumn();
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// get the location to save
		String space = this.keyValue.get(this.keysToGet[2]);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String fileName =  Utility.normalizePath(this.keyValue.get(keysToGet[1]));
		String filePath = assetFolder + "/" + fileName;

		// load the script
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");

		StringBuilder script = new StringBuilder();
		script.append("source(\"" + base + "/R/Lookup/fuzzy_lookup.r" + "\");");
		script.append("prepare_catalog(");
		script.append(frameName + ", ");
		script.append("catalog_fn=" + "\"" + filePath + "\"" + ", ");
		script.append("catalog_col=" + "\"" + column + "\"");
		script.append(")");

		logger.info("Running script to generate lookup table.");

		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());
//		String output = this.rJavaTranslator.runRAndReturnOutput(script.toString());
//		logger.info(output);

		return NounMetadata.getSuccessNounMessage("Successfully generated Lookup Table.");
	}

	// get column using key "COLUMN"
	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			NounMetadata noun1 = columnGRS.getNoun(0);
			String column = noun1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to select column to generate a lookup table");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to select column to generate a lookup table");
	}
}
