package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class LookupMergeReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = LookupMergeReactor.class.getName();
	private Logger logger = null;

	public static final String MATCHES = "matches";

	public LookupMergeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), MATCHES };
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
		String newColumn = column + "_matched";

		// create the matches frame and get the matches
		HashMap matches = getMatches();
		StringJoiner request = new StringJoiner(",", "c(", ")");
		StringJoiner match = new StringJoiner(",", "c(", ")");
				
		for (Object instance : matches.keySet()) {
			List<String> group = (List<String>) matches.get(instance);

			int groupIdx = 0;
			int groupLen = group.size();
			for (; groupIdx < groupLen; groupIdx++) {
				request.add("\"" + instance + "\"");
				match.add("\"" + group.get(groupIdx) + "\"");
			}
		}

		StringBuilder matchScript = new StringBuilder();
		matchScript.append("data.frame(");
		matchScript.append("\"" + column + "\"" + "=" + request + ", ");
		matchScript.append("\"" + newColumn + "\"" + "=" + match);
		matchScript.append(")");

		String matchFrame = "LookupMatch" + Utility.getRandomString(6);

		// get the join
		// we will merge on col1
		List<Map<String, String>> joinsList = new ArrayList<Map<String, String>>();
		Map<String, String> join = new HashMap<String, String>();
		join.put(column, column);
		joinsList.add(join);

		StringBuilder script = new StringBuilder();
		script.append(matchFrame + " <- " + matchScript + ";");
		script.append(RSyntaxHelper.getMergeSyntax(frameName, frameName, matchFrame, "inner.join", joinsList) + ";");
		script.append("rm(" + matchFrame + ");gc();");
		script.append("print(\"hello\")");
		
		//message out
		logger.info("Running script to merge lookup table.");

		// run it
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// update the metadata to include the new column
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColumn);
		metaData.setAliasToProperty(frameName + "__" + newColumn, newColumn);
		metaData.setDataTypeToProperty(frameName + "__" + newColumn, SemossDataType.STRING.toString());
		frame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColumn));
		return retNoun;
	}

	// get column using key "COLUMN"
	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			NounMetadata noun1 = columnGRS.getNoun(0);
			String column = noun1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to select column to merge a lookup table");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to select column to merge a lookup table");
	}

	private HashMap getMatches() {
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				HashMap matches = (HashMap) columnGrs.getAllValues().get(0);

				return matches;
			}
			throw new IllegalArgumentException("Need matches to merge a lookup table");
		}
		throw new IllegalArgumentException("Need matches to merge a lookup table");
	}
}
