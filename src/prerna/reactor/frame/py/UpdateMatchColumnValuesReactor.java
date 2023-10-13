package prerna.reactor.frame.py;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UpdateMatchColumnValuesReactor extends AbstractPyFrameReactor {

	public static final String MATCHES = "matches";
	public static final String MATCHES_TABLE = "matchesTable";

	public UpdateMatchColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), MATCHES_TABLE, MATCHES };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		if(column == null | column.isEmpty()) {
			throw new IllegalArgumentException("Must pass in the column to run the update on");
		}
		String matchesTable = this.keyValue.get(this.keysToGet[1]);

		List<String> scripts = new Vector<String>();

		// get single column input
		String linkFrame = "link" + Utility.getRandomString(5);
		PandasFrame frame = (PandasFrame) getFrame();
		String frameName = frame.getName();
		String wrapperName = frame.getWrapperName();
		String col1 = matchesTable + "col1";
		scripts.add(col1 + "= " + frameName + "['" + column + "']");

		// iterate matches and create the link frame
		List<String> allMatches = getInputList(MATCHES);
		if(allMatches == null || allMatches.isEmpty()) {
			throw new IllegalArgumentException("Must pass in matches to connect the 'current value' to the 'replacement value'");
		}
		// add all matches
		StringBuilder col1Builder = new StringBuilder();
		StringBuilder col2Builder = new StringBuilder();
		StringBuilder col3Builder = new StringBuilder();
		for (int i = 0; i < allMatches.size(); i++) {
			if (i != 0) {
				col1Builder.append(",");
				col2Builder.append(",");
				col3Builder.append(",");
			}
			String match = (String) allMatches.get(i);
			String[] matchList = match.split(" == ");
			if (matchList.length > 2) {
				throw new IllegalArgumentException("match seperator didnt work");
			}
			String column1 = matchList[0];
			String column2 = matchList[1];
			col1Builder.append("'" + column1 + "'");
			col2Builder.append("'" + column2 + "'");
			col3Builder.append("1");
		}
		// add all matches provided
		scripts.add(linkFrame + " = pd.DataFrame({'col1':[" + col1Builder + "], 'col2':[" + col2Builder + "]})");
		
		// make link frame unique
		scripts.add(linkFrame + " = " + linkFrame + ".drop_duplicates()");

		// get current frame data type
		boolean convertJoinColFromNum = false;
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		Map<String, SemossDataType> typeMap = metaData.getHeaderToTypeMap();
		SemossDataType dataType = typeMap.get(column);
		if (dataType == SemossDataType.DOUBLE || dataType == SemossDataType.INT) {
			convertJoinColFromNum = true;
		}

		scripts.add(wrapperName + ".merge_match_results('" + column + "'," + linkFrame + ")");
		
		// return data type to original state
		if (convertJoinColFromNum) {
			scripts.add(frameName + "['" + column + "'] = pd.to_numeric(" + frameName + "['" + column + "'])");
		}
		
		insight.getPyTranslator().runPyAndReturnOutput(scripts.toArray(new String[scripts.size()]));
		for(String script : scripts) {
			this.addExecutedCode(script);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"UpdateSimilarColumnValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);

		return retNoun;
	}

	private List<String> getInputList(String key) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(key);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<String> values = columnGrs.getAllStrValues();
				return values;
			}
		}
		// else, we assume it is values in the curRow
		List<String> values = this.curRow.getAllStrValues();
		return values;
	}
}
