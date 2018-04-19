package prerna.sablecc2.reactor.federation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationGetBestMatch extends AbstractRFrameReactor {
	public static final String SELECTED_MATCHES = "matches";
	public static final String FIRST_ITERATION = "firstIt";
	public static final String DB_TABLE_COL = "dbTableCol";
	public static final String FRAME_COLUMN = "frameCol";

	public AdvancedFederationGetBestMatch() {
		this.keysToGet = new String[] { SELECTED_MATCHES, FIRST_ITERATION, DB_TABLE_COL, FRAME_COLUMN };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String selected = this.keyValue.get(this.keysToGet[0]);
		boolean iteration = Boolean.valueOf(this.keyValue.get(this.keysToGet[1]));
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		final String fedFrame = "ADVANCED_FEDERATION_FRAME";
		final String rCol1 = "ADVANCED_FEDERATION_FRAME_COL1";
		final String rCol2 = "ADVANCED_FEDERATION_FRAME_COL2";
		int matchesNum = Integer.parseInt(selected);

		if (selected != null && !(selected.isEmpty())) {
			// not implemented yet - for iterating and refining matches
		}

		// for the first iteration we have to build the inputs, second iteration we already have them
		String newDb = "";
		String newCol = "";
		String newTable = "";
		String frameCol = "";

		if (iteration) {
			String dbTableCol = this.keyValue.get(this.keysToGet[2]);
			frameCol = this.keyValue.get(this.keysToGet[3]);
			if (dbTableCol == null || frameCol == null) {
				throw new IllegalArgumentException("Must provide join database, table, and column!");
			}
			// retrieve DB and Col from input, map or split with dunder
			List<String> one = Arrays.asList(dbTableCol.split("__"));

			// TODO: use split array size as index in case user chose to put
			// double underscores in db name
			newDb = one.get(one.size() - 3);
			newTable = one.get(one.size() - 2);
			newCol = one.get(one.size() - 1);
		}

		String bestMatchScript = "";
		if (iteration) {
			IEngine newColEngine = Utility.getEngine(newDb);
			RDataTable frame = (RDataTable) getFrame();
			String frameName = frame.getTableName();
			String rTable1 = rCol1 + " <- as.character(" + frameName + "$" + frameCol + ");";

			// create script to generate col2 from table to be joined
			QueryStruct2 qs = new QueryStruct2();
			qs.setEngine(newColEngine);
			QueryColumnSelector selector = new QueryColumnSelector();
			selector.setTable(newTable);
			selector.setColumn(newCol);
			qs.addSelector(selector);
			StringBuilder rTable2 = new StringBuilder();
			
			rTable2.append(rCol2 + " <- as.character(unique(c(");
			IRawSelectWrapper it2 = WrapperManager.getInstance().getRawWrapper(newColEngine, qs);
			int count = 0;
			while (it2.hasNext()) {
				Object[] values = it2.next().getValues();
				if (count != 0) {
					rTable2.append(",");
				}
				rTable2.append("\"");
				rTable2.append(values[0] + "");
				rTable2.append("\"");
				count++;
			}
			rTable2.append(")))");

			// execute the scripts
			this.rJavaTranslator.executeR(rTable1);
			this.rJavaTranslator.executeR(rTable2.toString());

			// generate script based on what george wants - empty list of
			// selected
			bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_fedeartion_blend.r\") ; "
					+ fedFrame + " <- best_match_mindist(" + rCol1 + "," + rCol2 + ", " + matchesNum + "); "
					+ "library(jsonlite); ADVANCED_FEDFRAME_JSON <- toJSON(" + fedFrame
					+ ", byrow = TRUE, colNames = TRUE);";

		} else {
			// not first time running this, use the existing rCols and
			// bump the matches min up one to give them less similar matches
			matchesNum += 1;
			bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_fedeartion_blend.r\") ; "
					+ fedFrame + " <- best_match_mindist(" + rCol1 + "," + rCol2 + "," + matchesNum + "); ";
		}
		bestMatchScript = bestMatchScript.replace("\\", "/");
		// execute script
		this.rJavaTranslator.runR(bestMatchScript);

		// convert to two columns
		String combineScript = fedFrame + "$match <- paste(" + fedFrame + "$col1, " + fedFrame + "$col2, sep=\" == \");"
				+ fedFrame + "$distance <- paste(" + fedFrame + "$col1, " + fedFrame + "$col2, " + fedFrame
				+ "$dist, sep=\"___\");" + fedFrame + "<-" + fedFrame
				+ "[,c(\"match\",\"distance\")];  library(jsonlite); ADVANCED_FEDFRAME_JSON <- toJSON(" + fedFrame
				+ ", byrow = TRUE, colNames = TRUE);";

		this.rJavaTranslator.runR(combineScript);

		// receive best matches JSON/Map
		String bestMatchesJson = this.rJavaTranslator.getString("ADVANCED_FEDFRAME_JSON;");

		List<Object> jsonMap = new ArrayList<Object>();
		if (bestMatchesJson != null) {
			try {
				// parse json here
				jsonMap = new ObjectMapper().readValue(bestMatchesJson, List.class);
			} catch (IOException e2) {
				// do nothing
			}
		} else {
			throw new IllegalArgumentException("All rows have matches already!");
		}

		// garbage collection (keep fedFrame)
		String gc = "rm(ADVANCED_FEDFRAME_JSON);";
		this.rJavaTranslator.runR(gc);

		return new NounMetadata(jsonMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SELECTED_MATCHES)) {
			return "The selected matches if this is not the first iteration.";
		} else if (key.equals(FIRST_ITERATION)) {
			return "Boolean flag for if this is the first iteration or not.";
		} else if (key.equals(DB_TABLE_COL)) {
			return "The db, table, and col to federate with. Combined with double underscores \"__\".";
		} else if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to join on.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
