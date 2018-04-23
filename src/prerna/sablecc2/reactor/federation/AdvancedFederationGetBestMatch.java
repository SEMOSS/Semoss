package prerna.sablecc2.reactor.federation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationGetBestMatch extends AbstractRFrameReactor {
	
	public static final String FRAME_COLUMN = "frameCol";
	
	public AdvancedFederationGetBestMatch() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), FRAME_COLUMN };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		final String fedFrame = "ADVANCED_FEDERATION_FRAME";
		final String rCol1 = "ADVANCED_FEDERATION_FRAME_COL1";
		final String rCol2 = "ADVANCED_FEDERATION_FRAME_COL2";

		// check if packages are installed
		String[] packages = {"jsonlite", "stringdist"};
		this.rJavaTranslator.checkPackages(packages);
		
		// for the first iteration we have to build the inputs, second iteration we already have them
		String newDb = this.keyValue.get(this.keysToGet[0]);
		String newCol = this.keyValue.get(this.keysToGet[1]);
		String newTable = this.keyValue.get(this.keysToGet[2]);
		String frameCol = this.keyValue.get(this.keysToGet[3]);

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
		Iterator<IHeadersDataRow> it2 = WrapperManager.getInstance().getRawWrapper(newColEngine, qs);
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

		// generate script based on what george wants - empty list of selected
		String bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; "
				+ fedFrame + " <- best_match_mindist(" + rCol1 + "," + rCol2 + ", 1); "
				+ "library(jsonlite); ADVANCED_FEDFRAME_JSON <- toJSON(" + fedFrame
				+ ", byrow = TRUE, colNames = TRUE);";

		bestMatchScript = bestMatchScript.replace("\\", "/");
		// execute script
		this.rJavaTranslator.runR(bestMatchScript);

		String combineScript = fedFrame + "$combined <- paste(" + fedFrame + "$col1, " + fedFrame + "$col2, sep=\" == \");"
				+ fedFrame + "$distance <- paste(" + fedFrame + "$dist);" + fedFrame + "<-" + fedFrame
				+ "[,c(\"col1\",\"col2\",\"distance\",\"combined\")]; ADVANCED_FEDFRAME_JSON <- toJSON(" + fedFrame
				+ ", byrow = TRUE, colNames = TRUE);";
		
		this.rJavaTranslator.runR(combineScript);

		// receive best matches JSON/Map
		String bestMatchesJson = this.rJavaTranslator.getString("ADVANCED_FEDFRAME_JSON;");

		List<Object> jsonMap = new ArrayList<Object>();
		if (bestMatchesJson == null) {
			throw new IllegalArgumentException("Something went wrong with your R connection!");
		} else {
			try {
				// parse json here
				jsonMap = new ObjectMapper().readValue(bestMatchesJson, List.class);
			} catch (IOException e2) {
				// do nothing
			}
		}
		if (jsonMap != null && jsonMap.isEmpty()){
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
		if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to join on.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
