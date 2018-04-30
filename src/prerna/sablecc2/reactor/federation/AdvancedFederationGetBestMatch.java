package prerna.sablecc2.reactor.federation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationGetBestMatch extends AbstractRFrameReactor {
	public static final String FRAME_COLUMN = "frameCol";
	public static final String MATCHES = "matches";
	public static final String FIRST_ITERATION = "firstIteration";
	
	public AdvancedFederationGetBestMatch() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), FRAME_COLUMN , MATCHES, FIRST_ITERATION};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// 4 column results df with matches, distance, and combined column
		final String matchesFrame = "ADVANCED_FEDERATION_FRAME";
		// 1 column df of all data in frame join column
		final String rCol1 = "ADVANCED_FEDERATION_FRAME_COL1";
		// 1 column df of all data in the incoming join column
		final String rCol2 = "ADVANCED_FEDERATION_FRAME_COL2";

		// check if packages are installed
		String[] packages = {"jsonlite", "stringdist", "data.table"};
		this.rJavaTranslator.checkPackages(packages);
		
		// for the first iteration we have to build the inputs, second iteration
		// we already have them
		String newDb = this.keyValue.get(this.keysToGet[0]);
		String newTable = this.keyValue.get(this.keysToGet[1]);
		String newCol = this.keyValue.get(this.keysToGet[2]);
		String frameCol = this.keyValue.get(this.keysToGet[3]);
		boolean firstIt = getFirstItFlag();

		List<String> allMatches = getMatches();

		// if matches is empty then its the first iteration, so create all DFs
		// otherwise we assume all resources are created already so just add matches 
		// to the link table and get fresh matches
		if (firstIt) {
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

			// create empty LinkTable - its lazy loaded with all exact matches in the blend reactor,
			// using rCol1 and rCol2 variables, in case the user ends up backing out before blending
			this.rJavaTranslator.runR("library(data.table); LinkFrame <- data.table(\"col1\" = character(), \"col2\" = character(), \"dist\"= integer(), stringsAsFactors=FALSE);");
			
		} else if (allMatches != null && !(allMatches.isEmpty())){
			StringBuilder col1Builder = new StringBuilder();
			StringBuilder col2Builder = new StringBuilder();
			StringBuilder col3Builder = new StringBuilder();
			String rand = Utility.getRandomString(8);
			if (allMatches != null && !(allMatches.isEmpty())) {
				for (int i = 0; i < allMatches.size(); i++) {
					if (i != 0) {
						col1Builder.append(",");
						col2Builder.append(",");
						col3Builder.append(",");
					}
					String match = (String) allMatches.get(i);
					String col1 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$col1)");
					String col2 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$col2)");
					String dist = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$distance)");
					col1Builder.append("\"" + col1 + "\"");
					col2Builder.append("\"" + col2 + "\"");
					col3Builder.append(dist);
				}
			}
				
				// add all matches provided
				String script =  rand + " <- data.table(\"col1\"=c(" + col1Builder + "), \"col2\"=c(" + col2Builder + "), \"dist\"=c(" + col3Builder + ")); LinkFrame <- rbind(LinkFrame," + rand + ");";
				this.rJavaTranslator.runR(script);
			
			// remove all selected matches from the input rCol1 and rCol2 so they dont see them again
			this.rJavaTranslator.runR(rCol1 + " <- " + rCol1 + "[!" + rCol1 + " %in% c(" + col1Builder + ")]");
			this.rJavaTranslator.runR(rCol2 + " <- " + rCol2 + "[!" + rCol2 + " %in% c(" + col2Builder + ")]");

			// run a check if either column is empty, if so, throw a warning and stop matching
			String r1empty = this.rJavaTranslator.getString("all.equal(" + rCol1 + ",character(0))");
			String r2empty = this.rJavaTranslator.getString("all.equal(" + rCol2 + ",character(0))");
			if (r1empty == null || r2empty == null){
				throw new IllegalArgumentException("All values have matches, blend now.");
			}
		}

		// generate script based on what george wants - empty list of selected
		String bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; "
				+ matchesFrame + " <- best_match_nonzero(" + rCol1 + "," + rCol2 + ");";
		bestMatchScript = bestMatchScript.replace("\\", "/");

		this.rJavaTranslator.runR(bestMatchScript);

		// add a unique combined col1 == col2, remove extra columns, 
		// order by distance, and convert to json so we can get it back in java
		String combineScript = matchesFrame + "$combined <- paste(" + matchesFrame + "$col1, " + matchesFrame + "$col2, sep=\" == \");"
				+ matchesFrame + "$distance <- as.numeric(" + matchesFrame + "$dist);" + matchesFrame + "<-" + matchesFrame
				+ "[,c(\"col1\",\"col2\",\"distance\",\"combined\")]; " + matchesFrame + "<-" + matchesFrame + "[order(" + matchesFrame + "$dist),] ; library(jsonlite); ADVANCED_FEDFRAME_JSON <- toJSON(head(unique(" + matchesFrame
				+ "), 500), byrow = TRUE, colNames = TRUE);";
		
		this.rJavaTranslator.runR(combineScript);
		
		// receive best matches JSON/Map
		String bestMatchesJson = this.rJavaTranslator.getString("ADVANCED_FEDFRAME_JSON;");

		List<Object> jsonMap = new ArrayList<Object>();
		if (bestMatchesJson == null) {
			throw new IllegalArgumentException("Best Matches Failed.");
		} else {
			try {
				// parse json here
				jsonMap = new ObjectMapper().readValue(bestMatchesJson, List.class);
			} catch (IOException e2) {
				throw new IllegalArgumentException(e2);
			}
		}

		// garbage collection (keep fedFrame)
		String gc = "rm(ADVANCED_FEDFRAME_JSON);";
		this.rJavaTranslator.runR(gc);

		return new NounMetadata(jsonMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private List<String> getMatches() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(MATCHES);
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
	
	private boolean getFirstItFlag() {
		GenRowStruct boolGrs = this.store.getNoun(FIRST_ITERATION);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to join on.";
		} else if (key.equals(MATCHES)) {
			return "The matches of columns that are selected by the user.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
