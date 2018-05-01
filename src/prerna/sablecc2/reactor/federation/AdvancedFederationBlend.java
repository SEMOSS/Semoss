package prerna.sablecc2.reactor.federation;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationBlend extends AbstractRFrameReactor {
	public static final String JOIN_TYPE = "joinType";
	public static final String FRAME_COLUMN = "frameCol";
	public static final String ADDITIONAL_COLS = "additionalCols";
	public static final String MATCHES = "matches";
	public static final String NONMATCHES = "nonMatches";
	public static final String PROP_MAX = "propagation";

	public AdvancedFederationBlend() {
		this.keysToGet = new String[] { JOIN_TYPE, FRAME_COLUMN, ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ADDITIONAL_COLS, MATCHES, NONMATCHES, PROP_MAX };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get all inputs
		String joinType = this.keyValue.get(this.keysToGet[0]);
		String frameCol = this.keyValue.get(this.keysToGet[1]);
		String newDb = this.keyValue.get(this.keysToGet[2]);
		String newTable = this.keyValue.get(this.keysToGet[3]);
		String newCol = this.keyValue.get(this.keysToGet[4]);
		String columns = this.keyValue.get(this.keysToGet[5]);
		List<String> allMatches = getInputList(MATCHES);
		List<String> nonMatches = getInputList(NONMATCHES);
		String propValue = this.keyValue.get(this.keysToGet[8]);

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		final String matchesFrame = "advanced__fed__frame";
		final String rCol1 = "advanced__fed__frame__col1";
		final String rCol2 = "advanced__fed__frame__col2";
		final String linkFrame = "ad__fed__link";
		String rand = Utility.getRandomString(8);
		final String trg = "trg_" + rand;

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// make empty link table
		this.rJavaTranslator.runR("library(data.table); " + linkFrame
				+ " <- data.table(\"col1\" = character(), \"col2\" = character(), \"dist\"= integer(), stringsAsFactors=FALSE);");

		// add propagation values to link frame if its not null
		if (propValue != null && !(propValue.isEmpty())) {
			Float intVal = (100 - Float.parseFloat(propValue)) / 100;
			String formattedString = String.format("%.08f", intVal);
			propValue = ", " + formattedString;
		} else {
			propValue = "";
		}

		// add all zero exact matches and propagation values to link table
		String linkScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; "
				+ "BaseLinkFrame <- best_match_lessthan(" + rCol1 + "," + rCol2 + propValue + "); " + linkFrame
				+ " <- rbind(" + linkFrame + ", BaseLinkFrame);";
		// rm(BaseLinkFrame)
		linkScript = linkScript.replace("\\", "/");
		this.rJavaTranslator.runR(linkScript);

		// add combined lookup column
		this.rJavaTranslator.runR(matchesFrame + "$combined <- paste(" + matchesFrame + "$col1, " + matchesFrame
				+ "$col2, sep=\" == \");");

		// add all matches
		if (allMatches != null && !(allMatches.isEmpty())) {
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
				String test = "as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match
						+ "\"), ]$col1)";
				String col1 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame
						+ "$combined %in% c(\"" + match + "\"), ]$col1)");
				String col2 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame
						+ "$combined %in% c(\"" + match + "\"), ]$col2)");
				String dist = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame
						+ "$combined %in% c(\"" + match + "\"), ]$distance)");
				col1Builder.append("\"" + col1 + "\"");
				col2Builder.append("\"" + col2 + "\"");
				col3Builder.append(dist);
			}
			// build link frame with all exact matches
			// add all matches provided
			String script = rand + " <- data.table(\"col1\"=c(" + col1Builder + "), \"col2\"=c(" + col2Builder
					+ "), \"dist\"=c(" + col3Builder + ")); " + linkFrame + " <- rbind(" + linkFrame + "," + rand
					+ "); rm(" + rand + ");";
			this.rJavaTranslator.runR(script);
		}
		// make linkframe unique
		this.rJavaTranslator.runR("" + linkFrame + " <- unique(" + linkFrame + ");");

		// remove all non-matches
		if (nonMatches != null && !(nonMatches.isEmpty())) {
			StringBuilder nonMatchCombo = new StringBuilder();
			for (int i = 0; i < nonMatches.size(); i++) {
				if (i != 0) {
					nonMatchCombo.append(",");
				}
				String match = (String) nonMatches.get(i);
				nonMatchCombo.append("\"" + match + "\"");
			}

			// add combined lookup column
			this.rJavaTranslator.runR("" + linkFrame + "$combined <- paste(" + linkFrame + "$col1, " + linkFrame
					+ "$col2, sep=\" == \");");

			// remove all non matches from LinkFrame
			String abc = "<- " + linkFrame + "[!" + linkFrame + "$combined %in% c(" + nonMatchCombo + ")]";
			this.rJavaTranslator.runR("" + linkFrame + " <- " + linkFrame + "[!" + linkFrame + "$combined %in% c("
					+ nonMatchCombo + ")]");

			// run a check if linkframe is empty, no data will result
			String link = this.rJavaTranslator.getString("all.equal(as.character(" + linkFrame + "),character(0))");
			if (link == null) {
				throw new IllegalArgumentException("No matching values left to blend.");
			}

			// drop combined column
			this.rJavaTranslator.runR("" + linkFrame + " <- " + linkFrame + "[,-c(\"combined\")]");
		}

		// get R Frame
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();

		// get all columns to federate on
		ArrayList<String> cleanColumns = new ArrayList<String>();
		ArrayList<String> columnArray = new ArrayList<String>();
		columnArray.add(newCol);
		List<String> inputCols = new ArrayList<String>();
		if (columns != null && !(columns.isEmpty())) {
			inputCols.addAll(getColumns());
			columnArray.addAll(inputCols);
		}

		IEngine newColEngine = Utility.getEngine(newDb);
		Map typesMap = new HashMap<String, SemossDataType>();
		QueryStruct2 qs = new QueryStruct2();
		qs.setEngine(newColEngine);
		for (int i = 0; i < columnArray.size(); i++) {
			String col = columnArray.get(i);
			QueryColumnSelector selector = new QueryColumnSelector();
			selector.setTable(newTable);
			selector.setColumn(col);
			qs.addSelector(selector);
			// set alias with clean column name in case the frame already that
			// header
			String name = getCleanNewColName(frame.getTableName(), col);
			// update newCol variable in case it gets updated
			if (col.equals(newCol)) {
				newCol = name;
			}
			cleanColumns.add(name);
			selector.setAlias(name);
			// get semoss type, update meta data and keep track
			String conceptDataType = MasterDatabaseUtility.getBasicDataType(newDb, col, newTable);
			SemossDataType semossType = SemossDataType.convertStringToDataType(conceptDataType);
			typesMap.put(col, semossType);
			// update meta data in frame
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(frameName, frameName + "__" + name);
			metaData.setAliasToProperty(frameName + "__" + name, name);
			metaData.setDataTypeToProperty(frameName + "__" + name, semossType.toString());
			metaData.setQueryStructNameToProperty(frameName + "__" + name, newDb, newTable + "__" + col);
		}

		// write iterator data to csv, then read csv into R table as trg
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(newColEngine, qs);
		String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/"
				+ Utility.getRandomString(6) + ".tsv";
		File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap, "\t");
		String loadFileRScript = trg + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/")
				+ "\", sep=\"\t\");";
		this.rJavaTranslator.runR(loadFileRScript);
		newFile.delete();

		// execute blend
		String blendedFrameScript = frameName + " <- blend(" + frameName + ", \"" + frameCol + "\"," + trg + ",\""
				+ newCol + "\", " + linkFrame + " , \"" + joinType + "\")";
		this.rJavaTranslator.runR(blendedFrameScript);

		// remove columns from frame that are temporary
		String removeExtras = frameName + " <- " + frameName
				+ "[,-c(\"i.and.d\",\"col1\",\"col2\",\"dist.y\",\"dist.x\")]";
		this.rJavaTranslator.runR(removeExtras);

		// reset the frame headers
		this.getFrame().syncHeaders();

		// delete advanced Fed frame in R, done with it
		this.rJavaTranslator.runR("rm(" + trg + "," + rCol1 + "," + rCol2 + ", " + matchesFrame + ", BaseLinkFrame, "
				+ linkFrame + ", best_match_zero, best_match, blend, best_match_lessthan)");

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata((inputCols)));
		return retNoun;
	}

	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ADDITIONAL_COLS);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}

		// else, we assume it is values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for (Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
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

	///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JOIN_TYPE)) {
			return "The join type for federating (inner, outer, left, etc.)";
		} else if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to use for joining.";
		} else if (key.equals(MATCHES)) {
			return "The matches of columns that were selected by the user.";
		} else if (key.equals(ADDITIONAL_COLS)) {
			return "Additional columns to pull join with the existing frame.";
		}  else if (key.equals(NONMATCHES)) {
			return "The non-matches that were selected by the user.";
		} else if (key.equals(PROP_MAX)) {
			return "Optional: The range for automatically matching instances (100 exact match, 1 is not similar)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
