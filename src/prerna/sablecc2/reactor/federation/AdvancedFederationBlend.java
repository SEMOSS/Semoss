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
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
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
	public static final String FED_FRAME = "fedFrame";

	public AdvancedFederationBlend() {
		this.keysToGet = new String[] { JOIN_TYPE, FRAME_COLUMN, ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), ADDITIONAL_COLS, MATCHES, NONMATCHES, FED_FRAME, PROP_MAX };
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
		List<String> columns = getColumns();
		List<String> allMatches = getInputList(MATCHES);
		List<String> nonMatches = getInputList(NONMATCHES);
		String matchesFrame = this.keyValue.get(this.keysToGet[8]);
		String propValue = this.keyValue.get(this.keysToGet[9]);

		final String linkFrame =  matchesFrame + "link";
		String rand = Utility.getRandomString(8);
		final String trg = "trg_" + rand;
		String updatedNewCol = newCol;

		// SUMMARY: The blend R script uses a link table of all matches selected:
		//
		// Build the base link table using the matchesFrame from best matches reactor using a max distance filter (prop value)
		//
		// Add any specific matches the user selected in the UI
		//
		// Remove any specific non-matches the user selected in UI
		//
		// Build the target table - join column and additional columns to bring in (account for duplicate headers)
		//
		// Convert data types of frame join col and target join col to chr if they are numbers because the link frame will only match chr values
		//
		// Execute blend script
		//
		// Convert data types back to num if they were converted
		
		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// add propagation values to link frame if its not null
		if (propValue != null && !(propValue.isEmpty())) {
			Float intVal = (100 - Float.parseFloat(propValue)) / 100;
			String formattedString = String.format("%.08f", intVal);
			propValue = formattedString;
		} else {
			propValue = "0";
		}
		
		// create link table by filtering for propagation value or less from fed frame
		this.rJavaTranslator.runR("library(data.table); " + linkFrame + " <- " + matchesFrame + "[" + matchesFrame + "$distance <= " + propValue + ",]");

		// add combined lookup column
		this.rJavaTranslator.runR(matchesFrame + "$combined <- paste(" + matchesFrame + "$col1, " + matchesFrame + "$col2, sep=\"==\");");

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
			// add all matches provided
			String script = rand + " <- data.table(\"col1\"=c(" + col1Builder + "), \"col2\"=c(" + col2Builder
					+ "), \"distance\"=c(" + col3Builder + ")); " + linkFrame + " <- rbind(" + linkFrame + "," + rand
					+ "); rm(" + rand + ");";
			this.rJavaTranslator.runR(script);
		}
		// make linkframe unique
		this.rJavaTranslator.runR(linkFrame + " <- unique(" + linkFrame + ");");
		this.rJavaTranslator.runR(linkFrame + "$combined <- paste(" + linkFrame + "$col1, " + linkFrame + "$col2, sep=\"==\");");
					
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

			// remove all non matches from LinkFrame
			this.rJavaTranslator.runR(linkFrame + " <- " + linkFrame + "[!" + linkFrame + "$combined %in% c(" + nonMatchCombo + ")]");

			// run a check if linkframe is empty, no data will result
			String link = this.rJavaTranslator.getString("all.equal(as.character(" + linkFrame + "),character(0))");
			if (link == null) {
				throw new IllegalArgumentException("No matching values left to blend.");
			}
		}
		// drop combined column
		this.rJavaTranslator.runR(linkFrame + " <- " + linkFrame + "[,-c(\"combined\")]");

		// get R Frame
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();

		// get all columns to federate on
		List<String> columnArray = new ArrayList<String>();
		columnArray.add(newCol);
		List<String> inputCols = new ArrayList<String>();
		if (columns != null && !(columns.isEmpty())) {
			inputCols.addAll(columns);
			columnArray.addAll(inputCols);
		}
		
		boolean convertJoinColFromNum = false;

		// update frame meta for new cols being added
		// build qs to pull the target data
		IEngine newColEngine = Utility.getEngine(newDb);
		Map typesMap = new HashMap<String, SemossDataType>();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setEngine(newColEngine);
		
		for (int i = 0; i < columnArray.size(); i++) {
			String additionalColumnInput = columnArray.get(i);
			
			// we will fill these once we figure out if it is a concept or property
			QueryColumnSelector selector = null;
			String conceptDataType = null;
			// this is a hack
			// since i dont know if it is a concept or a property
			// if i get a valid data type, new col is a property for new table
			// if i dont, then newtable is a concept with a prim key that i need to use
			if(newColEngine.getParentOfProperty(additionalColumnInput + "/" + newTable) == null) {
				// we couldn't find a parent for this property
				// this means it is a concept itself
				// and we should only use table
				selector = new QueryColumnSelector(newTable);
				conceptDataType = MasterDatabaseUtility.getBasicDataType(newDb, selector.getTable(), null);
			} else {
				selector = new QueryColumnSelector(newTable + "__" + additionalColumnInput);
				conceptDataType = MasterDatabaseUtility.getBasicDataType(newDb, selector.getColumn(), selector.getTable());
			}
			// add the selector to the qs
			qs.addSelector(selector);
			
			String name = getCleanNewColName(frame.getName(), selector.getAlias());

			// get semoss type, update meta data and keep track
			SemossDataType semossType = SemossDataType.convertStringToDataType(conceptDataType);
			typesMap.put(name, semossType);
			
			// update target join column name if it was cleaned
			if(additionalColumnInput.equals(newCol)){
				updatedNewCol = name;
			}
			
			// do we need to convert the join col to a string?
			if(selector.getAlias().equals(newCol)) {
				if(semossType == SemossDataType.DOUBLE || semossType == SemossDataType.INT) {
					convertJoinColFromNum = true;
				}
			}
			
			// update meta data in frame
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(frameName, frameName + "__" + name);
			metaData.setAliasToProperty(frameName + "__" + name, name);
			metaData.setDataTypeToProperty(frameName + "__" + name, semossType.toString());
			metaData.setQueryStructNameToProperty(frameName + "__" + name, newDb, selector.getQueryStructName());
			selector.setAlias(name);
		}

		// write iterator data to csv, then read csv into R table as trg
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(newColEngine, qs);
		String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
		File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap, "\t");
		String loadFileRScript = RSyntaxHelper.getFReadSyntax(trg, newFile.getAbsolutePath(), "\\t");
		//trg + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/") + "\", sep=\"\t\");";
		this.rJavaTranslator.runR(loadFileRScript);
		newFile.delete();

		// get frame join column data type
		OwlTemporalEngineMeta frameMeta = frame.getMetaData();
		String unique = frameMeta.getUniqueNameFromAlias(frameCol);
		String uniqueMetaName = frameMeta.getPhysicalName(unique);
		String frameColType = frameMeta.getHeaderTypeAsString(uniqueMetaName);

		boolean linkColWasNum = false;
		// if either join columns are numbers, update the frames so it joins properly
		if (SemossDataType.convertStringToDataType(frameColType).equals(SemossDataType.DOUBLE) || SemossDataType.convertStringToDataType(frameColType).equals(SemossDataType.INT)) {
			// convert frame col to chr
			this.rJavaTranslator.runR(frameName + "$" + frameCol + " <- as.character(" + frameName + "$" + frameCol + ");");
			// make note for later to convert back to num
			linkColWasNum = true;

		}
		if (convertJoinColFromNum) {
			// convert trg table col to chr
			this.rJavaTranslator.runR(trg + "$" + updatedNewCol + " <- as.character(" + trg + "$" + updatedNewCol + ");");
		}

		// execute blend
		String blendedFrameScript = frameName + " <- blend(" + frameName + ", \"" + frameCol + "\"," + trg + ",\"" + updatedNewCol + "\", " + linkFrame + " , \"" + joinType + "\")";
		this.rJavaTranslator.runR(blendedFrameScript);

		// if columns were num before convert them back so
		// the data types are the same as they started with
		if (linkColWasNum) {
			this.rJavaTranslator.runR(frameName + "$" + frameCol + " <- as.numeric(as.character(" + frameName + "$" + frameCol + "));");
		}
		if (convertJoinColFromNum) {
			this.rJavaTranslator.runR(frameName + "$" + updatedNewCol + " <- as.numeric(as.character(" + frameName + "$" + updatedNewCol + "));");
		}

		// remove columns from frame that are temporary
		String removeExtras = frameName + " <- " + frameName + "[,-c(\"i.and.d\",\"col1\",\"col2\",\"distance.y\",\"distance.x\")]";
		this.rJavaTranslator.runR(removeExtras);

		// reset the frame headers
		this.getFrame().syncHeaders();

		// delete advanced Fed frame in R, done with it
		this.rJavaTranslator.runR("rm(" + trg + "," + matchesFrame + ", BaseLinkFrame, " + linkFrame + ", best_match_zero, best_match, blend, best_match_lessthan, best_match_nonzero)");

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
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
