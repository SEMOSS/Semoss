package prerna.sablecc2.reactor.federation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.imports.MergeReactor;

public class FuzzyMergeReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = FuzzyMergeReactor.class.getName();

	public static final String FED_FRAME = "fedFrame";
	public static final String MATCHES = "matches";
	public static final String NONMATCHES = "nonMatches";
	public static final String PROP_MAX = "propagation";

	private Logger logger = null;

	public FuzzyMergeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.JOINS.getKey(), ReactorKeysEnum.FRAME.getKey(), FED_FRAME, MATCHES, NONMATCHES, PROP_MAX };
	}

	@Override
	public NounMetadata execute() {

		/*
		 * The logic for this is to determine the fuzzy matches to maintain within the frame
		 * and then use those new values to join additional columns
		 * 
		 * First, we will take the fedFrame which contains all the matches. 
		 * We will take a subset of this that is above the propagation value. Then, we will add the user defined
		 * matches and remove the user defined nonMatches.
		 * 
		 * Second, drop the unnecessary columns. And then join fuzzy column names to the existing column names
		 * We now have the headers we need
		 * 
		 */

		init();
		this.logger = getLogger(CLASS_NAME);

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// the main script to execute
		StringBuilder script = new StringBuilder();
		script.append("library(data.table);library(stringdist);");

		String propagationValue = getPropagation();
		String matchesFrame = getMatchesFrame();
		
		List<Join> joins = getJoins();
		// this should be of size 1
		if(joins.size() > 1) {
			throw new IllegalArgumentException("Can only support 1 fuzzy join at a time");
		}
		String frameCol = joins.get(0).getSelector();
		// ignore the table name which is most likely passed
		if(frameCol.contains("__")) {
			frameCol = frameCol.split("__")[1];
		}
		// add a combined column
		// we will use this a lookup with the allMatches/nonMatches
		script.append(matchesFrame + "$combined <- paste(" + matchesFrame + "$col1, " + matchesFrame + "$col2, sep=\"==\");");
		
		String linkFrame = matchesFrame + "_LINK";
		// grab the subset of the data required
		
		script.append(linkFrame + "<- " +  matchesFrame + "[" + matchesFrame + "$distance <= " + propagationValue + ",];");

		// grab the lists to append
		// using the combined lookup column, we will be able to just rbind those results with the current linkframe
		List<String> allMatches = getInputList(MATCHES);
		if(allMatches != null && !allMatches.isEmpty()) {
			script.append(linkFrame + "<- rbind(" + linkFrame + ", " +  matchesFrame + "[" + matchesFrame + "$combined %in% " + RSyntaxHelper.createStringRColVec(allMatches)+ ",]);");
		}
		// grab the lists to remove
		// we will use this list to just remove from the existing linkFrame
		List<String> nonMatches = getInputList(NONMATCHES);
		if(nonMatches != null && !nonMatches.isEmpty()) {
			script.append(linkFrame + "<- " + linkFrame + "[!(" + linkFrame + "$combined %in% " + RSyntaxHelper.createStringRColVec(nonMatches)+ "),];");
		}
		
		// drop the combined column + distance column
		script.append(linkFrame + " <- " + linkFrame + "[, combined :=NULL];");
		script.append(linkFrame + " <- " + linkFrame + "[, distance :=NULL];");

		// merge the fuzzy column onto the frame
		ITableDataFrame frame = getFrame();
		String rFrameVar = frame.getName();
		if(!(frame instanceof RDataTable)) {
			// TODO: flush frame into R if it is not already
			// check if r frame is unique name
			// otherwise update it
		}
		
		// rename the linkFrame col2 name so it is prettier
		String fuzzyColName = getCleanNewColName(rFrameVar, "Fuzzy_" + frameCol);
		script.append("names(" + linkFrame + ")[names(" + linkFrame + ") == \"col2\"] <- \"" + fuzzyColName + "\";");
		
		// we will merge on col1
		List<Map<String, String>> joinsList = new ArrayList<Map<String, String>>();
		Map<String, String> join = new HashMap<String, String>();
		join.put(frameCol, "col1");
		joinsList.add(join);
		// append the merge
		script.append(RSyntaxHelper.getMergeSyntax(rFrameVar, rFrameVar, linkFrame, "inner.join", joinsList) + ";");
		
		logger.info("Running script to append new fuzzy matches onto the frame");
		this.rJavaTranslator.runR(script.toString());
		// now we need to update the frame meta for this new column
		// i will match the data type of the existing frame
		// at the end, i will run a routine to actually set this data type as expected instead of being a string
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		SemossDataType frameColDataType = metaData.getHeaderTypeAsEnum(metaData.getUniqueNameFromAlias(frameCol));
		metaData.addProperty(rFrameVar, rFrameVar + "__" + fuzzyColName);
		metaData.setAliasToProperty(rFrameVar + "__" + fuzzyColName, fuzzyColName);
		metaData.setDataTypeToProperty(rFrameVar + "__" + fuzzyColName, frameColDataType.toString());
		metaData.setQueryStructNameToProperty(rFrameVar + "__" + fuzzyColName, "FuzzyMatching", "FuzzyMatching");
		
		// now we will just use the normal merge logic
		// just need to update to join on the new fuzzy column
		joins.get(0).setSelector(rFrameVar + "__" + fuzzyColName);
		logger.info("Running script to merge new fields onto frame");
		MergeReactor mergeReactor = new MergeReactor();
		mergeReactor.setInsight(this.insight);
		mergeReactor.setPixelPlanner(planner);
		mergeReactor.setNounStore(this.store);
		
		NounMetadata noun = mergeReactor.execute();
		// reset the data type for the fuzzy
		// to be that of the column type it was matched to
		this.rJavaTranslator.runR(RSyntaxHelper.alterColumnType(rFrameVar, fuzzyColName, frameColDataType));
		return noun;
	}
	
	////////////////////////////////////////////////////////////

	/*
	 * Getting the inputs 
	 */

	/**
	 * Get the name of the frame that contains the matches
	 * @return
	 */
	private String getMatchesFrame() {
		GenRowStruct grs = this.store.getNoun(FED_FRAME);
		if(grs != null && !grs.isEmpty()) {
			String outFrame = grs.get(0).toString().trim();
			if(!outFrame.isEmpty()) {
				return outFrame;
			}
		}
		throw new IllegalArgumentException("Must pass in the frame that contains the matches");
	}

	/**
	 * Get the propagation value to use for the matches
	 * @return
	 */
	private String getPropagation() {
		GenRowStruct grs = this.store.getNoun(PROP_MAX);
		if(grs != null && !grs.isEmpty()) {
			try {
				Double dVal = ((Number) grs.get(0)).doubleValue();
				return String.format("%.08f", dVal);
			} catch(ClassCastException e) {
				throw new IllegalArgumentException("Propagation value is not a valid number");
			}
		}
		return "0";
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
	
	/**
	 * Get the joins list
	 * @return
	 */
	private List<Join> getJoins() {
		List<Join> joins = new Vector<Join>();
		// try specific key
		{
			GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.JOINS.getKey());
			if(grs != null && !grs.isEmpty()) {
				joins = grs.getAllJoins();
				if(joins != null && !joins.isEmpty()) {
					return joins;
				}
			}
		}
		
		List<NounMetadata> joinsCur = this.curRow.getNounsOfType(PixelDataType.JOIN);
		if(joinsCur != null && !joinsCur.isEmpty()) {
			int size = joinsCur.size();
			for(int i = 0; i < size; i++) {
				joins.add( (Join) joinsCur.get(i).getValue());
			}
			
			return joins;
		}
		
		throw new IllegalArgumentException("Could not find the columns for the join");
	}
	
}
