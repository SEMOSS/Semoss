package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RClusteringAlgorithmReactor extends AbstractRFrameReactor {

	/**
	 * with specific cluster #
	 * RunClustering ( algorithm = [kmeans], multiOption = [false], instance = [ Species ] , attributes = [ "SepalLength" , "SepalWidth" , "PetalLength" , 
	 * "PetalWidth" ], numClusters = [ 3 ], uniqInstPerRow = [Yes] ) ;
	 * 
	 * with min-max range of cluster #s
	 * RunClustering ( algorithm = [kmeans], multiOption = [true], instance = [ Species ] , attributes = [ "SepalLength" , "SepalWidth" , "PetalLength" , 
	 * "PetalWidth" ],  minNumClusters = [2], maxNumClusters = [10] , uniqInstPerRow = [Yes]) ;
	 * 
	 * Input keys: 
	 * 		1. algorithm (optional) - kmeans (numerical data only), pam (numerical data only), pamGower (categorical/numerical data). 
	 * 								  default = kmeans for numerical only data or pamGower for numerical and/or categorical data 
	 * 		2. multiOption (required) - boolean (true or false)
	 * 			if true, then multiclustering (minNumClusters/maxnNumClusters can be specified)
	 * 			if false, then single clustering (numClusters can be specified)
	 * 		3. instance (required)
	 * 		4. attributes (required)
	 * 		5. numClusters (optional) - can be specified if multioption = false. default = 5
	 * 		6. minNumClusters (optional) - can be specified if multioption = true. default = 2
	 *  	7. maxnNumClusters (optional) - can be specified if multioption = true. default = 20
	 * 		8. uniqInstPerRow (optional; if not passed in, assumes no) - 
	 * 			if yes, then will treat each row in the frame as a unique instance/record; 
	 * 			if no, then will aggregate the data in the attributes columns by the instance column first
	 */
	
	private static final String MIN_NUM_CLUSTERS = "minNumClusters";
	private static final String MAX_NUM_CLUSTERS = "maxNumClusters";
	private static final String MULTI_BOOLEAN = "multiOption";
	private static final String ALGORITHM = "algorithm";
	private static final String UNIQUE_INSTANCE_PER_ROW= "uniqInstPerRow";
	
	public RClusteringAlgorithmReactor() {
		this.keysToGet = new String[]{ALGORITHM, MULTI_BOOLEAN, ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey(), 
				ReactorKeysEnum.CLUSTER_KEY.getKey(), MIN_NUM_CLUSTERS, MAX_NUM_CLUSTERS, UNIQUE_INSTANCE_PER_ROW};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "cluster" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getName();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		String tempKeyCol = "tempGenUUID99SM_" + Utility.getRandomString(6);
		StringBuilder rsb = new StringBuilder();
					
		// get first set of inputs in preparation for the first R function
		String instanceColumn = getInstanceColumn();
		List<String> attrNamesList = getColumnsList(instanceColumn);
		if (attrNamesList.contains(instanceColumn)) attrNamesList.remove(instanceColumn);
		
		// check if there are filters on the frame. if so then need to run algorithm on subsetted data and later join
		if(!frame.getFrameFilters().isEmpty()) {
			// prep the original frame by adding a temporary column, serving as row index
			addUUIDColumnToOrigFrame(dtName, meta, tempKeyCol);
			
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attrNamesList);
			selectedCols.add(instanceColumn);
			selectedCols.add(tempKeyCol);
			for(String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(frame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dtName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;
			
			//cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}
		
		// set R variables to run first R function 
		String targetDt = implicitFilter ? dtNameIF : dtName;
		String uniqInstPerRowStr = getUniqInstPerRow();
		String uniqInstPerRow_R = "uniqInstPerRow" + Utility.getRandomString(8);
		if (uniqInstPerRowStr != null && uniqInstPerRowStr.equalsIgnoreCase("TRUE")) {
			rsb.append(uniqInstPerRow_R + "<-TRUE;");
		} else {
			rsb.append(uniqInstPerRow_R + "<-FALSE;");
		}
		String instanceColumn_R = "instanceColumn" + Utility.getRandomString(8);
		rsb.append(instanceColumn_R + "<- \"" + instanceColumn + "\";");
		String attrNamesList_R = "attrNamesList" + Utility.getRandomString(8);
		rsb.append(attrNamesList_R + "<- " + RSyntaxHelper.createStringRColVec(attrNamesList.toArray())+ ";");
		String clusteringScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Clustering.R";
		clusteringScriptFilePath = clusteringScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + clusteringScriptFilePath + "\");");
		// call first R function
		int rsbLength = rsb.length();
		String scaleUniqueData_R = "scaleUniqueData" + Utility.getRandomString(8);
		rsb.append(scaleUniqueData_R + "<-scaleUniqueData(" + targetDt + "," + instanceColumn_R + "," + attrNamesList_R + "," + uniqInstPerRow_R + ");");
		this.rJavaTranslator.runR(rsb.toString());
		rsb.delete(rsbLength, rsb.length());
		int nrows = this.rJavaTranslator.getInt(scaleUniqueData_R + "$dtSubset[,.N];");
		if (nrows == 1){
			meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
			this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instanceColumn_R + "," + attrNamesList_R + "," + uniqInstPerRow_R + "," +
					dtNameIF + ",getDtClusterTable,getNewColumnNam,scaleUniqueData);gc();");
			throw new IllegalArgumentException("Instance column contains only 1 unique record.");
		}
		
		// get the rest of the inputs & set R equivalent variables in preparation for second R function
		boolean multiOption = getMultiOption();
		int numClusters = getNumClusters(keysToGet[4]);
		int minNumClusters = getNumClusters(keysToGet[5]);
		int maxNumClusters = getNumClusters(keysToGet[6]);
		String numClusters_R = "numClusters" + Utility.getRandomString(8);
		String minNumCluster_R = "minNumClusters" + Utility.getRandomString(8);
		String maxNumCluster_R = "maxNumClusters" + Utility.getRandomString(8);
		if (multiOption == false) {
			if (numClusters > 0 && numClusters >= nrows){
				meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
				this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instanceColumn_R + "," + attrNamesList_R + "," + uniqInstPerRow_R + "," +
						dtNameIF + ",getDtClusterTable,getNewColumnNam,scaleUniqueData);gc();");
				throw new IllegalArgumentException("Number of clusters requested, " + numClusters + ", should be less than the "
						+ "number of unique instances, " + nrows +".");
			}
			if (numClusters == -1){
				numClusters = (nrows <= 5 ? (nrows - 1) : 5);
			}
			rsb.append(numClusters_R + "<-" + numClusters + ";");
			rsb.append(minNumCluster_R + "<- NULL;");
			rsb.append(maxNumCluster_R + "<- NULL;");
		} else {
			if ((minNumClusters > 0 && minNumClusters >= nrows) || (maxNumClusters > 0 && maxNumClusters >= nrows)){
				meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
				this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instanceColumn_R + "," + attrNamesList_R + "," + uniqInstPerRow_R + "," +
						dtNameIF + ",getDtClusterTable,getNewColumnNam,scaleUniqueData);gc();");
				throw new IllegalArgumentException("Number of min/max clusters requested should be less than the "
						+ "number of unique instances, " + nrows +".");
			}
			if (minNumClusters == -1){
				minNumClusters = 2;
			}
			if (maxNumClusters == -1){
				maxNumClusters = (nrows <= 50 ? (nrows - 1) : 50);
			}
			rsb.append(minNumCluster_R + "<- " + minNumClusters + ";");
			rsb.append(maxNumCluster_R + "<- " + maxNumClusters + ";");
			rsb.append(numClusters_R + "<- NULL;");
		}
		boolean numericalAttrOnly = true;
		for (String attrName : attrNamesList) {
			attrName = attrName.replace(".", "_");
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + attrName);
			if (!Utility.isNumericType(dataType)) {
				numericalAttrOnly = false;
			}
		}
		String algorithm = getAlgorithm();
		String algorithm_R = "algorithm" + Utility.getRandomString(8);
		if (numericalAttrOnly == false) {
			rsb.append(algorithm_R + "<- \"pamGower\";");
		} else {
			rsb.append(algorithm_R + "<- \"" + algorithm + "\";");
		}

		// set call to second R function
		rsb.append(targetDt + " <- getDtClusterTable( " + algorithm_R + "," + scaleUniqueData_R + "," + instanceColumn_R
				+ "," + attrNamesList_R + ",numClusters=" + numClusters_R + ",minNumCluster=" + minNumCluster_R 
				+ ",maxNumCluster=" + maxNumCluster_R + ",uniqInstPerRow=" + uniqInstPerRow_R
				+ ",fullColNameList=" + RSyntaxHelper.createStringRColVec(frame.getColumnHeaders()) + ");");
				
		// execute R
		this.rJavaTranslator.runR(rsb.toString());
		
		// retrieve new columns to add to meta
		String[] updatedDfColumns = this.rJavaTranslator.getColumns(targetDt);
		
		// clean up r temp variables 
		this.rJavaTranslator.runR("rm(" + attrNamesList_R + "," + algorithm_R + "," + instanceColumn_R + "," + numClusters_R +
				"," + minNumCluster_R + "," + maxNumCluster_R + "," + uniqInstPerRow_R + "," + scaleUniqueData_R +
				",getDtClusterTable,getNewColumnName,scaleUniqueData);gc();");
		
		// get new cluster column of data
		List<String> origDfCols = new ArrayList<String>(Arrays.asList(frame.getColumnHeaders()));
		List<String> updatedDfCols = new ArrayList<String>(Arrays.asList(updatedDfColumns));
		updatedDfCols.removeAll(origDfCols);
		
		// drop the temporary column of row index from metadata
		meta.dropProperty(dtName + "__" + tempKeyCol, dtName);

		if (!updatedDfCols.isEmpty()) {
			// if implicitFilter == true, then need to join the resulting column to the whole frame (dtName var) 
			if (implicitFilter) {
				this.rJavaTranslator.runR(dtName +  "<-merge(" + dtName + ", " + dtNameIF + 
						"[,c('" + tempKeyCol + "'," + "'" + StringUtils.join(updatedDfCols,"','") + "'" +
						"), with=FALSE],by ='" + tempKeyCol + "', all.x=TRUE);" + dtName + "[," + tempKeyCol + " := NULL] ;");
			}
			this.rJavaTranslator.runR("rm(" + dtNameIF + ");gc();");
			
			// update metadata with the new column information 
			for (String newColName : updatedDfCols) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "DOUBLE");
			}
		} else {
			// no results
			this.rJavaTranslator.runR("rm(" + dtNameIF + ");gc();");
			throw new IllegalArgumentException("Selected attributes are not valid for clustering.");
		}
		
		String algName = multiOption ? "ClusterOptimization" : "Clustering";
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				algName, 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// now return this object
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(
				new NounMetadata(algName + " ran succesfully! See new \"" + updatedDfCols.get(0) + "\" column in the grid.", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	private void addUUIDColumnToOrigFrame(String frameName, OwlTemporalEngineMeta meta, String tempKeyCol){
		this.rJavaTranslator.executeEmptyR(frameName + "$" + tempKeyCol + "<- seq.int(nrow(" + frameName + "));");

		meta.addProperty(frameName, frameName + "__" + tempKeyCol);
		meta.setAliasToProperty(frameName + "__" + tempKeyCol, tempKeyCol);
		meta.setDataTypeToProperty(frameName + "__" + tempKeyCol, "INT");
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getAlgorithm() {
		GenRowStruct algorithmGrs = this.store.getNoun(keysToGet[0]);
		String algorithm;
		if (algorithmGrs != null) {
			algorithm = (String) algorithmGrs.getNoun(0).getValue();
		} else {
			// default to kmeans; if categorical data is detected in attributes cols, then will default to pamGower
			algorithm = "kmeans";
		}
		return algorithm;
	}
	
	private boolean getMultiOption() {
		GenRowStruct multiOptionGrs = this.store.getNoun(keysToGet[1]);
		if (multiOptionGrs != null) {
			return (boolean) multiOptionGrs.getNoun(0).getValue();
		} else {
			throw new IllegalArgumentException("Specify whether single or multiple clustering is being requested");
		}
	}
	
	private String getInstanceColumn() {
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[2]);
		String instanceCol = "";
		NounMetadata instanceColNoun;
		if (instanceGrs != null) {
			instanceColNoun = instanceGrs.getNoun(0);
			instanceCol = (String) instanceColNoun.getValue();
		} else {
			instanceColNoun = this.curRow.getNoun(0);
			instanceCol = (String) instanceColNoun.getValue();
		}
		return instanceCol;
	}

	
	private int getNumClusters(String key) {
		GenRowStruct numClustersGrs = this.store.getNoun(key);
		int numClusters = -1;
		if (numClustersGrs != null) {
			return(int) numClustersGrs.getNoun(0).getValue();
		}
		return numClusters; 
	}
		
	private List<String> getColumnsList(String instanceColumn) {
		// see if defined as individual key
		List<String> retList = new ArrayList<String>();
		// retList.add(this.instanceColumn);
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attr = noun.getValue().toString();
				if (!(attr.equals(instanceColumn))) {
					retList.add(attr);
				}
			}
		} else {
			// else, we assume it is the second index in the current row
			// grab lengths 2-> end columns
			int rowLength = this.curRow.size();
			for (int i = 2; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attr = colNoun.getValue().toString();
				if (!(attr.equals(instanceColumn))) {
					retList.add(attr);
				}
			}
		}
		return retList;
	}
	
	private String getUniqInstPerRow() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(UNIQUE_INSTANCE_PER_ROW);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				String value = columnGrs.get(0).toString().toUpperCase();
				if (value.equals("YES")) {
					return "TRUE";
				} else if (value.equals("NO")) {
					return "FALSE";
				}
			}
		} else {
			return "FALSE";
		}
		return null;
	}

}
