package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RClusteringAlgorithmRReactor extends AbstractRFrameReactor {

	/**
	 * available algorithm options: kmeans (numerical data only), pam (numerical data only), pamGower (categorical/numerical data)
	 * 
	 * with specific cluster #
	 * RunClusteringR ( algorithm = [kmeans], multiOption = [false], instance = [ Species ] , attributes = [ "SepalLength" , "SepalWidth" , "PetalLength" , 
	 * "PetalWidth" ], numClusters = [ 3 ] ) ;
	 * 
	 * with min-max range of cluster #s
	 * RunClusteringR ( algorithm = [kmeans], multiOption = [true], instance = [ Species ] , attributes = [ "SepalLength" , "SepalWidth" , "PetalLength" , 
	 * "PetalWidth" ],  minNumClusters = [2], maxNumClusters = [10] ) ;
	 */
	
	private static final String MIN_NUM_CLUSTERS = "minNumClusters";
	private static final String MAX_NUM_CLUSTERS = "maxNumClusters";
	private static final String MULTI_BOOLEAN = "multiOption";
	private static final String ALGORITHM = "algorithm";

	private String algorithm;
	private Boolean multiOption;
	private String instanceColumn;
	private List<String> attrNamesList;
	private int numClusters = -1;
	private int minNumClusters;
	private int maxNumClusters;
	
	public RClusteringAlgorithmRReactor() {
		this.keysToGet = new String[]{ALGORITHM, MULTI_BOOLEAN, ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey(), 
				ReactorKeysEnum.CLUSTER_KEY.getKey(), MIN_NUM_CLUSTERS, MAX_NUM_CLUSTERS};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "cluster", "stats" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dfName = frame.getTableName();
		StringBuilder rsb = new StringBuilder();		
		int nrows = frame.getNumRows(dfName);
		
		// get inputs & set R equivalent variables
		this.instanceColumn = getInstanceColumn();
		String instanceColumn_R = "instanceColumn" + Utility.getRandomString(8);
		rsb.append(instanceColumn_R + "<- \"" + this.instanceColumn + "\";");
		
		this.multiOption = getMultiOption();
		String numClusters_R = "numClusters" + Utility.getRandomString(8);
		String minNumCluster_R = "minNumClusters" + Utility.getRandomString(8);
		String maxNumCluster_R = "maxNumClusters" + Utility.getRandomString(8);
		if (this.multiOption == false) {
			this.numClusters = getNumClusters(keysToGet[4], nrows);
			rsb.append(numClusters_R + "<-" + this.numClusters + ";");
			rsb.append(minNumCluster_R + "<- NULL;");
			rsb.append(maxNumCluster_R + "<- NULL;");
		} else {
			this.minNumClusters = getNumClusters(keysToGet[5], nrows);
			this.maxNumClusters = getNumClusters(keysToGet[6], nrows);
			rsb.append(minNumCluster_R + "<- " + this.minNumClusters + ";");
			rsb.append(maxNumCluster_R + "<- " + this.maxNumClusters + ";");
			rsb.append(numClusters_R + "<- NULL;");
		}
		
		this.attrNamesList = getColumns();
		if (attrNamesList.contains(this.instanceColumn)) attrNamesList.remove(this.instanceColumn);
		Boolean numericalAttrOnly = true;
		//// check if there are any date-type values in selected attributes list and segregate those to separate list
		List<String> attrNamesListDate = new ArrayList<String>();
		for (String attrName : this.attrNamesList) {
			attrName = attrName.replace(".", "_");
			String dataType = meta.getHeaderTypeAsString(dfName + "__" + attrName);
			if (Utility.isDateType(dataType)) {
				attrNamesListDate.add(attrName);
				int index = this.attrNamesList.indexOf(attrName);
				attrNamesList.remove(index);
			} else if (!Utility.isNumericType(dataType)) {
				numericalAttrOnly = false;
			}
		}
		String attrNamesList_R = "attrNamesList" + Utility.getRandomString(8);
		rsb.append(attrNamesList_R + "<- " + RSyntaxHelper.createStringRColVec(attrNamesList.toArray())+ ";");
		String attrNamesListDate_R = "attrNamesDateCol" + Utility.getRandomString(8);
		if (attrNamesListDate.size() > 0) {
			rsb.append(attrNamesListDate_R + "<- " + RSyntaxHelper.createStringRColVec(attrNamesListDate.toArray())	+ ";");
		} else {
			rsb.append(attrNamesListDate_R + "<- NULL;");
		}
		
		this.algorithm = getAlgorithm();
		String algorithm_R = "algorithm" + Utility.getRandomString(8);
		if (numericalAttrOnly == false) {
			rsb.append(algorithm_R + "<- \"pamGower\";");
		} else rsb.append(algorithm_R + "<- \"" + this.algorithm + "\";");


		// clustering r script
		String clusteringScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Clustering.R";
		clusteringScriptFilePath = clusteringScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + clusteringScriptFilePath + "\");");
				
		// set call to R function
		rsb.append(dfName + " <- getDtClusterTable( " + algorithm_R + "," + dfName + "," + instanceColumn_R + ","
				+ attrNamesList_R + ",dateAttrColList=" + attrNamesListDate_R + ",numClusters=" + numClusters_R + 
				",minNumCluster=" + minNumCluster_R + ",maxNumCluster=" + maxNumCluster_R + ");");
				
		// execute R
		System.out.println(rsb.toString());
		this.rJavaTranslator.runR(rsb.toString());

		// add new columns to meta
		// update the metadata to include this new column
		String[] updatedDfColumns = this.rJavaTranslator.getColumns(dfName);
		
		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + attrNamesList_R + ");");
		cleanUpScript.append("rm(" + attrNamesListDate_R + ");");
		cleanUpScript.append("rm(" + algorithm_R + ");");
		cleanUpScript.append("rm(" + numClusters_R + ");");
		cleanUpScript.append("rm(" + minNumCluster_R + ");");
		cleanUpScript.append("rm(" + maxNumCluster_R + ");");
		cleanUpScript.append("rm(" + instanceColumn_R + ");");
		cleanUpScript.append("rm(getDtClusterTable);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		Collection<String> origDfCols = new ArrayList<String>(Arrays.asList(frame.getColumnHeaders()));
		Collection<String> updatedDfCols = new ArrayList<String>(Arrays.asList(updatedDfColumns));
		updatedDfCols.removeAll(origDfCols);
		if (!updatedDfCols.isEmpty()) {
			for (String newColName : updatedDfCols) {
				meta.addProperty(dfName, dfName + "__" + newColName);
				meta.setAliasToProperty(dfName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dfName + "__" + newColName, "DOUBLE");
			}
		} else {
			// no results
			throw new IllegalArgumentException("Selected attributes are not valid for clustering. "
					+ "Check that attribute columns are numeric or date types and do not contain nulls.");
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
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
	
	private Boolean getMultiOption() {
		GenRowStruct multiOptionGrs = this.store.getNoun(keysToGet[1]);
		Boolean multiOption;
		if (multiOptionGrs != null) {
			multiOption = (Boolean) multiOptionGrs.getNoun(0).getValue();
		} else {
			throw new IllegalArgumentException("Specify whether single or multiple clustering is being requested");
		}
		return multiOption;
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

	private int getNumClusters(String key, int nrows) {
		GenRowStruct numClustersGrs = this.store.getNoun(key);
		int numClusters = -1;
		NounMetadata numClustersNoun;
		if (numClustersGrs != null) {
			numClusters = (int) numClustersGrs.getNoun(0).getValue();
			if (numClusters > nrows) {
				throw new IllegalArgumentException("Number of clusters requested exceeds the number of data records");
			}
		} else {
			if (key == keysToGet[3]) numClusters = 5;
			if (key == keysToGet[4]) numClusters = 2;
			if (key == keysToGet[5]) numClusters = 50;
		}
		return numClusters;
	}
		
	private List<String> getColumns() {
		// see if defined as individual key
		List<String> retList = new ArrayList<String>();
		// retList.add(this.instanceColumn);
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attr = noun.getValue().toString();
				if (!(attr.equals(this.instanceColumn))) {
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
				if (!(attr.equals(this.instanceColumn))) {
					retList.add(attr);
				}
			}
		}
		return retList;
	}

}
