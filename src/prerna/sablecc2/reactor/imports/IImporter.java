package prerna.sablecc2.reactor.imports;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.om.Insight;
import prerna.sablecc2.om.Join;

public interface IImporter {

	/**
	 * Insert new data into the frame
	 */
	void insertData();
	
	/**
	 * Insert new data with the given meta
	 */
	void insertData(OwlTemporalEngineMeta metaData);
	
	/**
	 * Merge new data with existing frame data
	 */
	ITableDataFrame mergeData(List<Join> joins);

	/**
	 * Set the insight for the importer
	 * @param in
	 */
	void setInsight(Insight in);
	
}
