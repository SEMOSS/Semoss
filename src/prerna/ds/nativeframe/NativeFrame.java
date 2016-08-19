package prerna.ds.nativeframe;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerMetaData;
import prerna.sablecc.PKQLEnum;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

public class NativeFrame extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(NativeFrame.class.getName());
	NativeFrameBuilder builder;

	public NativeFrame() {
		this.metaData = new TinkerMetaData();
		this.builder = new NativeFrameBuilder();
	}

	// added as a path to get connection url for current dataframe
	public NativeFrameBuilder getBuilder() {
		return this.builder;
	}

	public void setConnection(String engineName) {
		this.builder.setConnection(engineName);
		Connection connection = this.builder.getConnection();
		
		if (connection != null) {
			try {
				// working with Mairiadb
				Statement stmt = connection.createStatement();
				String query = "select * from director";
				ResultSet rs = stmt.executeQuery(query);
				while (rs.next()) {
				 System.out.print(rs.toString());
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
	}

	@Override
	public void addRow(Object[] cleanCells, Object[] rawCells, String[] headers) {
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Object[] rawValues, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		return null;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		return null;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData, Map<String, Object> options) {
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, boolean getRawData, Map<String, Object> options) {
		return null;
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean getRawData, boolean iterateAll) {
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		return null;
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {

	}

	@Override
	public void unfilter(String columnHeader) {

	}

	@Override
	public void unfilter() {

	}

	@Override
	public void removeColumn(String columnHeader) {

	}

	@Override
	public Object[] getFilterModel() {
		return null;
	}

	@Override
	public void save(String fileName) {

	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {

	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow, Map<String, Object> rawRow) {

	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToValMap) {

	}

	@Override
	public Map<String, Object[]> getFilterTransformationValues() {
		return null;
	}

	@Override
	public void filter(String columnHeader,	Map<String, List<Object>> filterValues) {

	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {

	}

	@Override
	public String getDataMakerName() {
		return null;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.DATA_CONNECTDB, "prerna.sablecc.DataConnectDBReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.NativeApiReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.NativeImportDataReactor");

		return reactorNames;
	}

}
