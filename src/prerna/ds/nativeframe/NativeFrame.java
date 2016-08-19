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

	private static final Logger LOGGER = LogManager.getLogger(NativeFrame.class
			.getName());
	NativeFrameBuilder builder;

	public NativeFrame() {
		this.metaData = new TinkerMetaData();
		this.builder = new NativeFrameBuilder();
	}

	// added as a path to get connection url for current dataframe
	public NativeFrameBuilder getBuilder() {
		return this.builder;// = new NativeFrameBuilder();
	}

	public void setConnection(String engineName) {
		this.builder.setConnection(engineName);
		Connection connection = this.builder.getConnection();
//		if (connection != null) {
//			System.out.println("SUCCESS");
//		}
		
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
		// TODO Auto-generated method stub

	}

	@Override
	public void addRow(Object[] cleanCells, Object[] rawCells, String[] headers) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addRelationship(String[] headers, Object[] values,
			Object[] rawValues, Map<Integer, Set<Integer>> cardinality,
			Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub

	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable,
			String colNameInJoiningTable, double confidenceThreshold,
			IMatcher routine) {
		// TODO Auto-generated method stub

	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object[]> iterator(boolean getRawData,
			Map<String, Object> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader,
			boolean getRawData, Map<String, Object> options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader,
			boolean getRawData, boolean iterateAll) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unfilter(String columnHeader) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unfilter() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeColumn(String columnHeader) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object[] getFilterModel() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void save(String fileName) {
		// TODO Auto-generated method stub

	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow,
			Map<String, Object> rawRow) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow,
			Map<String, Object> rawRow) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData,
			Map<String, Object> rowRawData, Map<String, Set<String>> edgeHash,
			Map<String, String> logicalToValMap) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object[]> getFilterTransformationValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void filter(String columnHeader,
			Map<String, List<Object>> filterValues) {
		// TODO Auto-generated method stub

	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getDataMakerName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.DATA_CONNECTDB,
				"prerna.sablecc.DataConnectDBReactor");
		reactorNames
				.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");

		return reactorNames;
	}

}
