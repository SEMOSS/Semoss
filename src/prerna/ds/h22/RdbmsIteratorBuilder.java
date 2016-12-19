package prerna.ds.h22;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.AbstractTableDataFrame;
import prerna.ds.h2.H2Iterator;
import prerna.ds.util.RdbmsQueryBuilder;

public class RdbmsIteratorBuilder {

	private static final Logger LOGGER = LogManager.getLogger(RdbmsIteratorBuilder.class.getName());
	
	/**
	 * 
	 * @param options
	 *            - options needed to build the iterator
	 * @return
	 * 
	 * 		returns an iterator based on the options parameter
	 */
	public Iterator buildIterator(H2IteratorOptions options) {
		String tableName = options.getReadTable();
		String sortDir = options.getSortDirection();
		Integer limit = options.getLimit();
		Integer offset = options.getOffset();
		String sortBy = options.getSortColumn();
		List<String> selectors = options.getSelectors();
		
		String selectPortion = RdbmsQueryBuilder.makeSelect(tableName, selectors, options.selectDistinct());
		StringBuilder selectQuery = new StringBuilder(selectPortion);
		appendFilters(selectQuery, options);

		if (sortBy != null) {
			if (!options.getTableMeta().isJoined()) {
				//H2Builder.addColumnIndex(options.getTableMeta(), sortBy);
			}
			selectQuery.append(" ORDER BY " + sortBy);
		}
		if (limit > 0) {
			selectQuery.append(" LIMIT " + limit);
		}
		if (offset > 0) {
			selectQuery.append(" OFFSET " + offset);
		}
		
		return createIterator(selectQuery.toString(), options);
	}
	
	private Iterator createIterator(String generatedSelectQuery, H2IteratorOptions options) {
		
		try {
			ResultSet rs;
			long startTime = System.currentTimeMillis();
			System.out.println("TABLE NAME IS: " + options.getReadTable());
			System.out.println("RUNNING QUERY : " + generatedSelectQuery);
			
			rs = options.getConnection().createStatement().executeQuery(generatedSelectQuery);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("RAN QUERY:  "+ generatedSelectQuery);
			LOGGER.info("CONSTRUCTED ITERATOR: " + (endTime - startTime) + " ms");
			
			if(options.useSingleColumn()) {
				return new H2ColumnIterator(rs);
			} else {
				return new H2Iterator(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;//should return an empty iterator
	}
	
	private void appendFilters(StringBuilder selectQuery, H2IteratorOptions options) {
		// temporary filters to apply only to this iterator
		Map<String, List<Object>> temporalBindings = options.getBindings();
		Map<String, AbstractTableDataFrame.Comparator> compHash = new HashMap<String, AbstractTableDataFrame.Comparator>();
		for (String key : temporalBindings.keySet()) {
			compHash.put(key, AbstractTableDataFrame.Comparator.EQUAL);
		}

		// create a new filter substring and add/replace old filter substring
		String filters = "";
		if(options.useFilters()) {
			filters = options.getFilters().makeFilterSubQuery();
		}
		String bindings = RdbmsQueryBuilder.makeFilterSubQuery(temporalBindings, compHash); // default comparator is equals
		if (bindings.length() > 0) {
			if (filters.contains(" WHERE ")) {
				bindings = bindings.replaceFirst(" WHERE ", "");
			}
		}
		
		selectQuery.append(filters + bindings);
	}
}
