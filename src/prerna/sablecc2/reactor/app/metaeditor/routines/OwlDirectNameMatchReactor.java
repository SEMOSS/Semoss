package prerna.sablecc2.reactor.app.metaeditor.routines;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.EmptyIteratorException;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.Utility;

public class OwlDirectNameMatchReactor extends AbstractMetaEditorReactor {

	/*
	 * This reactor get the columns that match between all the tables
	 */
	
	public OwlDirectNameMatchReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), TABLES_FILTER};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = getAppId(appId, false);
		List<String> filters = getTableFilters();

		IEngine app = Utility.getEngine(appId);
		
		Map<String, List<String>> tableToCol = new TreeMap<String, List<String>>();
		// grab all the concepts
		Vector<String> concepts = app.getConcepts(false);
		for(String cUri : concepts) {
			String tableName = Utility.getInstanceName(cUri);

			// if this is empty
			// no filters have been defined
			if(!filters.isEmpty()) {
				// filters have been defined
				// now if the table isn't included
				// ignore it
				if(!filters.contains(tableName)) {
					continue;
				}
			}

			List<String> columnNames = new Vector<String>();

			// grab all the properties
			List<String> properties = app.getProperties4Concept(cUri, false);
			for(String pUri : properties) {
				// load all upper case to ignore case
				columnNames.add(Utility.getClassName(pUri));
			}

			// add the prim column as well
			columnNames.add(Utility.getClassName(cUri));

			tableToCol.put(tableName, columnNames);
		}
		
		Map<String, SemossDataType> typesMap = new HashMap<String, SemossDataType>();
		typesMap.put("targetTable", SemossDataType.STRING);
		typesMap.put("targetCol", SemossDataType.STRING);
		typesMap.put("sourceTable", SemossDataType.STRING);
		typesMap.put("sourceCol", SemossDataType.STRING);
		typesMap.put("distance", SemossDataType.DOUBLE);

		H2Frame frame = new H2Frame();
		
		OwlTemporalEngineMeta meta = new OwlTemporalEngineMeta();
		meta.addProperty(frame.getTableName(), frame.getTableName() + "__targetTable");
		meta.addProperty(frame.getTableName(), frame.getTableName() + "__targetCol");
		meta.addProperty(frame.getTableName(), frame.getTableName() + "__sourceTable");
		meta.addProperty(frame.getTableName(), frame.getTableName() + "__sourceCol");
		meta.addProperty(frame.getTableName(), frame.getTableName() + "__distance");

		meta.setDataTypeToProperty(frame.getTableName() + "__targetTable", "STRING");
		meta.setDataTypeToProperty(frame.getTableName() + "__targetCol", "STRING");
		meta.setDataTypeToProperty(frame.getTableName() + "__sourceTable", "STRING");
		meta.setDataTypeToProperty(frame.getTableName() + "__sourceCol", "STRING");
		meta.setDataTypeToProperty(frame.getTableName() + "__distance", "DOUBLE");

		meta.setAliasToProperty(frame.getTableName() + "__targetTable", "targetTable");
		meta.setAliasToProperty(frame.getTableName() + "__targetCol", "targetCol");
		meta.setAliasToProperty(frame.getTableName() + "__sourceTable", "sourceTable");
		meta.setAliasToProperty(frame.getTableName() + "__sourceCol", "sourceCol");
		meta.setAliasToProperty(frame.getTableName() + "__distance", "distance");

		meta.setDerivedToProperty(frame.getTableName() + "__targetTable", true);
		meta.setDerivedToProperty(frame.getTableName() + "__targetCol", true);
		meta.setDerivedToProperty(frame.getTableName() + "__sourceTable", true);
		meta.setDerivedToProperty(frame.getTableName() + "__sourceCol", true);
		meta.setDerivedToProperty(frame.getTableName() + "__distance", true);
		frame.setMetaData(meta);

		boolean emptyMessage = false;
		try {
			frame.addRowsViaIterator(new Iterator<IHeadersDataRow>() {
				
				String[] tables = tableToCol.keySet().toArray(new String[]{});
				int size = tables.length;
				
				int sourceTableIndex = 0;
				int targetTableIndex = 1;
				List<IHeadersDataRow> rows = new Vector<IHeadersDataRow>();
				
				@Override
				public IHeadersDataRow next() {
					return rows.remove(0);
				}
				
				@Override
				public boolean hasNext() {
					// if we have values stored up
					// return true
					if(!rows.isEmpty()) {
						return true;
					}
					
					// we have nothing stored up
					// try comparing the next set of tables
					for(; sourceTableIndex < size; sourceTableIndex++) {
						
						String sourceTable = tables[sourceTableIndex];
						List<String> sourceColumnsToTest = tableToCol.get(sourceTable);
						
						for(; targetTableIndex < size; targetTableIndex++) {
							
							String targetTable = tables[targetTableIndex];
							List<String> targetColumnsToTest = tableToCol.get(targetTable);
	
							for(int cIndex = 0; cIndex < sourceColumnsToTest.size(); cIndex++) {
								String testColumn = sourceColumnsToTest.get(cIndex);
								if(containsIgnoreCase(targetColumnsToTest, testColumn)) {
									// we have a match!!!
									// always send a distance measure of 1
									String[] headers = new String[]{"targetTable", "targetCol", "sourceTable", "sourceCol", "distance"};
									Object[] value = new Object[]{sourceTable, testColumn, targetTable, testColumn, 1.0};
									rows.add(new HeadersDataRow(headers, value));
								}
							}
						}
						
						// i will start to return if we have values in rows
						if(!rows.isEmpty()) {
							return true;
						} else {
							// we got to the last target table
							// need to reset the target index
							targetTableIndex = sourceTableIndex+2;
						}
					}
	
					return false;
				}
	
				private boolean containsIgnoreCase(List<String> targetColumnsToTest, String testColumn) {
					for(String s : targetColumnsToTest) {
						if(s.equalsIgnoreCase(testColumn)) {
							return true;
						}
					}
					return false;
				}
				
			}, typesMap);
		} catch(EmptyIteratorException e) {
			emptyMessage = true;
			int size = typesMap.size();
			String[] headers = new String[size];
			String[] types = new String[size];
			int i = 0;
			for(String header : typesMap.keySet()) {
				headers[i] = header;
				types[i] = typesMap.get(header).toString();
				i++;
			}
			frame.getBuilder().alterTableNewColumns(frame.getTableName(), headers, types);
			// ignore, but need to add message
		}
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, 
				PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		if(emptyMessage) {
			retNoun.addAdditionalReturn(new NounMetadata("There are no direct matches", PixelDataType.CONST_STRING, PixelOperationType.WARNING));
		}
		// store in insight
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(frame);
		}
		this.insight.getVarStore().put(frame.getTableName(), retNoun);
		
		// return the frame
		return retNoun;
	}
}
