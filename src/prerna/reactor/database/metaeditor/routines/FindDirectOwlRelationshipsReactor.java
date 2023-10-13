package prerna.reactor.database.metaeditor.routines;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.EmptyIteratorException;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.reactor.frame.FrameFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class FindDirectOwlRelationshipsReactor extends AbstractMetaEditorReactor {

	/*
	 * This reactor get the columns that match between all the tables
	 */
	
	public FindDirectOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), TABLES_FILTER};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = testDatabaseId(databaseId, false);
		List<String> filters = getTableFilters();

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		
		Map<String, List<String>> tableToCol = new TreeMap<String, List<String>>();
		// grab all the concepts
		List<String> concepts = database.getPhysicalConcepts();
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
			List<String> properties = database.getPropertyUris4PhysicalUri(cUri);
			for(String pUri : properties) {
				// load all upper case to ignore case
				columnNames.add(Utility.getClassName(pUri));
			}

			tableToCol.put(tableName, columnNames);
		}
		
		Map<String, SemossDataType> typesMap = new HashMap<String, SemossDataType>();
		typesMap.put("targetTable", SemossDataType.STRING);
		typesMap.put("targetCol", SemossDataType.STRING);
		typesMap.put("sourceTable", SemossDataType.STRING);
		typesMap.put("sourceCol", SemossDataType.STRING);
		typesMap.put("distance", SemossDataType.DOUBLE);

		AbstractRdbmsFrame frame = null;
		try {
			frame = (AbstractRdbmsFrame) FrameFactory.getFrame(this.insight, "GRID", null);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occurred trying to create frame of type GRID", e);
		}
		String tableName = frame.getName();
		
		OwlTemporalEngineMeta meta = new OwlTemporalEngineMeta();
		meta.addProperty(tableName, tableName + "__targetTable");
		meta.addProperty(tableName, tableName + "__targetCol");
		meta.addProperty(tableName, tableName + "__sourceTable");
		meta.addProperty(tableName, tableName + "__sourceCol");
		meta.addProperty(tableName, tableName + "__distance");

		meta.setDataTypeToProperty(tableName + "__targetTable", "STRING");
		meta.setDataTypeToProperty(tableName + "__targetCol", "STRING");
		meta.setDataTypeToProperty(tableName + "__sourceTable", "STRING");
		meta.setDataTypeToProperty(tableName + "__sourceCol", "STRING");
		meta.setDataTypeToProperty(tableName + "__distance", "DOUBLE");

		meta.setAliasToProperty(tableName + "__targetTable", "targetTable");
		meta.setAliasToProperty(tableName + "__targetCol", "targetCol");
		meta.setAliasToProperty(tableName + "__sourceTable", "sourceTable");
		meta.setAliasToProperty(tableName + "__sourceCol", "sourceCol");
		meta.setAliasToProperty(tableName + "__distance", "distance");

		meta.setDerivedToProperty(tableName + "__targetTable", true);
		meta.setDerivedToProperty(tableName + "__targetCol", true);
		meta.setDerivedToProperty(tableName + "__sourceTable", true);
		meta.setDerivedToProperty(tableName + "__sourceCol", true);
		meta.setDerivedToProperty(tableName + "__distance", true);
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
								String otherColumn = getIgnoreCase(targetColumnsToTest, testColumn);
								if(otherColumn != null) {
									// we have a match!!!
									// always send a distance measure of 1
									String[] headers = new String[]{"targetTable", "targetCol", "sourceTable", "sourceCol", "distance"};
									Object[] value = new Object[]{sourceTable, testColumn, targetTable, otherColumn, 1.0};
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
	
				private String getIgnoreCase(List<String> targetColumnsToTest, String testColumn) {
					for(String s : targetColumnsToTest) {
						if(s.equalsIgnoreCase(testColumn)) {
							return s;
						}
					}
					return null;
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
			frame.getBuilder().alterTableNewColumns(tableName, headers, types);
			// ignore, but need to add message
		}
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		if(emptyMessage) {
			retNoun.addAdditionalReturn(NounMetadata.getWarningNounMessage("There are no direct matches"));
		}	
		// store in insight
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(frame);
		}
		this.insight.getVarStore().put(tableName, retNoun);
		
		// return the frame
		return retNoun;
	}
}
