package prerna.sablecc2.reactor.qs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class QueryInsertReactor extends AbstractReactor {
	
	private NounMetadata qStruct = null;
	
	@Override
	public NounMetadata execute() {
		if(qStruct == null) {
			qStruct = getQueryStruct();
		}
		
		AbstractQueryStruct qs = (AbstractQueryStruct) qStruct.getValue();
		IEngine engine = null;
		ITableDataFrame frame = null;
		
		if(qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if(qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
				engine = qs.retrieveQueryStructEngine();
				if(!(engine instanceof RDBMSNativeEngine)) {
					throw new IllegalArgumentException("Insert query only works for rdbms databases");
				}
			} else if(qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
				frame = qs.getFrame();
				if(!(frame instanceof H2Frame)) {
					throw new IllegalArgumentException("Insert query only works for sql frames");
				}
			}
		} else {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}
		
		String userId = null;
		if(this.securityEnabled()) {
			User user = this.insight.getUser();
			userId = user.getAccessToken(user.getLogins().get(0)).getId();
		} else {
			userId = "require login for user";
		}
		
		StringBuilder prefixSb = new StringBuilder("INSERT INTO ");
		
		GenRowStruct col_grs = this.store.getNoun("into");
		GenRowStruct val_grs = this.store.getNoun("values");
		
		List<IQuerySelector> selectors = new Vector<IQuerySelector>();
		for(int i = 0; i < col_grs.size(); i++) {
			String s = col_grs.get(i).toString();
			selectors.add(new QueryColumnSelector (s));
		}
		
		// Insert table name
		QueryColumnSelector t = (QueryColumnSelector) selectors.get(0);
		prefixSb.append(t.getTable()).append(" (");
		
		// Insert columns
		for(int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector c = (QueryColumnSelector) selectors.get(i);
			if(i > 0) {
				prefixSb.append(", ");
			}
			if(c.getColumn().equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				prefixSb.append(getPrimKey(engine, c.getTable()));
			} else {
				prefixSb.append(c.getColumn());
			}
		}
		prefixSb.append(") VALUES (");
		
		String initial = prefixSb.toString();
		
		List<Object[]> valueCombinations = flattenCombinations(val_grs);
		for(Object[] values : valueCombinations) {
			StringBuilder valuesSb = new StringBuilder();
			// Insert values
			for(int i = 0; i < values.length; i++) {
				if(i == values.length - 1) {
					if(values[i] instanceof String) {
						valuesSb.append("'" + values[i] + "'");
					}
					else {
						valuesSb.append(values[i]);
					}
				}
				else {
					if(values[i] instanceof String) {
						valuesSb.append("'" + values[i] + "', ");
					}
					else {
						valuesSb.append(values[i] + ", ");
					}
				}
			}
			valuesSb.append(")");
			
			String query = initial + valuesSb.toString();
			System.out.println("SQL QUERY...." + query);
			if(qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE) {
				engine.insertData(query);
				AuditDatabase audit = ((RDBMSNativeEngine) engine).generateAudit();
				audit.auditInsertQuery(selectors, Arrays.asList(values), userId);
			} else {
				try {
					((H2Frame) frame).getBuilder().runQuery(query);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
	}
	
	private NounMetadata getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		NounMetadata queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return object;
		} 
		return queryStruct;
	}
	
	private List<Object[]> flattenCombinations(GenRowStruct val_grs) {
		List<Object[]> combinations = new Vector<Object[]>();
		
		Map<Integer, Integer> currIndexMap = new HashMap<Integer, Integer>();
		
		int numInputs = val_grs.size();
		boolean moreCombinations = true;
		while(moreCombinations) {
			Object[] row = new Object[numInputs];
			for(int i = 0; i < numInputs; i++) {
				
				Object thisValue = null;
				Object result = val_grs.get(i);
				if(result instanceof List) {
					// if we know which index to grab, lets just grab it
					if(currIndexMap.containsKey(new Integer(i))) {
						Integer indexToGrab = currIndexMap.get(new Integer(i));
						thisValue = ((List) result).get(indexToGrab);
					} else {
						thisValue = ((List) result).get(0);
						currIndexMap.put(new Integer(i), new Integer(0));
					}
				} else {
					thisValue = result;
				}
				
				// set the value into the current row
				row[i] = thisValue;
			}
			combinations.add(row);
			
			
			// now we need to know if we should update curr index map
			// or if we are done
			boolean loopAgain = false;
			UPDATE_LOOP : for(int i = numInputs-1; i >=0 ; i--) {
				// we start at the last list
				// and see if the current index is at the end
				Object result = val_grs.get(i);
				if(result instanceof List) {
					Integer indexToGrab = currIndexMap.get(new Integer(i));
					int numIndicesToGrab = ((List) result).size();
					if( (indexToGrab + 1) == numIndicesToGrab) {
						// we are have iterated through all of this guy
						// so let us reset him
						// BUT, this doesn't mean we know we need to loop again
						// i am just preparing for the case where a list above requires us to start
						// and loop through all the last pieces
						currIndexMap.put(new Integer(i), new Integer(0));
					} else {
						// we have not looped through everything in this list
						// we need to loop again
						// after i increase the index to grab
						currIndexMap.put(new Integer(i), new Integer(indexToGrab.intValue()+1));
						loopAgain = true;
						break UPDATE_LOOP;
					}
				}
			}
			
			moreCombinations = loopAgain;
		}
		
		return combinations;
	}
	
	/**
	 * Get the primary key for a table
	 * @param engine
	 * @param tableName
	 * @return
	 */
	private String getPrimKey(IEngine engine, String tableName) {
		String conceptualURI = "http://semoss.org/ontologies/Concept/" + tableName;
		String tableURI = engine.getPhysicalUriFromConceptualUri(conceptualURI);
		
		// since we also have the URI, just store the primary key as well
		// will most likely be used
		return Utility.getClassName(tableURI);
	}
	
	public static void main(String[] args) {
		GenRowStruct grs = new GenRowStruct();
		grs.add(new NounMetadata(1, PixelDataType.CONST_INT));
		List<Object> l1 = new Vector<Object>();
		l1.add("a");
		l1.add("b");
		l1.add("c");
		grs.add(new NounMetadata(l1, PixelDataType.VECTOR));
		List<Object> l2 = new Vector<Object>();
		l2.add("d");
		l2.add("e");
		grs.add(new NounMetadata(l2, PixelDataType.VECTOR));
		List<Object> l3 = new Vector<Object>();
		l3.add("x");
		l3.add("y");
		l3.add("z");
		grs.add(new NounMetadata(l3, PixelDataType.VECTOR));
		
		QueryInsertReactor qir = new QueryInsertReactor();
		List<Object[]> combinations = qir.flattenCombinations(grs);
		
		for(int i = 0; i < combinations.size(); i++) {
			System.out.println(Arrays.toString(combinations.get(i)));
		}
	}
}
