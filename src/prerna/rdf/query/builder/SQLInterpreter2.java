package prerna.rdf.query.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;
import prerna.ds.querystruct.QueryStruct2;
import prerna.ds.querystruct.QueryStructSelector;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class SQLInterpreter2 implements IQueryInterpreter{
	
	// core class to convert the query struct into a sql query
	QueryStruct2 qs = null;
	
	// this keeps the table aliases
	private Hashtable <String,String> aliases = new Hashtable<String,String>();
	
	// this keeps the column aliases
	// contains {tableName -> {colName -> colAliasToUse} }
//	private Hashtable<String, Hashtable<String, String>> colAlias = new Hashtable<String, Hashtable<String, String>>();
	
	// keep track of processed tables used to ensure we don't re-add tables into the from string
	private Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	
	// we will keep track of the conceptual names to physical names so we don't re-query the owl multiple times
	private transient Hashtable <String, String> conceptualConceptToPhysicalMap = new Hashtable<String, String>();
	// need to also keep track of the properties
	private transient Hashtable <String, String> conceptualPropertyToPhysicalMap = new Hashtable<String, String>();
	// need to keep track of the primary key for tables
	private transient Map<String, String> primaryKeyCache = new HashMap<String, String>();

	// we can create a statement without an engine... but everything needs to be the physical
	// we currently only use it when the engine is null, but we could use this to query on 
	// an in-memory rdbms like an H2Frame which is not an engine
	private IEngine engine; 
	
	// where the wheres are all kept
	// key is always a combination of concept and comparator
	// and the values are values
	private List<String> whereFilters = new Vector<String>();
//	private Hashtable <String, String> whereHash = new Hashtable<String, String>();

	private transient Map<String, String[]> relationshipConceptPropertiesMap = new HashMap<String, String[]>();
	
	private String selectors = "";
	private Set<String> selectorList = new HashSet<String>();
//	private String froms = "";
	private List<String[]> froms = new Vector<String[]>();
	// store the joins in the object for easy use
	private SqlJoinList relationList = new SqlJoinList();
	
	// value to determine the count of the query being executed
	private int performCount = QueryStruct.NO_COUNT;
	
	private SQLQueryUtil queryUtil = SQLQueryUtil.initialize(SQLQueryUtil.DB_TYPE.H2_DB);
	
	public SQLInterpreter2() {
		
	}

	public SQLInterpreter2(IEngine engine) {
		this.engine = engine;
		queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) engine).getDbType());
	}
	
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
		this.performCount = qs.getPerformCount();
	}
	
	public void clear() {
		this.selectors = "";
		this.froms.clear();
		this.relationList.clear();
//		this.whereHash.clear();
		this.whereFilters.clear(); 
		this.tableProcessed.clear();
	}
	
	/**
	 * Main method to invoke to take the QueryStruct to compose the appropriate SQL query
	 */
	public String composeQuery()
	{
		/*
		 * Need to create the query... 
		 * This to consider:
		 * 1) the user is going to be using the conceptual names as defined by the OWL (if present
		 * and OWL is the improved version). This has a few consequences:
		 * 1.a) when a user enters a table name, we need to determine what the primary key is
		 * 		for that table
		 * 1.b) need to consider what tables are used within joins and which are not. this will
		 * 		determine when we add it to the from clause or if the table will be defined via 
		 * 		the join 
		 */

		// we do the joins since when we get to adding the from portion of the query
		// we want to make sure that table is not used within the joins
		addJoins();
		addSelectors();
		addFilters();
		
		StringBuilder query = new StringBuilder("SELECT ");
		
		// I need to put the limit here
		
		// add the selectors
		// if this is meant to perform a count
		if(performCount == QueryStruct.COUNT_CELLS) {
			query.append(" COUNT(*) * ").append(selectors.split(",").length).append(" FROM ");
		} else if(performCount == QueryStruct.COUNT_DISTINCT_SELECTORS) { 
			query.append(" COUNT(DISTINCT ");
			String[] selectorArray = selectors.split(",");
			for(int i = 0; i < selectorArray.length; i++) {
				if(i > 0) {
					query.append(", ");
				}
				String selectorWithoutAlias = selectorArray[i].split(" AS ")[0];
				query.append(selectorWithoutAlias);
				
			}
			query.append(") AS COUNT FROM ");
		} else {
			if(this.engine != null && relationList.isEmpty()) {
				// if there are no joins, we know we are querying from a single table
				// the vast majority of the time, there shouldn't be any duplicates if
				// we are selecting all the columns
				String table = froms.get(0)[0];
				if(engine != null) {
					if( (engine.getConcepts(false).size() == 1) && (engine.getProperties4Concept(table, false).size() + 1) == selectorList.size()) {
						// plus one is for the concept itself
						// no distinct needed
						query.append(selectors).append(" FROM ");
					} else {
						query.append("DISTINCT ").append(selectors).append(" FROM ");
					}
				} else {
					// need a distinct
					query.append("DISTINCT ").append(selectors).append(" FROM ");
				}
			} else {
				// default is to use a distinct
				query.append("DISTINCT ").append(selectors).append(" FROM ");
			}
		}
		// if there is a join
		// can only have one table in from in general sql case 
		// thus, the order matters 
		// so get a good starting from table
		// we can use any of the froms that is not part of the join
		List<String> startPoints = new Vector<String>();
		if(relationList.isEmpty()) {
			String[] startPoint = froms.get(0);
			query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
			startPoints.add(startPoint[1]);
		} else {
			List<String[]> tablesToDefine = relationList.getTablesNotDefinedInJoinList();
			int i = 0;
			int size = tablesToDefine.size();
			for(; i < size; i++) {
				String[] startPoint = tablesToDefine.get(i);
				if( (i+1) == size) {
					query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
				} else {
					query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" , ");
				}
				startPoints.add(startPoint[1]);
			}
		}
		
		// add the join data
		query.append(relationList.getJoinPath(startPoints));
		
		boolean firstTime = true;
		for(String whereStatement : this.whereFilters) {
			if(firstTime) {
				firstTime = false;
				query.append(" WHERE ").append(whereStatement);
			} else {
				query.append(" AND ").append(whereStatement);
			}
		}
		
		//grab the order by and get the corresponding display name for that order by column
		query = appendGroupBy(query);
		query = appendOrderBy(query);
		
		int limit = qs.getLimit();
		int offset = qs.getOffset();
		
		query = this.queryUtil.addLimitOffsetToQuery(query, limit, offset);
		
		if(query.length() > 500) {
			System.out.println("QUERY....  " + query.substring(0,  500) + "...");
		} else {
			System.out.println("QUERY....  " + query);
		}

		return query.toString();
	}

	//////////////////////////// adding selectors //////////////////////////////////////////
	
	/**
	 * Loops through the selectors defined in the QS to add them to the selector string
	 * and considers if the table should be added to the from string
	 */
	public void addSelectors() {
		List<QueryStructSelector> selectorData = qs.getSelectors();
		
		for(QueryStructSelector selector : selectorData) {
			String table = selector.getTable();
			
			// now actually do the column add into the selector string
			addSelector(selector);
			
			// adds the from if it isn't part of a join
			if(relationList.isEmpty()) {
				addFrom(table);
			}
		}
	}
	
	private void addSelector(QueryStructSelector selector) {
		String table = selector.getTable();
		String colName = selector.getColumn();
		String alias = selector.getAlias();
		String math = selector.getMath();
		
		String selectorAddition = colName;
		// not sure how we get to the point where table would be null..
		// but this was here previously so i will just keep it I guess
		if(table != null) {
			String tableAlias = getAlias(getPhysicalTableNameFromConceptualName(table));
			// will be getting the physical column name
			String physicalColName = colName;
			// TODO: currently assuming the display name is the conceptual
			//		once we have this in the OWL, we need to add this
			
			// if engine is not null, get the info from the engine
			if(engine != null) {
				// if the colName is the primary key placeholder
				// we will go ahead and grab the primary key from the table
				if(colName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)){
					physicalColName = getPrimKey4Table(table);
					// the display name is defaulted to the table name
				} else {
					// default assumption is the info being passed is the conceptual name
					// get the physical from the conceptual
					physicalColName = getPhysicalPropertyNameFromConceptualName(table, colName);
				}
			}

			if(!math.isEmpty()) {
				selectorAddition = math + "(" + tableAlias + "." + physicalColName+ ")" + " AS " + "\""+alias+"\"";
			} else {
				selectorAddition = tableAlias + "." + physicalColName + " AS " + "\""+alias+"\"";
			}
		}
		
		if(selectors.length() == 0) {
			selectors = selectorAddition;
		} else {
			selectors = selectors + " , " + selectorAddition;
		}
		selectorList.add(selectorAddition);
	}
	
	//////////////////////////////////// end adding selectors /////////////////////////////////////
	
	
	/////////////////////////////////// adding from ////////////////////////////////////////////////
	
	
	/**
	 * Adds the form statement for each table
	 * @param conceptualTableName			The name of the table
	 */
	private void addFrom(String conceptualTableName)
	{
		String alias = getAlias(getPhysicalTableNameFromConceptualName(conceptualTableName));
		
		// need to determine if we can have multiple froms or not
		
		// we don't want to add the from table multiple times as this is invalid in sql
		if(!tableProcessed.containsKey(conceptualTableName)) {
			tableProcessed.put(conceptualTableName, "true");
			
			// we want to use the physical table name
			String physicalTableName = getPhysicalTableNameFromConceptualName(conceptualTableName);
			
			froms.add(new String[]{physicalTableName, alias});
		}
	}

	////////////////////////////////////// end adding from ///////////////////////////////////////
	
	
	////////////////////////////////////// adding joins /////////////////////////////////////////////
	
	/**
	 * Adds the joins for the query
	 */
	public void addJoins() {
		Hashtable<String, Hashtable<String, Vector>> relationsData = qs.relations;
		// loop through all the relationships
		// realize we can be joining on properties within a table
		for(String startConceptProperty : relationsData.keySet() ) {
			// the key for this object is the specific type of join to be used
			// between this instance and all the other ones
			Hashtable<String, Vector> joinMap = relationsData.get(startConceptProperty);
			for(String comparator : joinMap.keySet()) {
				Vector<String> joinColumns = joinMap.get(comparator);
				for(String endConceptProperty : joinColumns) {
					// go through and perform the actual join
					addJoin(startConceptProperty, comparator, endConceptProperty);
				}
			}
		}
	}

	/**
	 * Adds the join to the relationHash which gets added to the query in composeQuery
	 * @param fromCol					The starting column, this can be just a table
	 * 									or table__column
	 * @param thisComparator			The comparator for the type of join
	 * @param toCol						The ending column, this can be just a table
	 * 									or table__column
	 */
	private void addJoin(String fromCol, String thisComparator, String toCol) {
		SqlJoinObject thisJoin = null;
		
		// get the parts of the join
		String[] relConProp = getRelationshipConceptProperties(fromCol, toCol);
		String concept = relConProp[0];
		String property = relConProp[1];
		String toConcept = relConProp[2];
		String toProperty = relConProp[3];
		
		// the unique key for the join will be the concept and type of join
		// this is so we append the joins property
		String key = toConcept;
		
//		String queryString = "";
		String compName = thisComparator.replace(".", "  ");
		if(!relationList.doesJoinAlreadyExist(key)) {
//			queryString = compName + "  " + toConcept+ " " + getAlias(toConcept) + " ON " + getAlias(concept) + "." + property + " = " + getAlias(toConcept) + "." + toProperty;
			
			thisJoin = new SqlJoinObject(key, relationList);
			// if the concept is already defined
			// we need to get a new alias for it
			int counter = 1;
			String toConceptAlias = getAlias(toConcept);
			String usedToConceptAlias = toConceptAlias;
			while(relationList.allDefinedTableAlias().contains(usedToConceptAlias)) {
				usedToConceptAlias = toConceptAlias + "_" + counter;
			}
			// this method will determine everything required
			// the defined table and the required table
			thisJoin.setQueryString(compName, toConcept, usedToConceptAlias, toProperty, concept, getAlias(concept), property);

			// add to the list
			relationList.addSqlJoinObject(thisJoin);
		} else {
//			queryString = concept + " " + getAlias(concept) + " ON " + getAlias(concept) +  "." + property + " = " + getAlias(toConcept) + "." + toProperty;			

			thisJoin = relationList.getExistingJoin(key);
			
			// if the concept is already defined
			// we need to get a new alias for it
			int counter = 1;
			String conceptAlias = getAlias(concept);
			String usedConceptAlias = conceptAlias;
			while(relationList.allDefinedTableAlias().contains(usedConceptAlias)) {
				usedConceptAlias = conceptAlias + "_" + counter;
			}
			
			// this method will determine everything required
			// the defined table and the required table
			thisJoin.addQueryString(compName, concept, usedConceptAlias , property, toConcept, getAlias(toConcept), toProperty);
		}
	}
	
	////////////////////////////////////////// end adding joins ///////////////////////////////////////
	
	
	////////////////////////////////////////// adding filters ////////////////////////////////////////////
	
	public void addFilters()
	{
		List<Filter> filters = qs.filters.getFilters();
		for(Filter filter : filters) {
			GenRowStruct leftComp = filter.getLComparison();
			GenRowStruct rightComp = filter.getRComparison();
			String thisComparator = filter.getComparator();
			
			// loop through all the values compared to each other
			int numLeft = leftComp.size();
			int numRight = rightComp.size();
			LEFT_COMP_TYPE : for(int leftCount = 0; leftCount < numLeft; leftCount++) {
				// DIFFERENT PROCESSING BASED ON THE TYPE OF VALUE
				PkslDataTypes lCompType = leftComp.getMeta(leftCount);
				
				if(lCompType == PkslDataTypes.COLUMN) {
					// ON THE LEFT SIDE, WE HAVE A COLUMN!!!
					String left_concept_property = leftComp.get(leftCount).toString();
					if(engine != null) {
						List<String> parents = engine.getParentOfProperty2(left_concept_property);
						if(parents != null) {
							// since we can have 2 tables that have the same column
							// we need to pick one with the table that already exists
							for(String parent : parents) {
								if(aliases.containsKey(Utility.getInstanceName(parent))) {
									left_concept_property = Utility.getInstanceName(parent) + "__" + left_concept_property;
									break;
								}
							}
						}
					}
					String[] leftConProp = getConceptProperty(left_concept_property);
					String leftConcept = leftConProp[0];
					String leftProperty = leftConProp[1];
					
					String leftDataType = null;
					if(engine != null) {
						leftDataType = this.engine.getDataTypes("http://semoss.org/ontologies/Concept/" + leftProperty + "/" + leftConcept);
						// ugh, need to try if it is a property
						if(leftDataType == null) {
							leftDataType = this.engine.getDataTypes("http://semoss.org/ontologies/Relation/Contains/" + leftProperty + "/" + leftConcept);
						}
						leftDataType = leftDataType.replace("TYPE:", "");
					}
					
					// get the appropriate left hand where portion based on the table/column
					StringBuilder startFilterBuilder = new StringBuilder();
					startFilterBuilder.append(getAlias(leftConcept)).append(".").append(leftProperty);

					RIGHT_COMP_LOOP : for(int rightCount = 0; rightCount < numRight; rightCount++) {
						PkslDataTypes rCompType = rightComp.getMeta(rightCount);
						if(rCompType == PkslDataTypes.COLUMN) {
							// WE ARE COMPARING TWO COLUMNS AGAINST EACH OTHER
							String right_concept_property = rightComp.get(rightCount).toString();
							if(engine != null) {
								List<String> parents = engine.getParentOfProperty2(right_concept_property);
								if(parents != null) {
									// since we can have 2 tables that have the same column
									// we need to pick one with the table that already exists
									for(String parent : parents) {
										if(aliases.containsKey(Utility.getInstanceName(parent))) {
											right_concept_property = Utility.getInstanceName(parent) + "__" + right_concept_property;
											break;
										}
									}
								}
							}
							String[] rightConProp = getConceptProperty(right_concept_property);
							String rightConcept = rightConProp[0];
							String rightProperty = rightConProp[1];
							
							StringBuilder filterBuilder = new StringBuilder();
							filterBuilder.append(startFilterBuilder.toString());
							if(thisComparator.equals("==")) {
								filterBuilder.append(" = ");
							} else {
								filterBuilder.append(" ").append(thisComparator).append(" ");
							}
							filterBuilder.append(getAlias(rightConcept)).append(".").append(rightProperty);
							
							this.whereFilters.add(filterBuilder.toString());
						} else if(rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_STRING) {
							// WE ARE COMPARING A COLUMN AGAINST A LIST OF DECIMALS OR STRINGS
							List<Object> objects = new Vector<Object>();
							objects.add(rightComp.get(rightCount));
							String myFilterFormatted = getFormatedObject(leftDataType, objects, thisComparator);
							
							StringBuilder filterBuilder = new StringBuilder();
							filterBuilder.append(startFilterBuilder.toString());
							filterBuilder.append(" ");
							
							if(thisComparator.trim().equals("==")) {
								filterBuilder.append(" IN ");
							} else if(thisComparator.trim().equals("!=")) {
								filterBuilder.append(" NOT IN ");
							}
							
							if(thisComparator.trim().equals("==") || thisComparator.trim().equals("!=") || thisComparator.contains(" OR ")) {
								filterBuilder.append(" ( ").append(myFilterFormatted).append(" ) ");
							} else {
								filterBuilder.append(thisComparator).append(" ").append(myFilterFormatted);
							}
							
							this.whereFilters.add(filterBuilder.toString());
						}
					}
				} else if(lCompType == PkslDataTypes.CONST_DECIMAL || lCompType == PkslDataTypes.CONST_STRING) {
					// ON THE LEFT SIDE, WE HAVE SOME KIND OF CONSTANT
					List<Object> leftObjects = new Vector<Object>();
					leftObjects.add(leftComp.get(leftCount));
					
					String leftDataType = null;
					if(lCompType == PkslDataTypes.CONST_DECIMAL) {
						leftDataType = "NUMBER";
					} else {
						leftDataType = "STRING";
					}
					//TODO: NEED TO CONSIDER DATES!!!
					String leftFilterFormatted = getFormatedObject(leftDataType, leftObjects, thisComparator);
					
					RIGHT_COMP_LOOP : for(int rightCount = 0; rightCount < numRight; rightCount++) {
						PkslDataTypes rCompType = rightComp.getMeta(rightCount);
						if(rCompType == PkslDataTypes.COLUMN) {
							// WE ARE COMPARING A CONSTANT TO A COLUMN
							String right_concept_property = rightComp.get(rightCount).toString();
							if(engine != null) {
								List<String> parents = engine.getParentOfProperty2(right_concept_property);
								if(parents != null) {
									// since we can have 2 tables that have the same column
									// we need to pick one with the table that already exists
									for(String parent : parents) {
										if(aliases.containsKey(Utility.getInstanceName(parent))) {
											right_concept_property = Utility.getInstanceName(parent) + "__" + right_concept_property;
											break;
										}
									}
								}
							}
							String[] rightConProp = getConceptProperty(right_concept_property);
							String rightConcept = rightConProp[0];
							String rightProperty = rightConProp[1];
							
							StringBuilder filterBuilder = new StringBuilder();
							
							if(thisComparator.trim().equals("==")) {
								filterBuilder.append(getAlias(rightConcept)).append(".").append(rightProperty);
								filterBuilder.append(" IN ");
							} else if(thisComparator.trim().equals("!=")) {
								filterBuilder.append(getAlias(rightConcept)).append(".").append(rightProperty);
								filterBuilder.append(" NOT IN ");
							}
							
							if(thisComparator.trim().equals("==") || thisComparator.trim().equals("!=") || thisComparator.contains(" OR ")) {
								filterBuilder.append(" ( ").append(leftFilterFormatted).append(" ) ");
							} else {
								filterBuilder.append(leftFilterFormatted).append(" ").append(thisComparator).append(" ");
								filterBuilder.append(getAlias(rightConcept)).append(".").append(rightProperty);
							}
							
							this.whereFilters.add(filterBuilder.toString());
							
						} else if(rCompType == PkslDataTypes.CONST_DECIMAL || rCompType == PkslDataTypes.CONST_STRING) {
							// WE ARE COMPARING A CONSTANT TO ANOTHER CONSTANT
							// ... what is the point of this... this is a dumb thing... you are dumb
							
							List<Object> rightObjects = new Vector<Object>();
							rightObjects.add(rightComp.get(rightCount));
							
							String rightDataType = null;
							if(rCompType == PkslDataTypes.CONST_DECIMAL) {
								rightDataType = "NUMBER";
							} else {
								rightDataType = "STRING";
							}
							//TODO: NEED TO CONSIDER DATES!!!
							String rightFilterFormatted = getFormatedObject(rightDataType, rightObjects, thisComparator);
							
							StringBuilder filterBuilder = new StringBuilder();
							filterBuilder.append(leftFilterFormatted.toString());
							filterBuilder.append(" ");
							
							if(thisComparator.trim().equals("==")) {
								filterBuilder.append(" IN ");
							} else if(thisComparator.trim().equals("!=")) {
								filterBuilder.append(" NOT IN ");
							}
							
							if(thisComparator.trim().equals("==") || thisComparator.trim().equals("!=") || thisComparator.contains(" OR ")) {
								filterBuilder.append(" ( ").append(rightFilterFormatted).append(" ) ");
							} else {
								filterBuilder.append(thisComparator).append(" ").append(rightFilterFormatted);
							}
							
							this.whereFilters.add(filterBuilder.toString());
						}
					}
				}
			}
		}
	}
	
	/**
	 * This is an optimized version when we know we can get all the objects into 
	 * the proper sql query string in one go
	 * @param dataType
	 * @param objects
	 * @param comparator
	 * @return
	 */
	private String getFormatedObject(String dataType, List<Object> objects, String comparator) {
		// this will hold the sql acceptable format of the object
		StringBuilder myObj = new StringBuilder();
		
		// defining variables for looping
		int i = 0;
		int size = objects.size();
		if(size == 0) {
			return "";
		}
		// if we can get the data type from the OWL, lets just use that
		// if we dont have it, we will do type casting...
		if(dataType != null) {
			dataType = dataType.toUpperCase();
			IMetaData.DATA_TYPES type = Utility.convertStringToDataType(dataType);
			if(IMetaData.DATA_TYPES.NUMBER.equals(type)) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(IMetaData.DATA_TYPES.DATE.equals(type)) {
				String leftWrapper = null;
				String rightWrapper = null;
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = objects.get(0).toString();
				String d = Utility.getDate(val);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = Utility.getDate(val);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			}else {
				String leftWrapper = null;
				String rightWrapper = null;
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = objects.get(0).toString().replace("\"", "").replaceAll("'", "''").trim();
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString().replace("\"", "").replaceAll("'", "''").trim();
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
				}
			}
		} 
		else {
			// do it based on type casting
			// can't have mixed types
			// so only using first value
			Object object = objects.get(0);
			if(object instanceof Number) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(object instanceof java.util.Date || object instanceof java.sql.Date) {
				String leftWrapper = null;
				String rightWrapper = null;
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = objects.get(0).toString();
				String d = Utility.getDate(val);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = Utility.getDate(val);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			} else {
				String leftWrapper = null;
				String rightWrapper = null;
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = objects.get(0).toString().replace("\"", "").replaceAll("'", "''").trim();
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString().replace("\"", "").replaceAll("'", "''").trim();
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
				}
			}
		}
		
		return myObj.toString();
	}
	
	////////////////////////////////////// end adding filters ////////////////////////////////////////////

	
	//////////////////////////////////////append order by  ////////////////////////////////////////////
	
	//TODO : lets get to this once we determine other things work
	public StringBuilder appendOrderBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
//		List<QueryStructSelector> orderBy = qs.getOrderBy();
//		if(orderBy != null && !orderBy.isEmpty()) {
//			String orderByName = null;
//			for(String tableConceptualName : orderBy.keySet()) {
//				String columnConceptualName = orderBy.get(tableConceptualName);
//				if(columnConceptualName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)){
//					columnConceptualName = getPrimKey4Table(tableConceptualName);
//				} else {
//					columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
//				}
//				orderByName = getAlias(tableConceptualName) + "." + columnConceptualName;
//				break; //use first one
//			}
//			if(orderByName != null) {
//				query.append(" ORDER BY ").append(orderByName);
//			}
//		}
//		return query;
		return query;
	}
	
	//////////////////////////////////////end append order by////////////////////////////////////////////
	
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
	public StringBuilder appendGroupBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryStructSelector> groupBy = qs.getGroupBy();
		String groupByName = null;
		for(QueryStructSelector groupBySelector : groupBy) {
			String tableConceptualName = groupBySelector.getTable();
			String columnConceptualName = groupBySelector.getColumn();
			
			if(columnConceptualName.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)){
				columnConceptualName = getPrimKey4Table(tableConceptualName);
			} else {
				columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
			}
			
			if(groupByName == null) {
				groupByName = getAlias(tableConceptualName) + "." + columnConceptualName;
			} else {
				groupByName += ", "+ getAlias(tableConceptualName) + "." + columnConceptualName;
			}	
		}
		
		if(groupByName != null) {
			query.append(" GROUP BY ").append(groupByName);
		}
		return query;
	}
	
	//////////////////////////////////////end append group by////////////////////////////////////////////
	
	//////////////////////////////////// caching utility methods /////////////////////////////////////////
	
	/**
	 * Get the physical name of the 
	 * @param conceptualTableName
	 * @return
	 */
	private String getPhysicalTableNameFromConceptualName(String conceptualTableName) {
		// if engine present
		// get the appropriate physical storage name for the table
		if(engine != null) {
			// if we already have it, just grab from hash
			if(conceptualConceptToPhysicalMap.containsKey(conceptualTableName)) {
				return conceptualConceptToPhysicalMap.get(conceptualTableName);
			}
			
			// we dont have it.. so query for it
			String conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualTableName;
			String tableURI = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
			
			// table name is the instance name of the URI
			String tableName = Utility.getInstanceName(tableURI);
			
			// since we also have the URI, just store the primary key as well if we haven't already
			if(!primaryKeyCache.containsKey(conceptualTableName)) {
				// will most likely be used
				String primKey = Utility.getClassName(tableURI);
				primaryKeyCache.put(conceptualTableName, primKey);
			}
			
			// store the physical name as well in case we get it later
			conceptualConceptToPhysicalMap.put(conceptualTableName, tableName);
			return tableName;
		} else {
			// no engine is defined, just return the value
			return conceptualTableName;
		}
	}
	
	/**
	 * Get the physical name for a property
	 * @param columnConceptualName					The conceptual name of the property
	 * @return										The physical name of the property
	 */
	private String getPhysicalPropertyNameFromConceptualName(String tableConceptualName, String columnConceptualName) {
		if(engine != null) {
			// if we already have it, just grab from hash
			if(conceptualPropertyToPhysicalMap.containsKey(columnConceptualName)) {
				return conceptualPropertyToPhysicalMap.get(columnConceptualName);
			}
			
			String tablePhysicalName = getPhysicalTableNameFromConceptualName(tableConceptualName);
			
			// we don't have it... so query for it
			String propertyConceptualURI = "http://semoss.org/ontologies/Relation/Contains/" + columnConceptualName + "/" + tablePhysicalName;
			String colURI = this.engine.getPhysicalUriFromConceptualUri(propertyConceptualURI);
			String colName = null;
			
			// the class is the name of the column
			colName = Utility.getClassName(colURI);
			
			conceptualPropertyToPhysicalMap.put(columnConceptualName, colName);
			return colName;
		} else {
			// no engine is defined, just return the value
			return columnConceptualName;
		}
	}
	
	/**
	 * Get the primary key from the conceptual table name
	 * @param table						The conceptual table name
	 * @return							The physical table name
	 */
	private String getPrimKey4Table(String conceptualTableName){
		if(primaryKeyCache.containsKey(conceptualTableName)){
			return primaryKeyCache.get(conceptualTableName);
		}
		else if (engine != null){
			// we dont have it.. so query for it
			String conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualTableName;
			String tableURI = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
			
			// since we also have the URI, just store the primary key as well
			// will most likely be used
			String primKey = Utility.getClassName(tableURI);
			primaryKeyCache.put(conceptualTableName, primKey);
			return primKey;
		}
		return conceptualTableName;
	}
	
	/**
	 * Get the alias for each table name
	 * @param tableName				The table name
	 * @return						The alias for the table name
	 */
	public String getAlias(String curTableName)
	{
		// try to find if the table name has schema in it
		String [] tableTokens = curTableName.split("[.]");
	
		// now just take the latest one
		String tableName = tableTokens[tableTokens.length - 1];
		
		// alias already exists
		if(aliases.containsKey(tableName)) {
			return aliases.get(tableName);
		} else {
			boolean aliasComplete = false;
			int count = 0;
			String tryAlias = "";
			while(!aliasComplete)
			{
				if(tryAlias.length()>0){
					tryAlias+="_"; //prevent an error where you may create an alias that is a reserved word (ie, we did this with "as")
				}
				tryAlias = (tryAlias + tableName.charAt(count)).toUpperCase();
				aliasComplete = !aliases.containsValue(tryAlias);
				count++;
			}
			aliases.put(tableName, tryAlias);
			return tryAlias;
		}
	}
	
	////////////////////////////// end caching utility methods //////////////////////////////////////
	
	
	/////////////////////////////// other utility methods /////////////////////////////////////////
	
	/**
	 * Gets the 4 parts needed to define a relationship
	 * 1) the start table
	 * 2) the start tables column
	 * 3) the end table
	 * 4) the end tables column
	 * 
	 * We have 3 situations
	 * 1) If all 4 parts are defined within the fromString and toString parameters by utilizing
	 * a "__", then it just converts to the physical names and is done
	 * 2) If startTable and start column is defined but endTable/endColumn is not, it assumes the input
	 * for endString is a concept and should bind on its primary key.  This is analogous for when endTable
	 * and end column are defined but the startString is not.
	 * 3) Neither are defined, so we must use the OWL to define the relationship between the 2 tables
	 * 
	 * @param fromString				The start string defining the table/column
	 * @param toString					The end string defining the table/column
	 * @return							String[] of length 4 where the indices are
	 * 									[startTable, startCol, endTable, endCol]
	 */
	private String[] getRelationshipConceptProperties(String fromString, String toString){
		if(relationshipConceptPropertiesMap.containsKey(fromString + "__" + toString)) {
			return relationshipConceptPropertiesMap.get(fromString + "__" + toString);
		}
		
		String fromTable = null;
		String fromCol = null;
		String toTable = null;
		String toCol = null;
		
		// see if both the table name and column name are specified for the fromString
		if(fromString.contains("__")){
			String fromConceptualTable = fromString.substring(0, fromString.indexOf("__"));
			String fromConceptualColumn = fromString.substring(fromString.indexOf("__")+2);
			
			// need to make these the physical names
			if(engine != null) {
				fromTable = getPhysicalTableNameFromConceptualName(fromConceptualTable);
				fromCol = getPhysicalPropertyNameFromConceptualName(fromConceptualTable, fromConceptualColumn);
			} else {
				fromTable = fromConceptualTable;
				fromCol = fromConceptualColumn;
			}
		}
		
		// see if both the table name and column name are specified for the toString
		if(toString.contains("__")){
			String toConceptualTable = toString.substring(0, toString.indexOf("__"));
			String toConceptualColumn = toString.substring(toString.indexOf("__")+2);
			
			// need to make these the physical names
			if(engine != null) {
				toTable = getPhysicalTableNameFromConceptualName(toConceptualTable);
				toCol = getPhysicalPropertyNameFromConceptualName(toConceptualTable, toConceptualColumn);
			} else {
				toTable = toConceptualTable;
				toCol = toConceptualColumn;
			}
		}
		
		// if both have table and property defined, then we know exactly what we need to do
		// for the join... so we are done!
		
		// however, if one has a property specified and the other doesn't
		// then we want to connect the one table with col specified to the other table 
		// using the primary key of that table
		// lets try this for both cases of either toTable or fromTable not being specified 
		if(fromTable != null && toTable == null){
			String[] toConProp = getConceptProperty(toString);
			toTable = toConProp[0];
			toCol = toConProp[1];
		}
		
		else if(fromTable == null && toTable != null){
			String[] fromConProp = getConceptProperty(fromString);
			fromTable = fromConProp[0];
			fromCol = fromConProp[1];
		}
		
		// if neither has a property specified, use owl to look up foreign key relationship
		else if(engine != null && (fromCol == null && toCol == null)) // in this case neither has a property specified. time to go to owl to get fk relationship
		{
			String fromURI = null;
			String toURI = null;
			
			String fromConceptual = "http://semoss.org/ontologies/Concept/" + fromString;
			String toConceptual = "http://semoss.org/ontologies/Concept/" + toString;
			
			fromURI = this.engine.getPhysicalUriFromConceptualUri(fromConceptual);
			toURI = this.engine.getPhysicalUriFromConceptualUri(toConceptual);

			// need to figure out what the predicate is from the owl
			// also need to determine the direction of the relationship -- if it is forward or backward
			String query = "SELECT ?relationship WHERE {<" + fromURI + "> ?relationship <" + toURI + "> } ORDER BY DESC(?relationship)";
			System.out.println(query);
			TupleQueryResult res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
			String predURI = " unable to get pred from owl for " + fromURI + " and " + toURI;
			try {
				if(res.hasNext()){
					predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
				}
				else {
					query = "SELECT ?relationship WHERE {<" + toURI + "> ?relationship <" + fromURI + "> } ORDER BY DESC(?relationship)";
					System.out.println(query);
					res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
					if(res.hasNext()){
						predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
					}
				}
			} catch (QueryEvaluationException e) {
				System.out.println(predURI);
			}
			String[] predPieces = Utility.getInstanceName(predURI).split("[.]");
			if(predPieces.length == 4)
			{
				fromTable = predPieces[0];
				fromCol = predPieces[1];
				toTable = predPieces[2];
				toCol = predPieces[3];
			}
			else if(predPieces.length == 6) // this is coming in with the schema
			{
				// EHUB_CLM_SDS . EHUB_CLM_EVNT . CLM_EVNT_KEY . EHUB_CLM_SDS . EHUB_CLM_PROV_DMGRPHC . CLM_EVNT_KEY
				// [0]               [1]            [2]             [3]             [4]                    [5]
				fromTable = predPieces[0] + "." + predPieces[1];
				fromCol = predPieces[2];
				toTable = predPieces[3] + "." + predPieces[4];
				toCol = predPieces[5];
			}
		}
		
		String[] retArr = new String[]{fromTable, fromCol, toTable, toCol};
		relationshipConceptPropertiesMap.put(fromString + "__" + toString, retArr);
		
		return retArr;
	}
	
	
	/**
	 * Returns the physical concept name and property name for a given input
	 * If the input contains a "__" it returns the physical from both the 
	 * the concept and the property
	 * If the input doesn't contain a "__", get the concept and the primary key 
	 * @param concept_property				The input string
	 * @return								String[] containing the concept physical
	 * 										at index 0 and property physical at index 1
	 */
	private String[] getConceptProperty(String concept_property) {
		String conceptPhysical = null;
		String propertyPhysical = null;
		
		// if it contains a "__"
		// break the string and get the physical for both parts
		if(concept_property.contains("__")) {
			String concept = concept_property.substring(0, concept_property.indexOf("__"));
			String property = concept_property.substring(concept_property.indexOf("__")+2);
			
			conceptPhysical = getPhysicalTableNameFromConceptualName(concept);
			propertyPhysical = getPhysicalPropertyNameFromConceptualName(concept, property);
		} else {
			// if it doesn't contain a "__", then it is just a concept
			// get the physical and the prim key
			conceptPhysical = getPhysicalTableNameFromConceptualName(concept_property);
			propertyPhysical = getPrimKey4Table(concept_property);
		}
		
		return new String[]{conceptPhysical, propertyPhysical};
	}
	
	public int isPerformCount() {
		return performCount;
	}

	public void setPerformCount(int performCount) {
		this.performCount = performCount;
	}
	
	////////////////////////////////////////// end other utility methods ///////////////////////////////////////////
	
	
	///////////////////////////////////////// test method /////////////////////////////////////////////////
	
	public static void main(String[] args) {
		// load in the engine
		TestUtilityMethods.loadDIHelper();

		//TODO: put in correct path for your database
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
		
		
		QueryStruct qs = new QueryStruct();
		qs.addSelector("Title", "Title");
		qs.addSelector("Title", "Movie_Budget");

		Hashtable<String, Hashtable<String, String>> testAlias = new Hashtable<String, Hashtable<String, String>>();
		Hashtable<String, String> colHash = new Hashtable<String, String>();
		colHash.put("Movie_Budget", "Budget");
		testAlias.put("Title", colHash);
		
		SQLInterpreter qi = (SQLInterpreter) coreEngine.getQueryInterpreter();
		qi.setQueryStruct(qs);
		qi.setColAlias(testAlias);
		String query = qi.composeQuery();
		
		System.out.println(query);
	}

	@Override
	public void setQueryStruct(QueryStruct qs) {
		
	}
	
	///////////////////////////////////////// end test methods //////////////////////////////////////////////
	

}