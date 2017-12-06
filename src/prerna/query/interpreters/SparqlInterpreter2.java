package prerna.query.interpreters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.hp.hpl.jena.vocabulary.XSD;

import prerna.engine.api.IEngine;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.query.querystruct.selectors.QueryMultiColMathSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Utility;

public class SparqlInterpreter2 extends AbstractQueryInterpreter {

	private static final String SEMOSS_CONCEPT_PREFIX = "http://semoss.org/ontologies/Concept";
	private static final String SEMOSS_PROPERTY_PREFIX = "http://semoss.org/ontologies/Relation/Contains";
	private static final String SEMOSS_RELATION_PREFIX = "http://semoss.org/ontologies/Relation";
	
	// string containing the return variable section of the query
	private StringBuilder selectors;
	// keep track of the selectors that are added
	// stores the variable name to the physical uri
	private Map<String, String> addedSelectors;
	// string containing the concept and property definitions
	private StringBuilder selectorWhereClause;
	// string containing the relationships
	private StringBuilder relationshipWhereClause;
	// string containing the filters
	private StringBuilder filtersWhereClause;
	// string containing the bind
	private StringBuilder bindWhereClause;
	// string containing the bindings
	private StringBuilder bindingsWhereClause;
	// string containing the group by clause
	private StringBuilder groupByClause;
	// string containing the sort by clause
	private StringBuilder sortByClause;

	// some things around optimizing the filters
	private boolean bindingsAdded = false;
	
	// map to store the concept to its properties
	// need this so we know what things to add into the engine
	private Map<String, List<String>> conceptToProps;
	// map to store the selector name to the alisa
	private Map<String, String> selectorAlias;
	
	// store the engine
	private IEngine engine;
	
	public SparqlInterpreter2() {
		
	}
	
	public SparqlInterpreter2(IEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public String composeQuery() {
		if(this.qs instanceof HardQueryStruct) {
			return ((HardQueryStruct) this.qs).getQuery();
		}
		
		String baseUri = "http://semoss.org/ontologies";
		if(this.engine != null) {
			baseUri = this.engine.getNodeBaseUri();
		}
		
		// get the return statement 
		this.conceptToProps = new HashMap<String, List<String>>();
		this.selectorAlias = new HashMap<String, String>();
		addSelectors(this.qs.getSelectors());
		
		// add the concept and property where clause
		this.addedSelectors = new HashMap<String, String>();
		addSelectorWhereClause(this.conceptToProps);
		
		// add the join where clause
		addJoins(this.qs.getRelations());
		
		// add the filters
		addFilters(this.qs.getFilters(), baseUri);
		
		// add the group bys
		addGroupClause(this.qs.getGroupBy());

		// add sort bys
		addOrderByClause(this.qs.getOrderBy());
		
		// combine the pieces and return
		StringBuilder query = new StringBuilder();
		query.append("SELECT DISTINCT ").append(this.selectors.toString());
		query.append(" WHERE { ");
		query.append(this.bindWhereClause.toString());
		query.append(this.selectorWhereClause.toString());
		query.append(this.filtersWhereClause.toString());
		query.append(this.relationshipWhereClause.toString());
		query.append("}");
		query.append(this.groupByClause.toString());
		query.append(" ");
		query.append(this.sortByClause);
		
		long limit = qs.getLimit();
		long offset = qs.getOffset();
		if(limit > 0) {
			query.append(" LIMIT ").append(limit); 
		}
		if(offset > 0) {
			query.append(" OFFSET ").append(offset); 
		}
		query.append(" ");
		query.append(this.bindingsWhereClause.toString());
		
		if(query.length() > 500) {
			logger.info("SPARQL QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.info("SPARQL QUERY....  " + query);
		}

		return query.toString();
	}

	/**
	 * Add the selector variables
	 * @param selectors2
	 */
	private void addSelectors(List<IQuerySelector> selectors) {
		this.selectors = new StringBuilder();
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			
			String var = processSelector(selector);
			if(var.equals("?" + alias)) {
				this.selectors.append(var).append(" ");
			} else {
				this.selectors.append("(").append(var).append(" AS ?").append(alias).append(") ");
			}
		}
	}
	
	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @return
	 */
	private String processSelector(IQuerySelector selector) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MATH) {
			return processMathSelector((QueryMathSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MULTI_MATH) {
			return processMultiMathSelector((QueryMultiColMathSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector);
		}
		return null;
	}

	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
	}

	private String processColumnSelector(QueryColumnSelector selector) {
		String concept = selector.getTable();
		String property = selector.getColumn();
		String alias = selector.getAlias();
		
		/*
		 * START
		 * THIS SECTION IS SO WE PROCESS THE COLUMNS THAT ARE NEEDED AND MAKE SURE
		 * THEY ARE DEFINED WITHIN THE WHERE CLAUSE
		 */
		List<String> propList = null;
		if(this.conceptToProps.containsKey(concept)) {
			propList = this.conceptToProps.get(concept);
		} else {
			propList = new Vector<String>();
			this.conceptToProps.put(concept, propList);
		}
		
		String cleanVarName = Utility.cleanVariableString(concept);
		if(!property.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
			// only store properties in the prop list
			propList.add(property);
			// also concat the property so we have unique selectors
			// its okay, we will alias this guy
			cleanVarName += "__" + Utility.cleanVariableString(property);
		}
		this.selectorAlias.put(cleanVarName, alias);
		/*
		 * END
		 * THIS SECTION IS SO WE PROCESS THE COLUMNS THAT ARE NEEDED AND MAKE SURE
		 * THEY ARE DEFINED WITHIN THE WHERE CLAUSE
		 */
		
		return "?" + cleanVarName;
	}
	
	private String processMathSelector(QueryMathSelector selector) {
		IQuerySelector innerSelector = selector.getInnerSelector();
		QueryAggregationEnum math = selector.getMath();
		boolean isDistinct = selector.isDistinct();
		
		String innerExpression = processSelector(innerSelector);
		if(isDistinct) {
			return math.getSparqlSyntax() + "(DISTINCT " + innerExpression + ")";
		} else {
			return math.getSparqlSyntax() + "(" + innerExpression + ")";
		}
	}
	
	private String processMultiMathSelector(QueryMultiColMathSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		QueryAggregationEnum math = selector.getMath();
		String colCast = selector.getColCast();
		StringBuilder expression = new StringBuilder();
		expression.append(math.getSparqlSyntax()).append("(");
		int size = innerSelectors.size();
		for(int i = 0; i< size; i++) {
			if(i > 0) {
				expression.append(",");
			}
			if(!colCast.isEmpty()) {
				expression.append(colCast).append("(").append(processSelector(innerSelectors.get(i))).append(")");

			} else {
				expression.append(processSelector(innerSelectors.get(i)));
			}
		}
		expression.append(")");
		return expression.toString();
	}

	private String processArithmeticSelector(QueryArithmeticSelector selector) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector) + " " + mathExpr + " " + processSelector(rightSelector) + ")";
	}

	/**
	 * Add the where clause triples to define the concept and properties
	 * @param conceptToProps
	 */
	private void addSelectorWhereClause(Map<String, List<String>> conceptToProps) {
		this.selectorWhereClause = new StringBuilder();
		for(String concept : conceptToProps.keySet()) {
			String nodeVarName = Utility.cleanVariableString(concept);
			addNodeSelectorTriple(nodeVarName, concept);
			
			List<String> properties = conceptToProps.get(concept);
			for(String property : properties) {
				String propVarName = nodeVarName + "__" + Utility.cleanVariableString(property);
				addNodePropertySelectorTriple(propVarName, property, nodeVarName);
			}
		}
	}
	
	/**
	 * Adds the node triple
	 * @param nodeVarName
	 * @param concept
	 */
	private String addNodeSelectorTriple(String nodeVarName, String concept) {
		if(!this.addedSelectors.containsKey(nodeVarName)) {
			String nodeUri = engine.getPhysicalUriFromConceptualUri(SEMOSS_CONCEPT_PREFIX + "/" + concept);
			// add the pattern around the concept
			this.selectorWhereClause.append("{?").append(nodeVarName).append(" <").append(RDF.TYPE).append("> <").append(nodeUri).append(">}");
			this.addedSelectors.put(concept, nodeUri);
		}
		return this.addedSelectors.get(nodeVarName);
	}
	
	/**
	 * Add a node property triple
	 * @param propVarName
	 * @param property
	 * @param nodeVarName
	 */
	private String addNodePropertySelectorTriple(String propVarName, String property, String nodeVarName) {
		if(!this.addedSelectors.containsKey(propVarName)) {
			String propUri = engine.getPhysicalUriFromConceptualUri(SEMOSS_PROPERTY_PREFIX + "/" + property);
			// add the pattern around the property
			this.selectorWhereClause.append("{?").append(nodeVarName).append(" <").append(propUri).append("> ?").append(propVarName).append("}");
		}
		return this.addedSelectors.get(propVarName);
	}
	
	private void addJoins(Map<String, Map<String, List>> relations) {
		this.relationshipWhereClause = new StringBuilder();
		// from node points to..
		for(String fromNode : relations.keySet()) {
			// ... a map of the type of join (inner, left, right, etc.) which points to...
			Map <String, List> relTypeHash = relations.get(fromNode);
			for(String joinType : relTypeHash.keySet()) {
				// a list of the downstream nodes it has with this type of join
				List<String> toNodes = relTypeHash.get(joinType);
				int numToNodes = toNodes.size();
				for(int index = 0; index < numToNodes; index++) {
					addJoin(fromNode, joinType, toNodes.get(index));
				}
			}
		}
	}

	private void addJoin(String fromNode, String joinType, String toNode) {
		String fromNodeVarName =  Utility.cleanVariableString(fromNode);
		String fromURI = addNodeSelectorTriple(fromNodeVarName, fromNode);
		String toNodeVarName =  Utility.cleanVariableString(toNode);
		String toURI = addNodeSelectorTriple(toNodeVarName, toNode);
		
		// need to figure out what the predicate is from the owl
		// also need to determine the direction of the relationship -- if it is forward or backward
		String query = "SELECT ?relationship WHERE { {<" + fromURI + "> ?relationship <" + toURI + "> } filter(?relationship != <" + SEMOSS_RELATION_PREFIX + ">) } ORDER BY DESC(?relationship)";
		TupleQueryResult res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
		String predURI = " unable to get pred from owl for " + fromURI + " and " + toURI;
		try {
			if(res.hasNext()){
				predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
			}
		} catch (QueryEvaluationException e) {
			System.out.println(predURI);
		}
		if(predURI == null) {
			throw new IllegalArgumentException("Unable to add join because we are unable to find the predicate on the owl");
		}
		
		//TODO: how do i do these types of joins? i need to use this information to update the selectors to have coalesce with them
		//TODO: how do i do these types of joins? i need to use this information to update the selectors to have coalesce with them
		//TODO: how do i do these types of joins? i need to use this information to update the selectors to have coalesce with them
		//ignoring for now... old logic doesn't account for it so i will get to this later
		
		// create a random variable name that is unique
		String randomRelVarName = Utility.getRandomString(6);
		this.relationshipWhereClause.append("{?").append(randomRelVarName).append(" <").append(RDFS.SUBPROPERTYOF).append("> <").append(predURI).append(">} ");
		if(joinType.equals("inner.join")) {
			this.relationshipWhereClause.append("{?").append(fromNodeVarName).append(" ?").append(randomRelVarName).append(" ?").append(toNodeVarName).append("} ");
		} else {
			this.relationshipWhereClause.append("OPTIONAL{?").append(fromNodeVarName).append(" ?").append(randomRelVarName).append(" ?").append(toNodeVarName).append("} ");
		}
	}

	private void addFilters(GenRowFilters grf, String baseUri) {
		this.filtersWhereClause = new StringBuilder();
		this.bindWhereClause = new StringBuilder();
		this.bindingsWhereClause = new StringBuilder();
		
		List<SimpleQueryFilter> filters = grf.getFilters();
		int numFilters = filters.size();
		for(int i = 0; i < numFilters; i++) {
			SimpleQueryFilter filter = filters.get(i);
			NounMetadata leftComp = filter.getLComparison();
			NounMetadata rightComp = filter.getRComparison();
			String thisComparator = filter.getComparator();
			
			FILTER_TYPE fType = SimpleQueryFilter.determineFilterType(filter);
			if(fType == FILTER_TYPE.COL_TO_COL) {
				addColToColFilter(leftComp, rightComp, thisComparator);
			} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
				addColToValuesFilter(leftComp, rightComp, thisComparator, baseUri);
			} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
				// same logic as above, just switch the order and reverse the comparator if it is numeric
				addColToValuesFilter(rightComp, leftComp, SimpleQueryFilter.getReverseNumericalComparator(thisComparator), baseUri);
			} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
				// WHY WOULD YOU DO THIS!!!
				addValueToValueFilter(rightComp, leftComp, thisComparator);
			}
		}
	}

	/**
	 * Add a filter of one column based on another column
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private void addColToColFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		String leftValue = leftComp.getValue().toString();
		String rightValue = rightComp.getValue().toString();

		String leftCleanVarName = defineConceptAndPropertyInFilter(leftValue);;
		String rightCleanVarName = defineConceptAndPropertyInFilter(rightValue);
		
		this.filtersWhereClause.append("FILTER(?").append(leftCleanVarName).append(" ")
				.append(thisComparator).append(" ?").append(rightCleanVarName).append(")");
	}

	/**
	 * Add a filter of one column to a set of values
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private void addColToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String baseUri) {
		String leftValue = leftComp.getValue().toString();
		String leftCleanVarName = defineConceptAndPropertyInFilter(leftValue);
		
		boolean isProp = false;
		if(leftCleanVarName.contains("__")) {
			isProp = true;
		}
		
		List<Object> rightObjects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			rightObjects.addAll( (List) rightComp.getValue());
		} else {
			rightObjects.add(rightComp.getValue());
		}
		addColToValuesFilter(leftCleanVarName, thisComparator, rightObjects, rightComp.getNounType(), isProp, baseUri);
	}

	/**
	 * Add a filter of variable name to set of values
	 * Will figure out the most optimal way to add the filter
	 * @param leftCleanVarName
	 * @param thisComparator
	 * @param rightObjects
	 * @param nounType
	 * @param isProp
	 */
	private void addColToValuesFilter(String leftCleanVarName, String thisComparator, List<Object> rightObjects, PixelDataType nounType, boolean isProp, String baseUri) {
		int numObjects = rightObjects.size();
		if(numObjects == 1 && thisComparator.equals("==")) {
			// can add a bind
			if(isProp) {
				if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
					this.bindWhereClause.append("BIND(\"").append(rightObjects.get(0)).append("\"^^<").append(XSD.xdouble).append("> AS ?").append(leftCleanVarName).append(")");
				} else {
					this.bindWhereClause.append("BIND(\"").append(rightObjects.get(0)).append("\" AS ?").append(leftCleanVarName).append(")");
				}
			} else {
				String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
				this.bindWhereClause.append("BIND(<").append(baseUri).append(concpetType).append("/")
							.append(rightObjects.get(0)).append("> AS ?").append(leftCleanVarName).append(")");
			}
		} else if(!bindingsAdded && numObjects > 1 && thisComparator.equals("==")){
			// add bindings at end
			this.bindingsWhereClause = new StringBuilder();
			this.bindingsWhereClause.append("BINDINGS ?").append(leftCleanVarName).append("{");
			if(isProp) {
				if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
					for(int i = 0; i < numObjects; i++) {
						this.bindingsWhereClause.append("(\"").append(rightObjects.get(i)).append("\"^^<").append(XSD.xdouble).append(">)");
					}
				} else {
					for(int i = 0; i < numObjects; i++) {
						this.bindingsWhereClause.append("(\"").append(rightObjects.get(i)).append("\")");
					}
				}
			} else {
				String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
				for(int i = 0; i < numObjects; i++) {
					this.bindingsWhereClause.append("(<").append(baseUri).append(concpetType).append("/").append(rightObjects.get(i)).append(">)");
				}
			}
			this.bindingsWhereClause.append("}");
			this.bindingsAdded = true;
		} else {
			// add filter
			// start the syntax
			this.filtersWhereClause.append("FILTER(");
			// modify the == comparator to work on sparql
			if(thisComparator.equals("==")) {
				thisComparator = "=";
			}
			// based on the input, set the filter information
			// if it is a like, we will use a regex filter
			// otherwise, normal filter
			if(thisComparator.equals("?like")) {
				if(isProp) {
					if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								this.filtersWhereClause.append(" || ");
							}
							this.filtersWhereClause.append("REGEX(STR(?").append(leftCleanVarName).append("), \"").append(rightObjects.get(i)).append("\", 'i')");
						}
					} else {
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								this.filtersWhereClause.append(" || ");
							}
							this.filtersWhereClause.append("REGEX(?").append(leftCleanVarName).append(", \"").append(rightObjects.get(i)).append("\", 'i')");
						}
					}
				} else {
					String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
					for(int i = 0; i < numObjects; i++) {
						if(i != 0) {
							this.filtersWhereClause.append(" || ");
						}
						this.filtersWhereClause.append("REGEX(STR(?").append(leftCleanVarName).append("), \"")
								.append(baseUri).append(concpetType).append("/.*").append(rightObjects.get(i)).append("\", 'i')");
					}
				}
			} else {
				if(isProp) {
					if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								this.filtersWhereClause.append(" || ");
							}
							this.filtersWhereClause.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ");
							this.filtersWhereClause.append("\"").append(rightObjects.get(i)).append("\"^^<").append(XSD.xdouble).append(">");
						}
					} else {
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								this.filtersWhereClause.append(" || ");
							}
							this.filtersWhereClause.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ");
							this.filtersWhereClause.append("\"").append(rightObjects.get(i)).append("\"");
						}
					}
				} else {
					String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
					for(int i = 0; i < numObjects; i++) {
						if(i != 0) {
							this.filtersWhereClause.append(" || ");
						}
						this.filtersWhereClause.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ");
						this.filtersWhereClause.append("<").append(baseUri).append(concpetType).append("/").append(rightObjects.get(i)).append(">");
					}
				}
			}
			// close the filter object
			this.filtersWhereClause.append(")");
		}
	}

	/**
	 * Add a value to value comparison
	 * @param rightComp
	 * @param leftComp
	 * @param thisComparator
	 */
	private void addValueToValueFilter(NounMetadata rightComp, NounMetadata leftComp, String thisComparator) {
		// TODO Auto-generated method stub

	}

	/**
	 * Define the concept and properties associated with a filter is not already defined to be returned
	 * @param columnValue
	 * @return
	 */
	private String defineConceptAndPropertyInFilter(String columnValue) {
		String cleanVarName = null;
		if(columnValue.contains("__")) {
			String[] split = columnValue.split("__");
			String nodeVarName = Utility.cleanVariableString(split[0]);
			cleanVarName = Utility.cleanVariableString(split[0]) + "__" + Utility.cleanVariableString(split[1]);

			// need to make sure the node is there
			addNodeSelectorTriple(nodeVarName, split[0]);
			// and the property is there
			addNodePropertySelectorTriple(cleanVarName, split[1], nodeVarName);
		} else {
			cleanVarName = Utility.cleanVariableString(columnValue);
			// need to make sure the node is there
			addNodeSelectorTriple(cleanVarName, columnValue);
		}
		
		return cleanVarName;
	}
	
	/**
	 * Generate the group by clause
	 * @param groupBy
	 */
	private void addGroupClause(List<QueryColumnSelector> groupBy) {
		this.groupByClause = new StringBuilder();
		int numGroups = groupBy.size();
		if(numGroups == 0) {
			return;
		}
		
		this.groupByClause.append(" GROUP BY");
		for(int i = 0; i < numGroups; i++) {
			QueryColumnSelector gSelect = groupBy.get(i);
			String tableName = gSelect.getTable();
			String columnName = gSelect.getColumn();
			String alias = gSelect.getAlias();
			
			if(alias != null && !alias.isEmpty()) {
				this.groupByClause.append(" ?").append(alias);
			} else {
				String varName = Utility.cleanVariableString(tableName);
				if(!columnName.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					varName += "__" + Utility.cleanVariableString(columnName);
				}
				this.groupByClause.append(" ?").append(varName);
			}
		}
	}
	
	/**
	 * Generate the order by clause
	 * @param orderBy
	 */
	private void addOrderByClause(List<QueryColumnOrderBySelector> orderBy) {
		this.sortByClause = new StringBuilder();
		int numOrders = orderBy.size();
		if(numOrders == 0) {
			return;
		}

		this.sortByClause.append(" ORDER BY");
		for(int i = 0; i < numOrders; i++) {
			QueryColumnOrderBySelector gSelect = orderBy.get(i);
			String tableName = gSelect.getTable();
			String columnName = gSelect.getColumn();
			ORDER_BY_DIRECTION sortDirection = gSelect.getSortDir();
			
			String varName = Utility.cleanVariableString(tableName);
			if(!columnName.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
				varName += "__" + Utility.cleanVariableString(columnName);
			}

			if(sortDirection == ORDER_BY_DIRECTION.ASC) {
				this.sortByClause.append(" ASC(");
			} else {
				this.sortByClause.append(" DESC(");
			}
			
			if(this.selectorAlias.containsKey(varName)) {
				this.sortByClause.append("?").append(this.selectorAlias.get(varName));
			} else {
				this.sortByClause.append("?").append(varName);
			}
			// remember to close the direction block
			this.sortByClause.append(") ");
		}
	}
}
