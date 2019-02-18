package prerna.query.interpreters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.hp.hpl.jena.vocabulary.XSD;

import prerna.engine.api.IEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AbstractListFilter;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter.QUERY_FILTER_TYPE;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SparqlInterpreter extends AbstractQueryInterpreter {

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
	
	// map to store the selector name to the alisa
	private Map<String, String> selectorAlias;
	
	// filtered columns
	private Set<String> qsFilteredColumns;
	private Set<String> qsFilteredColumnsToNull;
	// store the engine
	private IEngine engine;
	
	public SparqlInterpreter() {
		
	}
	
	public SparqlInterpreter(IEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public String composeQuery() {
		if(this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct) this.qs).getQuery();
		}
		
		String baseUri = "http://semoss.org/ontologies";
		if(this.engine != null) {
			baseUri = this.engine.getNodeBaseUri();
		}
		
		// get the return statement 
		this.selectorAlias = new HashMap<String, String>();
		this.selectorWhereClause = new StringBuilder();
		this.addedSelectors = new HashMap<String, String>();
		GenRowFilters combinedFilters = this.qs.getCombinedFilters();
		qsFilteredColumns = combinedFilters.getAllQsFilteredColumns();
		qsFilteredColumnsToNull = getFilteredColumnsToNull(combinedFilters.getFilters());
		// add the join where clause
		addJoins(this.qs.getRelations());
				
		addSelectors(this.qs.getSelectors());
		
		// add the filters
		addFilters(combinedFilters, baseUri);
		
		// add the group bys
		addGroupClause(((SelectQueryStruct) this.qs).getGroupBy());

		// add sort bys
		addOrderByClause(((SelectQueryStruct) this.qs).getOrderBy());
		
		// combine the pieces and return
		StringBuilder query = new StringBuilder();
		String distinct = "";
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			distinct = "DISTINCT ";
		}
		query.append("SELECT ").append(distinct).append(this.selectors.toString());
		query.append(" WHERE { ");
		query.append(this.bindWhereClause.toString());
		query.append(this.selectorWhereClause.toString());
		query.append(this.filtersWhereClause.toString());
		query.append(this.relationshipWhereClause.toString());
		query.append("}");
		query.append(this.groupByClause.toString());
		query.append(" ");
		query.append(this.sortByClause);
		
		long limit = ((SelectQueryStruct) this.qs).getLimit();
		long offset = ((SelectQueryStruct) this.qs).getOffset();
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
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector);
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
		String conceptCleanVarName = Utility.cleanVariableString(concept);
		String cleanVarName = conceptCleanVarName;
		// add the node to the where statement
		addNodeSelectorTriple(conceptCleanVarName, concept);
		// and the property if it is a prop
		if(!property.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
			// make unique based on property
			cleanVarName += "__" + Utility.cleanVariableString(property);
			addNodePropertySelectorTriple(cleanVarName, property, concept, conceptCleanVarName);
		}
		this.selectorAlias.put(cleanVarName, alias);
		return "?" + cleanVarName;
	}
	
	private String processFunctionSelector(QueryFunctionSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		String function = selector.getFunction();
		String colCast = selector.getColCast();
		
		StringBuilder expression = new StringBuilder();
		expression.append(QueryFunctionHelper.convertFunctionToSparqlSyntax(function)).append("(");
		if(selector.isDistinct()) {
			expression.append("DISTINCT ");
		}
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
	 * Adds the node triple
	 * @param nodeVarName
	 * @param concept
	 */
	private String addNodeSelectorTriple(String nodeVarName, String concept) {
		if(!this.addedSelectors.containsKey(nodeVarName)) {
			String nodeUri = engine.getConceptPhysicalUriFromConceptualUri(concept);
			// add the pattern around the concept
			this.selectorWhereClause.append("{?").append(nodeVarName).append(" <").append(RDF.TYPE).append("> <").append(nodeUri).append(">}");
			this.addedSelectors.put(nodeVarName, nodeUri);
		}
		return this.addedSelectors.get(nodeVarName);
	}
	
	/**
	 * Add a node property triple
	 * @param propVarName
	 * @param property
	 * @param nodeVarName
	 */
	private String addNodePropertySelectorTriple(String propVarName, String property, String concept, String conceptVarName) {
		if(!this.addedSelectors.containsKey(propVarName)) {
			String propUri = engine.getPropertyPhysicalUriFromConceptualUri(property, concept);
			// add the pattern around the property
			// if filtered and not to a null
			// no need for optional
			if(this.qsFilteredColumns.contains(propVarName) && !this.qsFilteredColumnsToNull.contains(propVarName)) {
				this.selectorWhereClause.append("{?").append(conceptVarName).append(" <").append(propUri).append("> ?").append(propVarName).append("}");
			} else {
				this.selectorWhereClause.append("OPTIONAL{?").append(conceptVarName).append(" <").append(propUri).append("> ?").append(propVarName).append("}");
			}
			this.addedSelectors.put(propVarName, propUri);
		}
		return this.addedSelectors.get(propVarName);
	}
	
	private void addJoins(Set<String[]> relations) {
		this.relationshipWhereClause = new StringBuilder();
		for(String[] rel : relations) {
			String fromNode = rel[0];
			String joinType = rel[1];
			String toNode = rel[2];
			addJoin(fromNode, joinType, toNode);
		}
	}

	private void addJoin(String fromNode, String joinType, String toNode) {
		String fromNodeVarName = Utility.cleanVariableString(fromNode);
		String fromURI = this.engine.getConceptPhysicalUriFromConceptualUri(fromNode);
		String toNodeVarName = Utility.cleanVariableString(toNode);
		String toURI = this.engine.getConceptPhysicalUriFromConceptualUri(toNode);
		
		// need to figure out what the predicate is from the owl
		// also need to determine the direction of the relationship -- if it is forward or backward
		String query = "SELECT ?relationship WHERE { {<" + fromURI + "> ?relationship <" + toURI + "> } filter(?relationship != <http://semoss.org/ontologies/Relation>) } ORDER BY DESC(?relationship)";
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
//		String randomRelVarName = Utility.getRandomString(6);
		if(joinType.equals("inner.join")) {
			addNodeSelectorTriple(fromNodeVarName, fromNode);
			addNodeSelectorTriple(toNodeVarName, toNode);
//			this.relationshipWhereClause.append("{?").append(randomRelVarName).append(" <").append(RDFS.SUBPROPERTYOF).append("> <").append(predURI).append(">}");
			this.relationshipWhereClause.append("{?").append(fromNodeVarName).append(" <").append(predURI).append("> ?").append(toNodeVarName).append("}");
		
		} else if(joinType.equals("left.outer.join")) {
			addNodeSelectorTriple(fromNodeVarName, fromNode);
			this.relationshipWhereClause.append("OPTIONAL {")
//				.append("{?").append(randomRelVarName).append(" <").append(RDFS.SUBPROPERTYOF).append("> <").append(predURI).append(">}")
				.append("{?").append(toNodeVarName).append(" <").append(RDF.TYPE).append("> <").append(toURI).append(">} ")
				.append("{?").append(fromNodeVarName).append(" <").append(predURI).append("> ?").append(toNodeVarName).append("}")
				.append("}");
			
			this.addedSelectors.put(toNodeVarName, toURI);

		} else if(joinType.equals("right.outer.join")) {
			addNodeSelectorTriple(toNodeVarName, toNode);
			this.relationshipWhereClause.append("OPTIONAL {")
//			.append("{?").append(randomRelVarName).append(" <").append(RDFS.SUBPROPERTYOF).append("> <").append(predURI).append(">}")
				.append("{?").append(fromNodeVarName).append(" <").append(RDF.TYPE).append("> <").append(fromURI).append(">} ")
				.append("{?").append(fromNodeVarName).append(" <").append(predURI).append("> ?").append(toNodeVarName).append("}")
				.append("}");
			
			this.addedSelectors.put(fromNodeVarName, fromURI);
			
		} else if(joinType.equals("outer.join")) {
			addNodeSelectorTriple(fromNodeVarName, fromNode);
			addNodeSelectorTriple(toNodeVarName, toNode);
			this.relationshipWhereClause.append("OPTIONAL {")
//				.append("{?").append(randomRelVarName).append(" <").append(RDFS.SUBPROPERTYOF).append("> <").append(predURI).append(">}")
				.append("{?").append(fromNodeVarName).append(" <").append(predURI).append("> ?").append(toNodeVarName).append("}")
				.append("}");
		}
	}
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	//////////////////////// FILTERING //////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

	
	private void addFilters(GenRowFilters grf, String baseUri) {
		this.filtersWhereClause = new StringBuilder();
		this.bindWhereClause = new StringBuilder();
		this.bindingsWhereClause = new StringBuilder();
		
		List<IQueryFilter> filters = grf.getFilters();
		for(IQueryFilter filter : filters) {
			boolean startSimple = filter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE;
			StringBuilder filterSyntax = processFilter(filter, baseUri, startSimple);
			if(filterSyntax != null) {
				// NOTE! we add the filter here and not within the individual methods
				// so we can correctly do AND/OR filtering
				this.filtersWhereClause.append(" FILTER( ").append(filterSyntax.toString()).append(")");
			}
		}
	}
	
	private StringBuilder processFilter(IQueryFilter filter, String baseUri, boolean startSimple) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, baseUri, startSimple);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, baseUri);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, baseUri);
		}
		return null;
	}

	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String baseUri) {
		StringBuilder filterBuilder = new StringBuilder("(");
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i != 0) {
				filterBuilder.append(" || ");
			}
			filterBuilder.append(processFilter(filterList.get(i), baseUri, false));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String baseUri) {
		StringBuilder filterBuilder = new StringBuilder("(");
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i != 0) {
				filterBuilder.append(" && ");
			}
			filterBuilder.append(processFilter(filterList.get(i), baseUri, false));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String baseUri, boolean startSimple) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		FILTER_TYPE fType = filter.getFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, baseUri, startSimple);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), baseUri, startSimple);
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		
		return null;
	}

	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		StringBuilder filterBuilder = new StringBuilder(processSelector(leftSelector))
			.append(" ").append(thisComparator).append(" ").append(processSelector(rightSelector));
		
		return filterBuilder;
	}
	
	/**
	 * Add a filter of one column to a set of values
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String baseUri, boolean isSimple) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String columnExpression = processSelector(leftSelector);
		
		List<Object> rightObjects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			rightObjects.addAll( (List) rightComp.getValue());
		} else {
			rightObjects.add(rightComp.getValue());
		}
		
		// if we have a simple column, we can try to do optimizations with bind, bindings, etc.
		// if it is complex, we must do Filter
		if(leftSelector instanceof QueryColumnSelector) {
			// column expression already starts with ?
			// want to remove it for now
			columnExpression = columnExpression.substring(1);
			boolean isProp = false;
			if(columnExpression.contains("__")) {
				isProp = true;
			}
			return addColToValuesFilter(columnExpression, thisComparator, rightObjects, rightComp.getNounType(), isProp, baseUri, isSimple);
		} else {
			return addComplexExpressionToValuesFilter(columnExpression, thisComparator, rightObjects, rightComp.getNounType());
		}
	}
	
	private StringBuilder addComplexExpressionToValuesFilter(String leftExpression, String thisComparator, List<Object> rightObjects, PixelDataType nounType) {
		int numObjects = rightObjects.size();
		StringBuilder filterBuilder = new StringBuilder();
		// modify the == comparator to work on sparql
		if(thisComparator.equals("==")) {
			thisComparator = "=";
		}
		if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
			for(int i = 0; i < numObjects; i++) {
				if(i != 0) {
					this.filtersWhereClause.append(" || ");
				}
				filterBuilder.append("(").append(leftExpression).append(") ").append(thisComparator).append(" ")
					.append("\"").append(rightObjects.get(i)).append("\"^^<").append(XSD.xdouble).append(">");
			}
		} else {
			for(int i = 0; i < numObjects; i++) {
				if(i != 0) {
					filterBuilder.append(" || ");
				}
				filterBuilder.append("(").append(leftExpression).append(") ").append(thisComparator).append(" ")
					.append("\"").append(rightObjects.get(i)).append("\"");
			}
		}
		return filterBuilder;
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
	private StringBuilder addColToValuesFilter(String leftCleanVarName, String thisComparator, List<Object> rightObjects, 
			PixelDataType nounType, boolean isProp, String baseUri, boolean isSimple) {
		
		// need to account for null inputs
		boolean addNullCheck = false;
		boolean hasMoreValuesThanNull = true;
		if(rightObjects.contains(null)) {
			addNullCheck = true;
			rightObjects.remove(null);
			if(rightObjects.isEmpty()) {
				hasMoreValuesThanNull = false;
			}
		}
		
		int numObjects = rightObjects.size();
		if(!addNullCheck && numObjects == 1 && isSimple && thisComparator.equals("==")) {
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
			// do not return anything
			// since it is simple and we will append this at the start
			return null;
		} else if(!addNullCheck && !bindingsAdded && isSimple && numObjects > 1 && thisComparator.equals("==")){
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
			// do not return anything
			// since it is simple and we will append this at the start
			return null;
		} else {
			// add filter
			// start the syntax
			StringBuilder filterBuilder = new StringBuilder();
			// modify the == comparator to work on sparql
			if(thisComparator.equals("==")) {
				thisComparator = "=";
			}
			
			if(addNullCheck) {
				if(thisComparator.equals("=")) {
					filterBuilder.append("!BOUND(?").append(leftCleanVarName).append(")");
				} else {
					filterBuilder.append("BOUND(?").append(leftCleanVarName).append(")");
				}
			}
			
			if(hasMoreValuesThanNull) {
				
				// if we added a null check
				// we need to add and or to the rest of everything
				if(addNullCheck) {
					filterBuilder.append(" || (");
				}
				
				// based on the input, set the filter information
				// if it is a like, we will use a regex filter
				// otherwise, normal filter
				if(thisComparator.equals("?like")) {
					if(isProp) {
						if(nounType == PixelDataType.CONST_DECIMAL || nounType == PixelDataType.CONST_INT) {
							for(int i = 0; i < numObjects; i++) {
								if(i != 0) {
									filterBuilder.append(" || ");
								}
								filterBuilder.append("REGEX(STR(?").append(leftCleanVarName).append("), \"").append(rightObjects.get(i)).append("\", 'i')");
							}
						} else {
							for(int i = 0; i < numObjects; i++) {
								if(i != 0) {
									filterBuilder.append(" || ");
								}
								filterBuilder.append("REGEX(?").append(leftCleanVarName).append(", \"").append(rightObjects.get(i)).append("\", 'i')");
							}
						}
					} else {
						String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								filterBuilder.append(" || ");
							}
							filterBuilder.append("REGEX(STR(?").append(leftCleanVarName).append("), \"")
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
								filterBuilder.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ")
									.append("\"").append(rightObjects.get(i)).append("\"^^<").append(XSD.xdouble).append(">");
							}
						} else {
							for(int i = 0; i < numObjects; i++) {
								if(i != 0) {
									filterBuilder.append(" || ");
								}
								filterBuilder.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ")
									.append("\"").append(rightObjects.get(i)).append("\"");
							}
						}
					} else {
						String concpetType = Utility.getInstanceName(this.addedSelectors.get(leftCleanVarName));
						for(int i = 0; i < numObjects; i++) {
							if(i != 0) {
								filterBuilder.append(" || ");
							}
							filterBuilder.append("?").append(leftCleanVarName).append(" ").append(thisComparator).append(" ")
								.append("<").append(baseUri).append(concpetType).append("/").append(rightObjects.get(i)).append(">");
						}
					}
				}
				
				
				// close if added null check
				if(addNullCheck) {
					filterBuilder.append(")");
				}
			}
			
			return filterBuilder;
		}
	}

	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	//////////////////////// OTHER PARAMS ///////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	
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
				if(!columnName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
			if(!columnName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
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
	
	/**
	 * Need to determine if a filter is set to equal null 
	 * Need to make sure this property definition is an optional
	 * @param combinedFilters
	 * @return
	 */
	private Set<String> getFilteredColumnsToNull(List<IQueryFilter> filters) {
		Set<String> qsColumnsEqualNull = new HashSet<String>();
		for(IQueryFilter f : filters) {
			if(f.getQueryFilterType() == QUERY_FILTER_TYPE.SIMPLE) {
				SimpleQueryFilter simpleF = (SimpleQueryFilter) f;
				
				IQuerySelector selector = null;
				Object val = null;
				if(simpleF.getFilterType() == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
					selector = (IQuerySelector) simpleF.getLComparison().getValue();
					val = simpleF.getRComparison().getValue();
				} else if(simpleF.getFilterType() == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
					val = simpleF.getLComparison().getValue();
					selector = (IQuerySelector) simpleF.getRComparison().getValue();
				} else {
					// ignore other types
					continue;
				}
				
				if(val == null) {
					qsColumnsEqualNull.add(selector.getQueryStructName()); 
				} else {
					if(val instanceof List) {
						if(((List) val).contains(null)) {
							qsColumnsEqualNull.add(selector.getQueryStructName()); 
						}
					}
				}
			} else if(f.getQueryFilterType() == QUERY_FILTER_TYPE.OR || f.getQueryFilterType() == QUERY_FILTER_TYPE.AND) {
				AbstractListFilter listF = (AbstractListFilter) f;
				qsColumnsEqualNull.addAll(getFilteredColumnsToNull(listF.getFilterList()));
			}
		}
		
		return qsColumnsEqualNull;
	}
}
