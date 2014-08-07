package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class SEMOSSQuery {
	private static final String main = "Main";
	private ArrayList<SPARQLTriple> triples = new ArrayList<SPARQLTriple>();
	private ArrayList<TriplePart> retVars = new ArrayList<TriplePart>();
	private Hashtable<TriplePart, ISPARQLReturnModifier> retModifyPhrase = new Hashtable<TriplePart,ISPARQLReturnModifier>();
	private Hashtable<String, SPARQLPatternClause> clauseHash = new Hashtable<String, SPARQLPatternClause>();
	private SPARQLGroupBy groupBy = null;
	private SPARQLBindings bindings = null;
	private Integer limit = null;
	private String customQueryStructure ="";
	private String queryType;
	private boolean distinct = false;
	private String query;
	private String queryID;
	
	public SEMOSSQuery()
	{
	}
	
	public SEMOSSQuery(String queryID)
	{
		this.queryID = queryID;
	}
	
	public void setQueryID (String queryID)
	{
		this.queryID = queryID;
	}
	
	public String getQueryID()
	{
		return this.queryID;
	}
	
	public void createQuery()
	{
		query = queryType + " ";
		if(distinct)
			query= query+ "DISTINCT ";
		String retVarString = createReturnVariableString();
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString(true);
		query=query+retVarString + wherePatternString +postWhereString;
		
	}
	
	public String getCountQuery()
	{
		String countQuery = "SELECT ";
		String retVarString = createCountReturnVariableString();
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString(false);
		countQuery=countQuery+retVarString + wherePatternString +postWhereString;
		return countQuery;
	}
	
	public String getQueryPattern(boolean includeLimit){
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString(includeLimit);
		return wherePatternString + postWhereString;
	}
	
	public void addSingleReturnVariable(TriplePart var) 
	{
		if(var.getType().equals(TriplePart.VARIABLE))
		{
			retVars.add(var);
		}
		else
		{
			throw new IllegalArgumentException("Cannot add non-variable parts to the return var string");
		}
	}
	
	public void addSingleReturnVariable(TriplePart var, ISPARQLReturnModifier mod) 
	{
		if(var.getType().equals(TriplePart.VARIABLE))
		{
			retVars.add(var);
			retModifyPhrase.put(var,  mod);
		}
		else
		{
			throw new IllegalArgumentException("Cannot add non-variable parts to the return var string");
		}
	}
	
	public String createPatternClauses()
	{
		String wherePatternString;
		if(customQueryStructure.equals(""))
		{
			Enumeration clauseKeys = clauseHash.keys();
			wherePatternString= "";
			while (clauseKeys.hasMoreElements())
			{
				String key = (String) clauseKeys.nextElement();
				SPARQLPatternClause clause = (SPARQLPatternClause) clauseHash.get(key);
				wherePatternString = wherePatternString + clause.getClauseString();
			}
			wherePatternString = "WHERE { " + wherePatternString + " } ";
		}
		else
		{
			Enumeration clauseKeys = clauseHash.keys();
			wherePatternString= "WHERE { " + customQueryStructure + " } ";
			while (clauseKeys.hasMoreElements())
			{
				String key = (String) clauseKeys.nextElement();
				SPARQLPatternClause clause = (SPARQLPatternClause) clauseHash.get(key);
				wherePatternString = wherePatternString.replaceAll(key, clause.getClauseString());
			}
		}
		return wherePatternString;
	}
	
	public String createReturnVariableString()
	{
		String retVarString = "";
		for (int varIdx=0; varIdx<retVars.size();varIdx++)
		{
			//space out the variables
			if(retModifyPhrase.containsKey(retVars.get(varIdx)))
			{
				ISPARQLReturnModifier mod= retModifyPhrase.get(retVars.get(varIdx));
				retVarString = retVarString + createRetStringWithMod(retVars.get(varIdx), mod) + " ";
			}
			else
			{
			retVarString = retVarString + SPARQLQueryHelper.createComponentString(retVars.get(varIdx))+" ";
			}
		}
		return retVarString;
	}
	
	public String createCountReturnVariableString()
	{
		String retVarString = "(COUNT(CONCAT(" ;
		for (int varIdx=0; varIdx<retVars.size();varIdx++)
		{
			if(varIdx != 0){
				retVarString = retVarString + ", ";
			}
			String varVal = retVars.get(varIdx).getValue() + "";
			retVarString = retVarString + "STR(?" + varVal + ")";
			
		}
		retVarString = retVarString + ")) AS ?entity)";
		return retVarString;
	}
	
	public String createPostPatternString(boolean includeLimit)
	{
		String postWhereString = "";
		if(includeLimit && limit != null)
		{
			postWhereString = "LIMIT " + limit;
		}
		if(groupBy != null)
		{
			postWhereString = groupBy.getString();
		}
		if(bindings != null)
		{
			postWhereString += " " + bindings.getBindingString();
		}
		return postWhereString;
	}
	
	public void addTriple(TriplePart subject, TriplePart predicate, TriplePart object)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLTriple newTriple = new SPARQLTriple(subject, predicate, object);
		if(!this.hasTriple(newTriple))
			clause.addTriple(newTriple);
		
		clauseHash.put(main,  clause);
	}
	
	public void addTriple(TriplePart subject, TriplePart predicate, TriplePart object, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLTriple newTriple = new SPARQLTriple(subject, predicate, object);
		if(!this.hasTriple(newTriple))
			clause.addTriple(newTriple);
		
		clauseHash.put(clauseName,  clause);
	}
	
	public boolean hasTriple(SPARQLTriple triple)
	{
		boolean retBoolean = false;
		
		for(String clauseKey : clauseHash.keySet())
		{
			SPARQLPatternClause clause = clauseHash.get(clauseKey);
			if(clause.hasTriple(triple))
			{
				retBoolean = true;
				break;
			}
		}
		return retBoolean;
	}
	
	public void addBind(TriplePart bindSubject, TriplePart bindObject)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLBind newBind = new SPARQLBind(bindSubject, bindObject);
		clause.addBind(newBind);
		clauseHash.put(main, clause);
	}
	
	public void addBind(TriplePart bindSubject, TriplePart bindObject, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLBind newBind = new SPARQLBind(bindSubject, bindObject);
		clause.addBind(newBind);
		clauseHash.put(clauseName,  clause);
	}
	
	public void addRegexFilter(TriplePart var, ArrayList<TriplePart> filterData, boolean isValueString, boolean or)
	{
		ArrayList<Object> addToFilter = new ArrayList<Object>();
		for(TriplePart bindVar : filterData)
		{
			SPARQLRegex regex = new SPARQLRegex(var, bindVar, isValueString);
			addToFilter.add(regex);
		}
		addFilter(addToFilter, or);
	}
	
	public void addRegexFilter(TriplePart var, ArrayList<TriplePart> filterData, boolean isValueString, boolean or,  String clauseName)
	{
		ArrayList<Object> addToFilter = new ArrayList<Object>();
		for(TriplePart bindVar : filterData)
		{
			SPARQLRegex regex = new SPARQLRegex(var, bindVar, isValueString);
			addToFilter.add(regex);
		}
		addFilter(addToFilter, or, clauseName);
	}
	
	public void addFilter(ArrayList<Object> filterData, boolean or)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLFilter newFilter = new SPARQLFilter(filterData, or);
		clause.addFilter(newFilter);
		clauseHash.put(main, clause);
	}
	
	public void addFilter(ArrayList<Object> filterData, boolean or, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLFilter newFilter = new SPARQLFilter(filterData, or);
		clause.addFilter(newFilter);
		clauseHash.put(clauseName,  clause);
	}
	
	public String createRetStringWithMod(TriplePart var, ISPARQLReturnModifier mod)
	{
		String retString = "("+mod.getModifierAsString()+ " AS " + SPARQLQueryHelper.createComponentString(var)+ ")";
		return retString;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getQueryType()
	{
		return queryType;
	}
	
	public void setQueryType(String queryType)
	{
		if( !(queryType.equals(SPARQLConstants.SELECT)) && !(queryType.equals(SPARQLConstants.CONSTRUCT)))
		{
			throw new IllegalArgumentException("SELECT or CONSTRUCT Queries only");
		}
		this.queryType = queryType;
	}
	
	public ArrayList<SPARQLTriple> getTriples()
	{
		return triples;
	}
	
	public ArrayList<TriplePart> getRetVars()
	{
		return retVars;
	}
	
	public void setCustomQueryStructure(String structure)
	{
		this.customQueryStructure= structure;
	}
	
	public void setDisctinct(boolean distinct)
	{
		this.distinct=distinct;
	}
	
	public void setGroupBy(SPARQLGroupBy groupBy)
	{
		this.groupBy = groupBy;
	}
	
	public SPARQLGroupBy getGroupBy()
	{
		return this.groupBy;
	}
	
	public void setBindings(SPARQLBindings bindings)
	{
		this.bindings = bindings;
	}

	public void setLimit(Integer limit)
	{
		this.limit = limit;
	}
	
}
