package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.openrdf.model.URI;



public class SEMOSSQuery {
	private static final String main = "Main";
	private ArrayList<SPARQLTriple> triples = new ArrayList<SPARQLTriple>();
	private ArrayList<TriplePart> retVars = new ArrayList<TriplePart>();
	private Hashtable<TriplePart, ISPARQLReturnModifier> retModifyPhrase = new Hashtable<TriplePart,ISPARQLReturnModifier>();
	private Hashtable<String, SPARQLPatternClause> clauseHash = new Hashtable<String, SPARQLPatternClause>();
	private SPARQLGroupBy groupBy = null;
	private String retVarString, wherePatternString, postWhereString;
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
		createReturnVariableString();
		createPatternClauses();
		createPostPatternString();
		query=query+retVarString + wherePatternString +postWhereString;
		
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
	

	
	public void createPatternClauses()
	{
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
	}
	
	public void createReturnVariableString()
	{
		retVarString = "";
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
	}
	
	
	public void createPostPatternString()
	{
		
		if(groupBy != null)
		{
			postWhereString = groupBy.getString();
		}
		else
		{
			postWhereString = "";
		}
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
		clauseHash.put(main,  clause);
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
	
}
