package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class SEMOSSQueryHelper {

	static final Logger logger = LogManager.getLogger(SEMOSSQueryHelper.class.getName());

	public static void addSingleReturnVarToQuery(String varString, SEMOSSQuery seQuery) 
	{
		TriplePart var = new TriplePart(varString, TriplePart.VARIABLE);
		seQuery.addSingleReturnVariable(var);
	}

	public static void addSingleReturnVarToQuery(String varString, ISPARQLReturnModifier modifier, SEMOSSQuery seQuery) 
	{
		TriplePart var = new TriplePart(varString, TriplePart.VARIABLE);
		seQuery.addSingleReturnVariable(var, modifier);
	}

	public static ISPARQLReturnModifier createReturnModifier(ISPARQLReturnModifier modifier, SPARQLModifierConstant type) 
	{		
		SPARQLAbstractReturnModifier newModifier = new SPARQLAbstractReturnModifier();
		SPARQLModifierConstant modConst;
		if(type.equals(SPARQLAbstractReturnModifier.SUM))
			modConst = SPARQLAbstractReturnModifier.SUM;
		else if(type.equals(SPARQLAbstractReturnModifier.COUNT))
			modConst = SPARQLAbstractReturnModifier.COUNT;
		else if(type.equals(SPARQLAbstractReturnModifier.DISTINCT))
			modConst = SPARQLAbstractReturnModifier.DISTINCT;
		
		else if(type.equals(SPARQLAbstractReturnModifier.AVERAGE))
			modConst = SPARQLAbstractReturnModifier.AVERAGE;
		else if(type.equals(SPARQLAbstractReturnModifier.MAX))
			modConst = SPARQLAbstractReturnModifier.MAX;
		else if(type.equals(SPARQLAbstractReturnModifier.MIN))
			modConst = SPARQLAbstractReturnModifier.MIN;
		else if(type.equals(SPARQLAbstractReturnModifier.NONE))
			modConst = SPARQLAbstractReturnModifier.NONE;
		else
			throw new IllegalArgumentException("Modifiers include only SUM, COUNT, or DISTINCT");
		
		newModifier.createModifier(modifier, modConst);
		return newModifier;
	}
	
	public static ISPARQLReturnModifier createReturnModifier(String varString, SPARQLModifierConstant type) 
	{		
		SPARQLAbstractReturnModifier newModifier = new SPARQLAbstractReturnModifier();
		TriplePart var = new TriplePart(varString, TriplePart.VARIABLE);
		SPARQLModifierConstant modConst;
		if(type.equals(SPARQLAbstractReturnModifier.SUM))
			modConst = SPARQLAbstractReturnModifier.SUM;
		else if(type.equals(SPARQLAbstractReturnModifier.COUNT))
			modConst = SPARQLAbstractReturnModifier.COUNT;
		else if(type.equals(SPARQLAbstractReturnModifier.DISTINCT))
			modConst = SPARQLAbstractReturnModifier.DISTINCT;
		
		else if(type.equals(SPARQLAbstractReturnModifier.AVERAGE))
			modConst = SPARQLAbstractReturnModifier.AVERAGE;
		else if(type.equals(SPARQLAbstractReturnModifier.MAX))
			modConst = SPARQLAbstractReturnModifier.MAX;
		else if(type.equals(SPARQLAbstractReturnModifier.MIN))
			modConst = SPARQLAbstractReturnModifier.MIN;
		else if(type.equals(SPARQLAbstractReturnModifier.NONE))
			modConst = SPARQLAbstractReturnModifier.NONE;
		else
			throw new IllegalArgumentException("Modifiers include only SUM, COUNT, or DISTINCT");
		
		newModifier.createModifier(var, modConst);
		return newModifier;
	}
	
	public static ISPARQLReturnModifier createReturnModifier(ArrayList<Object> dataList, ArrayList<String> opList) 
	{		
		for (int enIdx = 0; enIdx<dataList.size(); enIdx++)
		{
			if(dataList.get(enIdx) instanceof String)
			{
				TriplePart var = new TriplePart(dataList.get(enIdx), TriplePart.VARIABLE);
				dataList.remove(enIdx);
				dataList.add(enIdx, var);
			}
		}
		ArrayList<SPARQLModifierConstant> operatorList = new ArrayList<SPARQLModifierConstant>();
		for (int opIdx = 0; opIdx<opList.size(); opIdx++)
		{
			String opString = opList.get(opIdx);
			SPARQLModifierConstant modConst;
			if(opString.equals("+"))
				modConst = SPARQLMathModifier.ADD;
			else if(opString.equals("-"))
				modConst = SPARQLMathModifier.SUBTRACT;
			else if(opString.equals("*"))
				modConst = SPARQLMathModifier.MULTIPLY;
			else if(opString.equals("/"))
				modConst = SPARQLMathModifier.DIVIDE;
			else
				throw new IllegalArgumentException("Math operators currently include only +, -, *, /");
					
			operatorList.add(opIdx, modConst);
		}
		SPARQLMathModifier modifier = new SPARQLMathModifier();
		modifier.createModifier(dataList, operatorList);
		return modifier;
	}

	public ISPARQLReturnModifier createReturnModifier(String varString, ISPARQLReturnModifier modifier, SEMOSSQuery seQuery) 
	{
		TriplePart var = new TriplePart(varString, TriplePart.VARIABLE);
		seQuery.addSingleReturnVariable(var, modifier);
		return modifier;
	}

	public static void addConceptTypeTripleToQuery(String variableName, String conceptURI, SEMOSSQuery seQuery)
	{
		TriplePart conceptVar = new TriplePart(variableName, TriplePart.VARIABLE);
		TriplePart typeURI = new TriplePart(SPARQLConstants.TYPE_URI, TriplePart.URI);
		TriplePart conceptTypeURI = new TriplePart(conceptURI, TriplePart.URI);
		seQuery.addTriple(conceptVar, typeURI, conceptTypeURI);
	}

	public static void addConceptTypeTripleToQuery(String variableName, String conceptURI, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart conceptVar = new TriplePart(variableName, TriplePart.VARIABLE);
		TriplePart typeURI = new TriplePart(SPARQLConstants.TYPE_URI, TriplePart.URI);
		TriplePart conceptTypeURI = new TriplePart(conceptURI, TriplePart.URI);
		seQuery.addTriple(conceptVar, typeURI, conceptTypeURI, clauseName);
	}

	public static void addRelationTypeTripleToQuery(String variableName, String relationURI, SEMOSSQuery seQuery)
	{
		TriplePart relationVar = new TriplePart(variableName, TriplePart.VARIABLE);
		TriplePart subPropURI = new TriplePart(SPARQLConstants.SUBPROP_URI, TriplePart.URI);
		TriplePart relationTypeURI = new TriplePart(relationURI, TriplePart.URI);
		seQuery.addTriple(relationVar, subPropURI, relationTypeURI);
	}

	public static void addRelationTypeTripleToQuery(String variableName, String relationURI, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart relationVar = new TriplePart(variableName, TriplePart.VARIABLE);
		TriplePart subPropURI = new TriplePart(SPARQLConstants.SUBPROP_URI, TriplePart.URI);
		TriplePart relationTypeURI = new TriplePart(relationURI, TriplePart.URI);
		seQuery.addTriple(relationVar, subPropURI, relationTypeURI, clauseName);
	}

	public static void addRelationshipVarTripleToQuery(String subject, String predicate, String object, SEMOSSQuery seQuery)
	{
		TriplePart subjectVar = new TriplePart(subject, TriplePart.VARIABLE);
		TriplePart predicateVar = new TriplePart(predicate, TriplePart.VARIABLE);
		TriplePart objectVar = new TriplePart(object, TriplePart.VARIABLE);
		seQuery.addTriple(subjectVar, predicateVar, objectVar);
	}

	public static void addRelationshipVarTripleToQuery(String subject, String predicate, String object, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart subjectVar = new TriplePart(subject, TriplePart.VARIABLE);
		TriplePart predicateVar = new TriplePart(predicate, TriplePart.VARIABLE);
		TriplePart objectVar = new TriplePart(object, TriplePart.VARIABLE);
		seQuery.addTriple(subjectVar, predicateVar, objectVar, clauseName);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, String object, TriplePartConstant objectType, SEMOSSQuery seQuery)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, int object, TriplePartConstant objectType, SEMOSSQuery seQuery)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put integer as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, double object, TriplePartConstant objectType, SEMOSSQuery seQuery)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put double as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, Date object, TriplePartConstant objectType, SEMOSSQuery seQuery)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put date as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, String object, TriplePartConstant objectType, SEMOSSQuery seQuery, String clauseName)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery, clauseName);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, int object, TriplePartConstant objectType, SEMOSSQuery seQuery, String clauseName)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put integer as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery, clauseName);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, double object, TriplePartConstant objectType, SEMOSSQuery seQuery, String clauseName)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put double as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery, clauseName);
	}

	public static void addGenericTriple(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, Date object, TriplePartConstant objectType, SEMOSSQuery seQuery, String clauseName)
	{
		if (subjectType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Subject cannot be a literal");
		else if (predicateType.equals(TriplePart.LITERAL))
			throw new IllegalArgumentException("Predicate cannot be a literal");
		else if (objectType.equals(TriplePart.VARIABLE) || objectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put date as variable or URI, use String");
		addTriplesToQueryFromGenericCall(subject, subjectType, predicate, predicateType, object, objectType, seQuery, clauseName);
	}


	private static void addTriplesToQueryFromGenericCall(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, Object object, TriplePartConstant objectType, SEMOSSQuery seQuery)
	{
		TriplePart subjectPart = new TriplePart(subject, subjectType);
		TriplePart predicatePart = new TriplePart(predicate, predicateType);
		TriplePart objectPart = new TriplePart(object, objectType);
		seQuery.addTriple(subjectPart, predicatePart, objectPart);
	}

	private static void addTriplesToQueryFromGenericCall(String subject, TriplePartConstant subjectType, String predicate, TriplePartConstant predicateType, Object object, TriplePartConstant objectType, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart subjectPart = new TriplePart(subject, subjectType);
		TriplePart predicatePart = new TriplePart(predicate, predicateType);
		TriplePart objectPart = new TriplePart(object, objectType);
		seQuery.addTriple(subjectPart, predicatePart, objectPart, clauseName);
	}
	
	public static void addBindPhrase(String bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery);
	}

	public static void addBindPhrase(int bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put integer as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery);
	}

	public static void addBindPhrase(double bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put double as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery);
	}

	public static void addBindPhrase(Date bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put Date as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery);
	}

	public static void addBindPhrase(String bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery, String clauseName)
	{
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery, clauseName);
	}

	public static void addBindPhrase(int bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery, String clauseName)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put integer as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery, clauseName);
	}

	public static void addBindPhrase(double bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery, String clauseName)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put double as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery, clauseName);
	}

	public static void addBindPhrase(Date bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery, String clauseName)
	{
		if (bindSubjectType.equals(TriplePart.VARIABLE) || bindSubjectType.equals(TriplePart.URI))
			throw new IllegalArgumentException("Cannot put Date as variable or URI, use String");
		addBindToQueryFromCall(bindSubject, bindSubjectType, bindObject, seQuery, clauseName);
	}
	
	private static void addBindToQueryFromCall(Object bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		TriplePart subjectBindPart = new TriplePart(bindSubject, bindSubjectType);
		TriplePart objectBindPart = new TriplePart(bindObject, TriplePart.VARIABLE);
		seQuery.addBind(subjectBindPart, objectBindPart);
	}

	private static void addBindToQueryFromCall(Object bindSubject, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart subjectBindPart = new TriplePart(bindSubject, bindSubjectType);
		TriplePart objectBindPart = new TriplePart(bindObject, TriplePart.VARIABLE);
		seQuery.addBind(subjectBindPart, objectBindPart, clauseName);
	}
	
	public static void addRegexFilterPhrase(String var, TriplePartConstant varType, ArrayList<Object> filterData, TriplePartConstant filterDataType, boolean isValueString, boolean or, SEMOSSQuery seQuery)
	{
		TriplePart varBindPart = new TriplePart(var, varType);
		ArrayList<TriplePart> filterDataPart = new ArrayList<TriplePart>();
		for(Object filterElem : filterData)
		{
			TriplePart filterElemPart = new TriplePart(filterElem, filterDataType);
			filterDataPart.add(filterElemPart);
		}
		seQuery.addRegexFilter(varBindPart, filterDataPart, isValueString, or);
	}
	
	public static void addRegexFilterPhrase(String var, TriplePartConstant varType, ArrayList<Object> filterData, TriplePartConstant filterDataType, boolean isValueString, boolean or, SEMOSSQuery seQuery, String clauseName)
	{
		TriplePart varBindPart = new TriplePart(var, varType);
		ArrayList<TriplePart> filterDataPart = new ArrayList<TriplePart>();
		for(Object filterElem : filterData)
		{
			TriplePart filterElemPart = new TriplePart(filterElem, filterDataType);
			filterDataPart.add(filterElemPart);
		}
		seQuery.addRegexFilter(varBindPart, filterDataPart, isValueString, or, clauseName);
	}
	
	public static void addGroupByToQuery(ArrayList<String> list, SEMOSSQuery seQuery)
	{
		// needs to be a unique list... having group by with the same variable doesn't make sense
		logger.info("Passed " + list.size() + " for binding");
		Set<String> uniqueList = new HashSet<String>(list);
		logger.info("Unique list count: " + uniqueList.size());
				
		ArrayList<TriplePart> varsList = new ArrayList<TriplePart>();
		Iterator<String> uniqueIt = uniqueList.iterator();
		int count = 0;
		while (uniqueIt.hasNext())
		{
			TriplePart var = new TriplePart(uniqueIt.next(), TriplePart.VARIABLE);
			varsList.add(count, var);
			count++;
		}
		SPARQLGroupBy groupBy = new SPARQLGroupBy(varsList);
		seQuery.setGroupBy(groupBy);
	}
	
	public static void addBindingsToQuery(ArrayList<Object> subjectList, TriplePartConstant bindSubjectType, String bindObject, SEMOSSQuery seQuery)
	{
		ArrayList<TriplePart> bindList = new ArrayList<TriplePart>();
		for(int bindIdx = 0; bindIdx < subjectList.size(); bindIdx++)
		{
			TriplePart bind = new TriplePart(subjectList.get(bindIdx), bindSubjectType);
			bindList.add(bindIdx, bind);
		}
		TriplePart bindVar = new TriplePart(bindObject, TriplePart.VARIABLE);
		SPARQLBindings bindings = new SPARQLBindings(bindList, bindVar);
		seQuery.setBindings(bindings);
	}
	
	public static void addParametersToQuery(ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery seQuery, String clauseName){
		for(int i=0; i < parameters.size(); i++){
			Hashtable<String, String> paramHash = parameters.get(i);
			
			Set<String> paramKeys = paramHash.keySet();
			for(String key : paramKeys){
				TriplePart paramPart = new TriplePart(key, TriplePart.VARIABLE);

				seQuery.addParameter(key, paramHash.get(key), paramPart, clauseName);
			}
		}
	}
	
	public static void addMathFuncToQuery(String grouping, String selected, SEMOSSQuery semossQuery, String varName) {
		String returnModifier = grouping;
		
		ISPARQLReturnModifier mod = null;
		
		if(returnModifier.equals("count")){
			mod = SEMOSSQueryHelper.createReturnModifier(selected, SPARQLAbstractReturnModifier.COUNT);
		}
		else if (returnModifier.equals("average")){
			mod = SEMOSSQueryHelper.createReturnModifier(selected, SPARQLAbstractReturnModifier.AVERAGE);
		}
		else if (returnModifier.equals("max")){
			mod = SEMOSSQueryHelper.createReturnModifier(selected, SPARQLAbstractReturnModifier.MAX);
		}
		else if (returnModifier.equals("min")){
			mod = SEMOSSQueryHelper.createReturnModifier(selected, SPARQLAbstractReturnModifier.MIN);
		}
		SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, mod, semossQuery);
	}
}
