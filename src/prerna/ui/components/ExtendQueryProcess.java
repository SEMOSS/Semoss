package prerna.ui.components;

import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JComboBox;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class ExtendQueryProcess {
	
	public static String getQuery(String nodeString, String queryQuestion)
	{
		String query = null;
		String processString = null;
		JComboBox extendBox = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.EXTENDLIST);
		
		
		Hashtable extendHash = (Hashtable)DIHelper.getInstance().getLocalProp(Constants.EXTEND_TABLE);
		Vector questionV = (Vector) extendHash.get(nodeString);
		int vSize = questionV.size();
		for (int i=0;i<vSize;i++)
		{
			String qString = (String) questionV.get(i);
			String[] qStringArray = qString.split(";");
    		if (qStringArray[1].equals(queryQuestion))
			{
    			processString = qStringArray[0];
    			break;
			}
    		
		}
		
		query = createQuery(processString, nodeString);
		return query;
	}

	private static String createQuery(String processString, String nodeString) {
		String[]qElementsArray=processString.split("-");
		String subject=qElementsArray[0];
		String pred=qElementsArray[1];
		String object=qElementsArray[2];
		String filterNode=null;
		String query=null;
		//are the nodes being extend subject or object in extend query
		if (nodeString.equals(qElementsArray[0]))
		{
			filterNode=subject;
		
			query = "CONSTRUCT {?sub ?pred ?obj.?sub1 ?pred1 ?obj1}" + "WHERE {{"+"{ ?sub <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/"+subject+">;}"+ "{?pred <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/"+pred+">;}"+ "{?obj <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/" +object+">;}"+"{?sub ?pred ?obj;}"+"{?sub ?label ?name;}"+"Filter (?name in (FILTER_VALUES))."+"{?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>;}"+"{?pred ?contains ?weight;}"+"FILTER (?weight >= @weight@).}"+"UNION{"+"{ ?sub1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/"+subject+">;}"+ "{?pred1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/"+pred+">;}"+ "{?obj1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/" +object+">;}"+"{?sub1 ?pred1 ?obj1;}"+"{?sub1 ?label1 ?name1;}"+"Filter (?name1 in (FILTER_VALUES))."+"MINUS {?pred1 <http://health.mil/ontologies/dbcm/Relation/Contains/weight>?weight1;}. }}";
				
			
		}
		else
		{
			filterNode=object;
			query = "CONSTRUCT {?sub ?pred ?obj.?sub1 ?pred1 ?obj1}" + "WHERE {{"+"{ ?sub <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/"+subject+">;}"+ "{?pred <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/"+pred+">;}"+ "{?obj <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/" +object+">;}"+"{?sub ?pred ?obj;}"+"{?obj ?label ?name;}"+"Filter (?name in (FILTER_VALUES))."+"{?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>;}"+"{?pred ?contains ?weight;}"+"FILTER (?weight >= @weight@).}"+"UNION{"+"{ ?sub1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/"+subject+">;}"+ "{?pred1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/"+pred+">;}"+ "{?obj1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/" +object+">;}"+"{?sub1 ?pred1 ?obj1;}"+"{?obj1 ?label1 ?name1;}"+"Filter (?name1 in (FILTER_VALUES))."+"MINUS {?pred1 <http://health.mil/ontologies/dbcm/Relation/Contains/weight>?weight1;}. }}";
		}
			
		

		return query;
	}
	
}
