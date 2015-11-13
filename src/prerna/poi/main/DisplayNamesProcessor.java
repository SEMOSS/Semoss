package prerna.poi.main;

import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class DisplayNamesProcessor{
	
	private static final Logger logger = LogManager.getLogger(DisplayNamesProcessor.class.getName());

	/**
	 * process a custom property that is passed in as subject%value;subject%value; etc
	 * @param customProperty
	 * @param RDFType
	 * @throws EngineException 
	 */
	public static void addDisplayNamesToOWL(Hashtable<String,String> displayNameHash, Hashtable<String,String> basePropURIHash, 
			Hashtable<String, String> baseConceptURIHash, BaseDatabaseCreator baseEngCreator) {	
		
		
		for(String relArray : displayNameHash.keySet()){
			String node = relArray;
			String customValue = displayNameHash.get(node);
			String propertyValue = node;
			String propertyValueURI = "";
			if(node.contains("%")){
				String[] splitNode = node.split("%");
				//get parent node as parentValue, get its display name if one exists, append that to your display name so that you know which 
				//properties display name goes with which node
				String parentValue = splitNode[0];
				customValue = parentValue + "/" + customValue;
				String propertyURI = basePropURIHash.get(propertyValue);
				if(propertyURI!=null && !propertyURI.isEmpty()){
					propertyValueURI = propertyURI;
				} 
			} else {
			
				String conceptURI = baseConceptURIHash.get(propertyValue);
				//check concept first, then property
				if(conceptURI != null && !conceptURI.isEmpty()){
					propertyValueURI = conceptURI;
				}
			}
			if(propertyValueURI.length() > 0){
				String predicate = Constants.DISPLAY_URI.substring(0,(Constants.DISPLAY_URI.length()-1));//semossURI + "/" + Constants.DEFAULT_DISPLAY_NAME ;
				String object = predicate + "/" + customValue;
				baseEngCreator.addToBaseEngine(new Object[]{propertyValueURI, predicate, object, false});
			} else {
				logger.error("Unable to create label/display naming in OWL for node: " + node);
			}
		}
	}
	
	public static Hashtable<String,String> generateDisplayNameMap(Hashtable<String, String> rdfMap){
		String displayNames = Constants.DISPLAY_NAME;
		Hashtable<String,String> displayNamesHash = new Hashtable<String,String>();

		if(rdfMap.get(displayNames) != null)
		{
			String customPropertyValue = rdfMap.get(displayNames);
			StringTokenizer custTokens = new StringTokenizer(customPropertyValue, ";");
			while(custTokens.hasMoreElements())
			{
				String custrelation = custTokens.nextToken();
				if(!custrelation.contains(":"))
					break;
				String[] strSplit = custrelation.split(":");
				String subject = strSplit[0];
				String node = subject;
				String property = subject;
				String displayName = Utility.cleanVariableString(strSplit[1]); //if we want to support special chars, then dont clean this:  strSplit[1];
				if(subject.contains("%")){
					String[] splitSubject = subject.split("%");
					node = Utility.cleanVariableString(splitSubject[0]);
					property = Utility.cleanVariableString(splitSubject[1]);
					subject = node+"%"+property;
				}
				
				if(!displayNamesHash.contains(displayName)){
					displayNamesHash.put(subject,displayName);//displayNamesHash.put(property,displayName);
				} else {
					logger.error("Unable to process display name " + displayName + " on " + subject + ".  Verify that the displayName is not a repeat and try again.");
				}
			}			
		}
		return displayNamesHash;
	}
	
	
}