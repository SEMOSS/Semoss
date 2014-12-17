package prerna.ui.components;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class CSVPropFileBuilder{

	private StringBuilder relationships = new StringBuilder();
	private StringBuilder node_properties = new StringBuilder();	
	
	private Hashtable<String, String> propHash = new Hashtable<String, String>();
	private Hashtable<String, String> dataTypeHash = new Hashtable<String, String>();
	
	private StringBuilder propFile = new StringBuilder();
	
	public CSVPropFileBuilder(){
		propFile.append("START_ROW\t2\n");
		propFile.append("END_ROW\t100000\n");
	}
	
	public void addProperty(ArrayList<String> sub, ArrayList<String> obj, String dataType) {
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}
		
		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}
		
		dataTypeHash.put(object, dataType);
		node_properties.append(subject+"%"+object+";");
	}

	public void addRelationship(ArrayList<String> sub, String pred, ArrayList<String> obj) {		
		Iterator<String> subIt = sub.iterator();
		String subject = subIt.next();
		while(subIt.hasNext()){
			subject = subject + "+" + subIt.next();
		}

		Iterator<String> objIt = obj.iterator();
		String object = objIt.next();
		while(objIt.hasNext()){
			object = object + "+" + objIt.next();
		}

		relationships.append(subject+"@"+pred+"@"+object+";");
	}

	public void columnTypes(ArrayList<String> header){
		propHash.put("NUM_COLUMNS", Integer.toString(header.size()));
		propFile.append("NUM_COLUMNS" + "\t" + Integer.toString(header.size()) + "\n");
		for(int i = 0; i < header.size(); i++)
		{
			if(header.get(i) != null && dataTypeHash.containsKey(header.get(i)))
			{
				propHash.put(Integer.toString(i+1), dataTypeHash.get(header.get(i)));
				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ":" + dataTypeHash.get(header.get(i)));
				propFile.append(Integer.toString(i+1) + "\t" + dataTypeHash.get(header.get(i)) + "\n");
			}
			else
			{
				propHash.put(Integer.toString(i+1), "STRING");
				System.out.println(header.get(i) + ":" + Integer.toString(i+1) + ": STRING");
				propFile.append(Integer.toString(i+1) + "\tSTRING\n");
			}
		}
	}

	public void constructPropHash(String startRowNumAsString, String endRowNumAsString){
		propHash.put("START_ROW", startRowNumAsString);
		propHash.put("END_ROW", endRowNumAsString);
		propHash.put("NOT_OPTIONAL", ";");
		propHash.put("RELATION", relationships.toString());
		propHash.put("NODE_PROP", node_properties.toString());
		propHash.put("RELATION_PROP", "");
		
		propFile.append("RELATION\t" + relationships.toString() + "\n");
		propFile.append("NODE_PROP\t" + node_properties.toString() + "\n");
		propFile.append("RELATION_PROP\t \n");
	}

	public Hashtable<String, String> getPropHash(String startRowNumAsString, String endRowNumAsString) {
		constructPropHash(startRowNumAsString, endRowNumAsString);
		return propHash;
	}
	
	public String getPropFile() {
		return propFile.toString();
	}
}
