package prerna.ui.components;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

public class CSVPropFileBuilder{

	private StringBuilder relationships = new StringBuilder();
	private StringBuilder node_properties = new StringBuilder();

	private Hashtable<String, String> propHash = new Hashtable<String, String>();
	private Hashtable<String, String> dataTypeHash = new Hashtable<String, String>();
	
	public void addProperty(ArrayList<String> sub, ArrayList<String> obj, String dataType) {
		String subject = "";
		String object = "";

		Iterator<String> subIt = sub.iterator();
		while(subIt.hasNext()){
			subject = subject + subIt.next() + "+";
		}
		subject = subject.substring(0, subject.length()-1);
		Iterator<String> objIt = obj.iterator();
		while(objIt.hasNext()){
			object = object + objIt.next() + "+";
		}
		object = object.substring(0, object.length()-1);
		
		dataTypeHash.put(object, dataType);
		node_properties.append(subject+"%"+object+";");
	}

	public void addRelationship(ArrayList<String> sub, String pred, ArrayList<String> obj) {
		String subject = "";
		String object = "";

		Iterator<String> subIt = sub.iterator();
		while(subIt.hasNext()){
			subject = subject + subIt.next() + "+";
		}

		Iterator<String> objIt = obj.iterator();
		while(objIt.hasNext()){
			object = object + objIt.next() + "+";
		}

		relationships.append(subject.substring(0, subject.length()-1)+"@"+pred+"@"+object.substring(0, object.length()-1)+";");
	}

	public void columnTypes(String[] header){
		for(int i = 0; i < header.length; i++)
		{
			if(dataTypeHash.containsKey(header[i]))
			{
				propHash.put(Integer.toString(i+1), dataTypeHash.get(header[i]));
			}
			else
			{
				propHash.put(Integer.toString(i+1), "STRING");
			}
		}
	}

	public void constructPropHash(){
		propHash.put("RELATION", relationships.toString());
		propHash.put("NODE_PROP", node_properties.toString());
		propHash.put("RELATION_PROP", "");
		propHash.put("NOT_OPTIONAL", ";");
		propHash.put("START_ROW","2");
		propHash.put("END_ROW","100000");
	}

	public Hashtable<String, String> getPropHash() {
		constructPropHash();
		return propHash;
	}
}
