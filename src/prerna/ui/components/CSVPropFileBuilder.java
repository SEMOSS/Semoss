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

	public void addProperty(ArrayList<String> sub, ArrayList<String> obj) {
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

		node_properties.append(subject.substring(0, subject.length()-1)+"@"+object.substring(0, object.length()-1)+";");
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

	public void columnDecomp(String file){
		try {
			ICsvListReader listReader = new CsvListReader(new FileReader(file), CsvPreference.STANDARD_PREFERENCE);
			String[] header = listReader.getHeader(true);
			propHash.put("NUM_COLUMNS",String.valueOf(header.length));

			// exception handling to determine type of input
			String[] colInstance = new String[header.length];
			List<String> instances = listReader.read();
			Boolean needMoreInstanceData = true;
			while((instances = listReader.read()) != null && needMoreInstanceData){
				for(int i = 0; i < header.length; i++){
					if(instances.get(i) != null && colInstance[i] == null){
						colInstance[i] = instances.get(i).toString();
					}
				}
				if(!Arrays.asList(colInstance).contains(null)){
					needMoreInstanceData = false;
				}
			}

			for(int i = 0; i < colInstance.length; i++){
				//Boolean isInt = isInteger(colInstance[i]);
				String processor = determineProcessor(colInstance[i]);
				propHash.put(String.valueOf(i+1), processor);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}


	public String determineProcessor(String s) {
		String processor = "";
		boolean isInt = true;
		try { 
			Integer.parseInt(s); 
		} catch(NumberFormatException e) { 
			isInt = false;
		}

		if(isInt){
			return (processor = "NUMBER");
		}

		boolean isDouble = true;
		try {
			Double.parseDouble(s);
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return (processor = "DECIMAL");
		}
		
		//TODO: combine determining long date vs. simple date into a loop

		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		Date longdate = null;
		try {
			formatLongDate.setLenient(true);
			longdate  = formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}

		if(isLongDate){
			return (processor = "DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("mm/dd/yyyy");
		Date simpleDate = null;
		try {
			formatSimpleDate.setLenient(true);
			simpleDate  = formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}

		if(isSimpleDate){
			return (processor = "SIMPLEDATE");
		}

		if(Boolean.parseBoolean(s)){
			return (processor = "BOOLEAN");
		}

		return (processor = "STRING");
	}

	public static boolean isDate(String s) {
		Boolean isDate = false;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		Date date = null;
		try {
			format.setLenient(false);
			date  = format.parse(s);
		} catch (ParseException e) {
		}

		if(date != null){
			isDate = true;
		}

		return isDate;
	}


	public void writeProp(){
		String propFile = "";

		propFile += propFile + 
				"RELATION\t" + relationships.toString() + "\n\n" +
				"NODE_PROP\t" + node_properties.toString() + "\n\n";

		System.out.println(propFile);
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
