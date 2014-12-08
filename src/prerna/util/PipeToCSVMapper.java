package prerna.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

public class PipeToCSVMapper {
	
	public static void main(String [] args) throws Exception
	{
		String srcFile = "C:/Users/pkapaleeswaran/Desktop/From C Drive Root/RFP/scienergy/morbidmapnew.txt";
		String destFile = "C:/Users/pkapaleeswaran/Desktop/From C Drive Root/RFP/scienergy/morbidmapnew-csv.csv";
		// takes a source file
		// takes a destination file and for each field maps to the new format
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(srcFile)));
		//BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destFile)));
		PrintWriter writer2 = new PrintWriter(new File(destFile));
		
		String input = null;
		//input = reader.readLine();
		while((input = reader.readLine()) != null)
		{
			System.err.println("Processing " + input);
			//input = "1.14#3#23#14#1p36.33#CPSF3L, INTS11, RC68#P#Cleavage and polyadenylation-specific factor 3-like, trial2##611354#REc#### # ##";
			String [] tokens2 = input.split("#");
			//System.err.println("Tokens 2 " + tokens2.length);
			//StringTokenizer tokens = new StringTokenizer(input, ";");
			Hashtable hiColumns = new Hashtable();
			Vector inputVector = new Vector();
			//String storySoFar = "";
			int count = 0;
			String [] storySoFar = new String[tokens2.length];
			//System.err.println("Token Count " + tokens.countTokens());
			for(int tokIndex = 0;tokIndex < tokens2.length;tokIndex++)
			{
				String thisToken = tokens2[tokIndex];
				//System.err.println("Token is " + thisToken);
				if(thisToken.indexOf(",") >= 0)
				{
					// add this guy along with the index into some place
					hiColumns.put(count+"", thisToken);
				}
				else
					storySoFar[count] = thisToken;
				count++;
			}
			inputVector.add(storySoFar);
			
			// flattening the comma seperated columns
			if(hiColumns.size() >= 0)
			{
				// need to flatten each of it
				inputVector = flattenArray(inputVector, hiColumns);
			}
			
			// flattening array into csv
			for(int inputIndex = 0;inputIndex < inputVector.size();inputIndex++)
			{
				String normalizedString = "";
				String [] arr = (String[])inputVector.get(inputIndex);
				//System.out.println("Array Length is " + arr.length);
				for(int arrIndex = 0;arrIndex < arr.length;arrIndex++)
				{
					if(arrIndex == 0)
						normalizedString = arr[arrIndex].trim();
					normalizedString = normalizedString + "," + arr[arrIndex].trim();
				}				
				writer2.println(normalizedString);	
				writer2.flush();
				System.err.println("Normalized String " + normalizedString);
			}
		}
	}
	
	public static Vector flattenArray(Vector inputVector, Hashtable hiColumns)
	{
		if(hiColumns.size() != 0)
		{
			String key = (String)hiColumns.keys().nextElement();
			String value = (String)hiColumns.get(key);
			hiColumns.remove(key);
			int index = Integer.parseInt(key);
			
			StringTokenizer tokens = new StringTokenizer(value, ",");
			Vector newInputVector = new Vector();
			while(tokens.hasMoreTokens())
			{	
				String data = tokens.nextToken();
				for(int curIndex = 0;curIndex < inputVector.size();curIndex++)
				{
					String [] array = (String [])inputVector.get(curIndex);
					String [] newArray = new String[array.length];
					
					// copy it in
					System.arraycopy(array, 0, newArray, 0, array.length);
					
					// set new data
					newArray[index] = data;
					
					// set the vector
					newInputVector.add(newArray);
					//curIndex++;
				}			
			}
			return flattenArray(newInputVector, hiColumns);
		}
		else
			return inputVector;
	}

}
