package prerna.io.connector.google;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.AbstractHttpHelper;

public class GoogleFileRetriever implements IConnectorIOp{

	@Override
	public Object execute(User user, Hashtable params) {
		
		
		String fileName = (String)params.remove("target");
		
		try {
			String url_str = "https://docs.google.com/spreadsheets/export"; 
			//System.out.println("....");
			
			BufferedReader br = AbstractHttpHelper.getHttpStream(url_str, null, params, false);
			
			// create a file
			File outputFile = new File(fileName);
			
			BufferedWriter target = new BufferedWriter(new FileWriter(outputFile));
			String data = null;
			
			
			while((data = br.readLine()) != null)
			{
				target.write(data);
				target.write("\n");
				target.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return fileName;
	}

	// https://docs.google.com/spreadsheets/export?id=1it40jNFcRo1ur2dHIYUk18XmXdd37j4gmJm_Sg7KLjI&exportFormat=csv
}
