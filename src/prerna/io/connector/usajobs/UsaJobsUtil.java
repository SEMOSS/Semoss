package prerna.io.connector.usajobs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;
import prerna.util.Utility;

public class UsaJobsUtil {

	ObjectMapper mapper = null;
	JmesPath<JsonNode> jmespath = new JacksonRuntime();
	int total = -1;
	int page = 1;
	int recCount = 0;
	String email = null;
	String key = null;
	String host = null;
	HttpClient client = new DefaultHttpClient();
	FileWriter curFile = null;
	boolean header = false;


	
//	public static void main(String[] args)
//	{
//		String date = getToday("_");
//		UsaJobsUtil test = new UsaJobsUtil("c:/temp/output" + date + ".csv");
//		//test.runStateSearch(new String[]{"CA", "CO", "CT", "GA", "IN", "KY", "MN", "MI", "NV", "NH", "NY", "OH", "VA", "WI"});
//		List <String> states = new ArrayList<String>();
//		states.add("VA");
//		List <String> cred = new ArrayList<String>();
//		cred.add("prabhuk12@gmail.com");
//		cred.add("v1y+niWf2+6TDnDU/uXyqtojViOvLryPIhBO7WfG5So=");
//		cred.add("data.usajobs.gov");
//
//		// make sure the credential is fulled properly
//		test.runStateSearch(cred, states); //", "CO", "CT", "GA", "IN", "KY", "MN", "MI", "NV", "NH", "NY", "OH", "VA", "WI"});
//	}
	
	public UsaJobsUtil(String fileName)
	{
		openFile(fileName);
	}
	
	
	private void setCredentials(List credentials)
	{
		email = credentials.get(0).toString();
		key = credentials.get(1).toString();
		host = credentials.get(2).toString();
	}
	
	public void runStateSearch(List credentials, List states)
	{
		// TODO Auto-generated method stub
		try {
			//"SearchResultCount":25,"SearchResultCountAll":1590
			// PositionEndDate, PositionStartDate
			// MinimumRange":"62556.0","MaximumRange":"80721.0"
			// PublicationStartDate":"2020-02-03","ApplicationCloseDate":"2021-01-28",
			// "PositionID":"TI-FBIJOBS1627023-17"
			// LocationName":"Atlanta, Georgia","CountryCode":"United States","CountrySubDivisionCode":"Georgia","CityName":"Atlanta, Georgia"
			
			// total count - SearchResult.SearchResultCountAll
			// Position ID - SearchResult.SearchResultItems[*].MatchedObjectDescriptor.PositionID
			// Position End Date - ".PositionEndDate
			// JobGrade - ".JobGrade
			
			// jmes path
			// SearchResult.SearchResultItems[*].MatchedObjectDescriptor.[PositionID, PositionEndDate, JobGrade[0].Code, length(PositionLocation)]
			
			
			setCredentials(credentials);
			
			String url = "https://data.usajobs.gov/api/search?Keyword=software";
			for(int stateIndex = 0;stateIndex < states.size();stateIndex++)
			{
				String state = (String)states.get(stateIndex);
				System.err.println("Running state " + state);
				total = -1;
				page = 1;
				url = "https://data.usajobs.gov/api/search?LocationName=" + URLEncoder.encode(state, java.nio.charset.StandardCharsets.UTF_8.toString());
				/*
				 * System.err.println(" >> "); System.err.println(output); String fileName =
				 * "c:/Temp/output.txt"; FileUtils.writeStringToFile(new File(fileName) ,
				 * output); System.err.println("<<");
				 */
				String output = null;
				do
				{
					output = getUrlData(url);
					if(output != null)
						processJob(output, true, state);
				}while(output != null);
			}				
			closeFile();
			System.err.println("Total Records .. !! " + recCount);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
	private ObjectMapper getMapper()
	{
		if(this.mapper == null)
			mapper = new ObjectMapper();

		return mapper;
	}
	
	public String [] processJob(String inputData, boolean text, String state)
	{
		try {
			// takes the job 
			// processes it as record with the following fields
			// get the mapper
			getMapper();
			JsonNode node = null;
			//read the file
			if(!text)
			{
				File file = new File(Utility.normalizePath(inputData));
				node = mapper.readTree(file);
			}
			else
			{
				node = mapper.readTree(inputData);
			}
			
			String path = "SearchResult.SearchResultItems[*].MatchedObjectDescriptor.[PositionID, PositionEndDate, JobGrade[0].Code, PositionLocation[*]]";
			// hiring path says if it is open to publix or not
			path = "SearchResult.SearchResultItems[*].MatchedObjectDescriptor.[PositionID, PositionEndDate, JobGrade[*].Code, PositionLocation[].CountrySubDivisionCode, OrganizationName, PositionRemuneration[*].MinimumRange, PositionRemuneration[*].MaximummumRange, "
					+ "PositionTitle, "
					+ "UserArea.Details.LowGrade, UserArea.Details.HighGrade, UserArea.Details.HiringPath, UserArea.Details.Relocation, UserArea.Details.OrganizationCodes]"; // hiring path (array) -public or not as well as graduate or not, relocation true / false, org code - high level org - DOJ etc. 
			Expression<JsonNode> expression = jmespath.compile(path);
			JsonNode result = expression.search(node);
			//System.out.println("Result .. " + result);
			
			if(result instanceof ArrayNode)
			{
				StringBuffer record = new StringBuffer();
				
				if(!header)
				{
					record.append("Generated_On").append(",");
					record.append("State").append(",");
					record.append("Position_ID").append(",");
					record.append("End_Date").append(",");
					record.append("SubDivision").append(",");
					record.append("Agency").append(",");
					record.append("Salary_Minimum").append(",");
					record.append("Salary_Maximum").append(",");
					record.append("Position_Name").append(",");
					record.append("Job_Grade_Low").append(",");
					record.append("Job_Grade_High").append(",");
					record.append("Hiring_Path").append(",");
					record.append("Relocation").append(",");
					record.append("Organization").append(",");
					
					writeRecord(record.toString());
					header = true;
				}
				ArrayNode parNode = (ArrayNode)result;
				System.err.println(" Total on this run " + parNode.size());
				
				// need something here to write the headers
				
				for(int parIndex = 0;parIndex < parNode.size();parIndex++)
				//if(parNode.size() > 0)
				{
					ArrayNode arrNode = (ArrayNode)parNode.get(parIndex);
					//for(int recIndex = 0;recIndex < arrNode.size();recIndex++)
					{
						record = new StringBuffer();
						record.append(getToday("/")).append(",");
						record.append(state).append(",");
						String positionID = arrNode.get(0) + "";
						record.append(positionID).append(",");
						String positionEndDate = "\""+arrNode.get(1).asText() + "\"";
						record.append(positionEndDate).append(",");
						String grade = arrNode.get(2) + "";
						grade = formatArray(grade, false);
						String place = arrNode.get(3) + "";
						place = formatArray(place, true);
						record.append(place).append(",");
						String org = "\"" + arrNode.get(4).asText() + "\"";
						record.append(org).append(",");
						String minAmount = arrNode.get(5) + "";
						minAmount = formatArray(minAmount, false);
						record.append(minAmount).append(",");
						String maxAmount = arrNode.get(6) + "";
						maxAmount = formatArray(maxAmount, false);
						record.append(maxAmount).append(",");
						String title = "\"" + arrNode.get(7).asText()  + "\"";
						record.append(title).append(",");
						String lowGrade = arrNode.get(8).asText();
						record.append(grade).append(lowGrade).append(",");
						String highGrade = arrNode.get(9).asText();
						record.append(grade).append(highGrade).append(",");
						String openTo = arrNode.get(10) + "";
						openTo = formatArray(openTo, true);
						record.append(openTo).append(",");
						String reloc =  arrNode.get(11) + "";
						record.append(reloc).append(",");
						String orgCode = "\"" + arrNode.get(12).asText() + "\"";
						record.append(orgCode); // ending here

						
						writeRecord(record.toString());
						//System.out.println(recCount + " >>  Position " + positionID + " Title  " + title + "  end Date " + positionEndDate +  "  Organization " + org + " Min Amount "+ minAmount + " Max Amount " + maxAmount); 
						recCount++;
					}
				}
			}
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
		
	}
	
	private void openFile(String fileName)
	{
		try
		{
			File file = new File(Utility.normalizePath(fileName));
			file.getParentFile().mkdirs();
			curFile = new FileWriter(file);
		}catch(Exception ex)
		{
			ex.printStackTrace();			
		}
	}
	
	public static String getToday(String separator)
	{
		try {
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy" + separator + "MM" + separator + "dd");  
			LocalDateTime now = Utility.getLocalDateTimeUTC(LocalDateTime.now());
			//System.out.println(dtf.format(now));
			return dtf.format(now);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		return null;
	}
	
	private void writeRecord(String record)
	{
		try
		{
			curFile.write(record);
			curFile.write("\n");
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void closeFile()
	{
		try {
			curFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String formatArray(String arrayString, boolean doQuote)
	{
		String quote = "\"";
		if(!doQuote)
			quote = "";
		if(doQuote)
			arrayString = arrayString.replace("\"", "'");
		else
			arrayString = arrayString.replace("\"", "");			
		arrayString = arrayString.replace("[", quote);
		arrayString = arrayString.replace("]", quote);
		
		return arrayString;
	}
	
	public int getTotalResults(String inputData)
	{
		if(total < 0)
		{
			getMapper();
			try {
				JsonNode node = null;
				//read the file
				node = mapper.readTree(inputData);
				String path = "SearchResult.SearchResultCountAll";
				Expression<JsonNode> expression = jmespath.compile(path);
				JsonNode result = expression.search(node);
				System.out.println("Result .. " + result.asInt());
				total = result.asInt();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		return total;
	}
	
	private void printCurCount(String inputData)
	{
		try {
			JsonNode node = mapper.readTree(inputData);
			String path = "SearchResult.SearchResultCount";
			Expression<JsonNode> expression = jmespath.compile(path);
			JsonNode result = expression.search(node);
			System.out.println("Current count is .. " + result.asInt());

		}catch(Exception ex)
		{
			
		}
	}
	
	// get the url to play
	private String getUrlData(String url)
	{
		try {
			String retUrl = null;
			if((page -1)* 500 < total || total < 0)
			{
				url = url + "&Page=" + page + "&ResultsPerPage=500";
				System.err.println("Firing URL " + url);
				HttpGet request = new HttpGet(url);
				request.setHeader("User-Agent", email);
				request.setHeader("Host", host);
				request.setHeader("Authorization-Key", key);
				
				HttpResponse response = client.execute(request);
				String output = EntityUtils.toString(response.getEntity());
				getTotalResults(output);
				printCurCount(output);
				page++;
				return output;
			}
			return retUrl;
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	public void getBudgetByTime()
	{
		// you can see how much every state is spending
		// you can get month by month
		// and see how the state fluctuates
		// you can see how much money remains in every agency - there is no time to it - but this can tell you how much money remains - You can see what the total is and then prorate it based on state fluctuations
		
		/*
		 * 
		 POST URL - https://api.usaspending.gov/api/v2/search/spending_by_geography/
		 
		 {
      "filters": {
          "keywords": ["Filter is required"],
		  "time_period": [
							{
							"start_date": "2017-10-01",
							"end_date": "2018-09-30"
							}
						]
      		    },
      "scope": "place_of_performance",
      "geo_layer": "state",
      "geo_layer_filters": ["VA", "CA"]
      
      */
	
		// agency wide spending - https://api.usaspending.gov/api/v2/references/toptier_agencies/
		// you can see obligated - authorized amount - still to spend
		
		
		
	}
}
		 
