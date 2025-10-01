package prerna.reactor.agent.mcp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleMCPClient {

	private static final Logger classLogger = LogManager.getLogger(GetMCPInternalToolsReactor.class);
	static String clientName = "Semoss";
	static long id = 1;	
		
	public static HttpURLConnection connect(String mcpEndpoint, String bearerToken)
	{
		HttpURLConnection connection = null;
		try
		{
			URL url = new URI(mcpEndpoint).toURL();
			
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			// Set the request method (e.g., GET, POST, PUT)
			connection.setRequestMethod("POST");
			// Add custom headers
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Authorization", bearerToken);

			// Optional: Set connection and read timeouts
			connection.setConnectTimeout(5000); // 5 seconds
			//this.mcpOutputStream = connection.getOutputStream();
			//this.bw = new BufferedWriter(new OutputStreamWriter(this.mcpOutputStream));
			//this.mcpInputStream = connection.getInputStream();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}
	
	
	public static String initMCP(String mcpEndpoint, String bearerToken)
	{
		HttpURLConnection connection = connect(mcpEndpoint, bearerToken);
		String initMessage = "{\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},\"clientInfo\":{\"name\":\"" + clientName + "\",\"version\":\"0.1.0\"}},\"jsonrpc\":\"2.0\",\"id\":" + id + "}";
		write(initMessage, connection);
		id++;
		return read(connection);
	}

	public static String getTools(String mcpEndpoint, String bearerToken)
	{
		HttpURLConnection connection = connect(mcpEndpoint, bearerToken);
		String toolMessage = "{\"method\":\"tools/list\",\"params\":{},\"jsonrpc\":\"2.0\",\"id\": " + id  +"}";
		write(toolMessage, connection);
		id++;
		return read(connection);

	}
	
	public static String getResources(String mcpEndpoint, String bearerToken)
	{
		//{"method":"resources/list","params":{},"jsonrpc":"2.0","id":3}
		HttpURLConnection connection = connect(mcpEndpoint, bearerToken);
        String resourceMessage = "{\"method\":\"resources/list\",\"params\":{},\"jsonrpc\":\"2.0\",\"id\": " + id  +"}";
		write(resourceMessage, connection);
		id++;
		return read(connection);
	}
	
	public static String getPrompts(String mcpEndpoint, String bearerToken)
	{
		//{"method":"resources/list","params":{},"jsonrpc":"2.0","id":3}
		HttpURLConnection connection = connect(mcpEndpoint, bearerToken);
        String promptMessage = "{\"method\":\"prompts/list\",\"params\":{},\"jsonrpc\":\"2.0\",\"id\": " + id  +"}";
		write(promptMessage, connection);
		id++;
		return read(connection);
	}

	
	public static String callTool(String mcpEndpoint, String bearerToken, String toolCallMessage)
	{
		HttpURLConnection connection = connect(mcpEndpoint, bearerToken);
		//{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_stock_price","arguments":{"symbol":"GOOG"}}}
		write(toolCallMessage, connection);
		id++;
		return read(connection);
	}
	
	private static void write(String jsonString, HttpURLConnection connection)
	{
		classLogger.info("Writing .. " + jsonString);
		try {
			//mcpOutputStream.write(jsonString.getBytes());
			//mcpOutputStream.flush();
			//this.mcpOutputStream = connection.getOutputStream();
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
	        bw.write(jsonString);
	        bw.flush();
	        bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String read(HttpURLConnection connection)
	{
		 String output = null;
		 //System.err.println("starting thread");
		 boolean preLine = true;
		 BufferedReader in = null;
		 boolean done = false;
		 try {
			 in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			 while((output = in.readLine()) != null  && !done)
			 {
				 if(output.length() > 0 && preLine)
					 preLine = false;
				 else if(output.length() == 0 &&  preLine)
					 done = true;
				 classLogger.info(output);
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		 {
			try {
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 
		//System.err.println("closing thread");
		 return output;
				 
	}
	
	public static void main(String []args)
	{
		String serverUrl = "https://workshop.cfg.deloitte.com/cfg-ai-dev/Monolith/api/ext/mcp/adbc3ae3-f80c-48fb-ab18-d9e8849c4fc6/comms";
		String bearerToken = "Bearer2d950c28-a710-418d-83b4-68d9b1572e92:edc76483-4f09-4cb6-b747-7314b87283e6";
		SimpleMCPClient client = new SimpleMCPClient();
		
		//client.connect();
		
		//client.initMCP();
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		client.getTools(serverUrl, bearerToken);
		
		//Thread thread = new Thread(client);
		//thread.start();
		
		
	}
	
	// ---------------------  graveyard --------------------
	
	public static void main2(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		String serverUrl = "https://workshop.cfg.deloitte.com/cfg-ai-dev/Monolith/api/ext/mcp/adbc3ae3-f80c-48fb-ab18-d9e8849c4fc6/comms";
		
		
		URL url = new URL(serverUrl);
	
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        // Set the request method (e.g., GET, POST, PUT)
        connection.setRequestMethod("POST");

        // Add custom headers
        connection.setRequestProperty("User-Agent", "Mozilla/5.0");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", "Bearer2d950c28-a710-418d-83b4-68d9b1572e92:edc76483-4f09-4cb6-b747-7314b87283e6");

        // Optional: Set connection and read timeouts
        connection.setConnectTimeout(5000); // 5 seconds
        //connection.setReadTimeout(5000);    // 5 seconds
        String data = "{\"method\":\"tools/list\",\"params\":{},\"jsonrpc\":\"2.0\",\"id\":1}";

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
        bw.write(data);
        //bw.close();
        data = "{\"method\":\"tools/list\",\"params\":{},\"jsonrpc\":\"2.0\",\"id\":2}";
        
        
        bw = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
        bw.write(data);
        bw.write(data);
        bw.flush();
        
        SimpleMCPClient client = new SimpleMCPClient();
        //client.mcpInputStream = connection.getInputStream();
        //Thread t = new Thread(client);
        //t.start();


        
        
        // Get the response code
        int responseCode = connection.getResponseCode();
        System.out.println("Response Code: " + responseCode);
        // Read the response
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        
        while ((inputLine = in.readLine()) != null) 
        {
            //response.append(inputLine);
            System.err.println(inputLine);
        }
//        in.close();

        // Print the response
      //  System.out.println("Response Body: " + response.toString());
		
		
				
	}
	
}
