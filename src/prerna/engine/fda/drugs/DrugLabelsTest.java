package prerna.engine.fda.drugs;

import java.io.InputStreamReader;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DrugLabelsTest {

	private static Gson gson = new GsonBuilder()
			.disableHtmlEscaping()
			.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
			.setPrettyPrinting()
			.create();

	public static void main(String[] args) {

		/*
		 * We have the following high level fields:
		 * 
		 * headers which contains some information about the event
		 * patient which contains details about the patient
		 * drugs which contains drugs taken while the event was happening
		 * reactions is information about the reaction the patient experienced
		 * 
		 * 
		 */
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		String url = "https://api.fda.gov/drug/label.json";
//		printResponse(httpclient, url,  "");
		
		// lets look at how we can narrow down what we want
		url += "?limit=2?search=";

		// brand name
		String component = "brand_name:ADVIL";
//		printResponse(httpclient, url,  component);
		
		// substance name
		// for advil, it is IBUPROFEN
		component = "substance_name:IBUPROFEN";
//		printResponse(httpclient, url,  component);
		
		component = "manufacturer_name:CVS";
		printResponse(httpclient, url,  component);
	}

	private static void printResponse(CloseableHttpClient httpclient, String baseUrl, String params) {
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + params);
		HttpResponse response = null;
		InputStreamReader isr = null;
		try {
			HttpGet getRequest = new HttpGet(baseUrl + params);
			response = httpclient.execute(getRequest);
			isr = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Map<String, Object> jsonMap = new Gson().fromJson(isr, new TypeToken<Map<String, Object>>() {}.getType());
		System.out.println(gson.toJson(jsonMap));
	}

}
