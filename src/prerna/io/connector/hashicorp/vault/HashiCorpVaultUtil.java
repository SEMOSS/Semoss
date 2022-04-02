package prerna.io.connector.hashicorp.vault;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;

public class HashiCorpVaultUtil {

	
	
	public static void main(String[] args) throws VaultException, ParseException, IOException {
		
		final VaultConfig config =
			    new VaultConfig()
			        .address("http://0.0.0.0:8200")              			// Defaults to "VAULT_ADDR" environment variable
			        .token("***REMOVED***")  				// Defaults to "VAULT_TOKEN" environment variable
			        .openTimeout(5)                                 		// Defaults to "VAULT_OPEN_TIMEOUT" environment variable
			        .readTimeout(30)                                		// Defaults to "VAULT_READ_TIMEOUT" environment variable
			        .sslConfig(new SslConfig().build())             		// See "SSL Config" section below
			        .build();
		
		final Vault vault = new Vault(config);
		Map<String, String> dataMap = vault.logical().read("secret/SQL%20SERVER%20VHA%20SUPPLY__3831cb1a-4496-46fe-8763-8044a00c04a7").getData();
		System.out.println(dataMap);
		
		configureSecretsEngines();
		
		
		Map<String, Object> newDetails = new HashMap<>();
		newDetails.put("USERNAME", "SA");
		newDetails.put("PASSWORD", "semoss@123123");
		vault.logical().write("db/Sql%20Server%20VHA%20Supply__fe5e2c23-59e6-42ae-939d-b2ca9699f38c", newDetails);
		
		dataMap = vault.logical().read("db/Sql%20Server%20VHA%20Supply__fe5e2c23-59e6-42ae-939d-b2ca9699f38c").getData();
		System.out.println(dataMap);
	}


	public static void configureSecretsEngines() throws ParseException, IOException {
		HttpPost post = new HttpPost("http://0.0.0.0:8200/v1/sys/mounts/db");
		post.setHeader("X-Vault-Token", "***REMOVED***");
		HttpEntity body = new StringEntity("{"
				+ "\"type\":\"kv\", "
				+ "\"description\":\"the secrets for database smss files\","
				+ "\"options\":{\"version\":\"2\"}"
				+ "}") ;
		post.setEntity(body);

		CloseableHttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(post);

		String responseBody = null;
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			responseBody = EntityUtils.toString(entity);
		}

		StatusLine statusLine = response.getStatusLine();
		System.out.println("status line = " + statusLine.getStatusCode());
		System.out.println("response body = " + responseBody);
	}
	
}
