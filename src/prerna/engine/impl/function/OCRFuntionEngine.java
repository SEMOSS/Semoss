package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClient;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.ai.formrecognizer.documentanalysis.models.AnalyzeResult;
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentLine;
import com.azure.ai.formrecognizer.documentanalysis.models.OperationResult;
import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;

import prerna.util.Constants;


public class OCRFuntionEngine extends AbstractFunctionEngine{
	
	private static final Logger classLogger = LogManager.getLogger(OCRFuntionEngine.class);
	
	private String engineId;
	private String engineName;
	private String connectionUrl;
	private String apiKey;		
	private List<String> requiredParameters = new ArrayList<String>();
	DocumentAnalysisClient documentAnalysisClient = null;	
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		this.connectionUrl = smssProp.getProperty("URL");
		this.apiKey = smssProp.getProperty("API_KEY");	
		this.requiredParameters.add(smssProp.getProperty("REQUIREDPARAMETERS").toString());
				
		if(this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()){
			throw new RuntimeException("Must pass in an access key");
		}		
		if(this.apiKey == null || this.apiKey.isEmpty()){
			throw new RuntimeException("Must pass in a secret key");
		}	
		
		try {
		this.documentAnalysisClient = new DocumentAnalysisClientBuilder()
       	     .credential(new AzureKeyCredential(this.apiKey))
       	     .endpoint(this.connectionUrl)
       	     .buildClient();	
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}	

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Object output = null;
		List<String> extractedTextFromDoc = new ArrayList<String>(); 
		StringBuffer extractedTextForeachLine = new StringBuffer();		
		// validate all the required keys are set
		if(this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for(String requiredP : this.requiredParameters) {
				if(!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if(!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}
		try {
			for(String k : parameterValues.keySet()) {
				 File document = new File(parameterValues.get(k).toString());
			        SyncPoller<OperationResult, AnalyzeResult> analyzeResultPoller =
			            this.documentAnalysisClient.beginAnalyzeDocument("prebuilt-read",
			                BinaryData.fromFile(document.toPath(),
			                    (int) document.length()));
			        AnalyzeResult analyzeResult = analyzeResultPoller.getFinalResult(); 
			        analyzeResult.getPages().forEach(documentPage -> {      	     
			            // line          
			            for(DocumentLine documentLine :documentPage.getLines()) {
			            	extractedTextForeachLine.append(documentLine.getContent());            	 
			            }  
			            extractedTextFromDoc.add(extractedTextForeachLine.toString());
			            extractedTextForeachLine.setLength(0);
			        });
			    		      
			        output = extractedTextFromDoc;
			}
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		
		return output;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
