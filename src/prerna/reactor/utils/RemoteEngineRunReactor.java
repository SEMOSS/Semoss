package prerna.reactor.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Constants;
import prerna.util.Utility;

public class RemoteEngineRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RemoteEngineRunReactor.class);
	
	public RemoteEngineRunReactor() {
		this.keysToGet = new String [] {ReactorKeysEnum.PAYLOAD.getKey()}; // just get this as string and turn it into json
		this.keyRequired = new int [] {1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		// get the PayloadStruct string
		String message = Utility.decodeURIComponent(keyValue.get(ReactorKeysEnum.PAYLOAD.getKey()));
		PayloadStruct ps = new Gson().fromJson(message, PayloadStruct.class);
		ps = convertPayloadClasses(ps);
		
		// run the engine call
		NativePyEngineWorker pyw = new NativePyEngineWorker(this.insight.getUser(), ps, this.insight);
		pyw.run();

		// retrieve the output
		PayloadStruct output = null;
		try {
			output = pyw.getOutput();
			output.payloadClasses = null;
		} catch (Exception ex) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			output.response = true;
			output.ex = sw.toString();
		}
		
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
    private PayloadStruct convertPayloadClasses(PayloadStruct input) {
    	if(input.payloadClassNames != null) {
    		input.payloadClasses = new Class[input.payloadClassNames.length];
    		for(int classIndex = 0;classIndex < input.payloadClassNames.length;classIndex++) {
    			try {
					String className = input.payloadClassNames[classIndex];
					input.payloadClasses[classIndex] = Class.forName(className);
					if(input.payloadClasses[classIndex] == Insight.class)
					{
						Insight insight = this.insight;
						input.payload[classIndex] = insight;
					}
				} catch (ClassNotFoundException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
    		}
    	}
    	
    	return input;
    }
}
