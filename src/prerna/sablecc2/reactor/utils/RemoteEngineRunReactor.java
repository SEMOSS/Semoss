package prerna.sablecc2.reactor.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.google.gson.Gson;

import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;

public class RemoteEngineRunReactor extends AbstractReactor {

	public RemoteEngineRunReactor()
	{
		//this.keysToGet = new String[] {ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.METHOD_NAME.getKey(), ReactorKeysEnum.PAYLOAD_CLASSES.getKey(), ReactorKeysEnum.PAYLOAD.getKey()};		
		this.keysToGet = new String [] {ReactorKeysEnum.PAYLOAD.getKey()}; // just get this as string and turn it into json
		this.keyRequired = new int[] {1};
	}
	
	
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		/*
		//abandoning this to enable a simpler string
		PayloadStruct execPayload = new PayloadStruct();
		execPayload.insightId = insight.getInsightId();
		execPayload.engineType = this.getNounStore().getNoun(keysToGet[0]).get(0).toString();
		execPayload.engineType = this.getNounStore().getNoun(keysToGet[0]).get(0).toString();
		
		*/
		
		organizeKeys();
		String message = keyValue.get(keysToGet[0]);
		Gson gson = new Gson();
		PayloadStruct ps = gson.fromJson(message, PayloadStruct.class);
		ps = convertPayloadClasses(ps);
		NativePyEngineWorker pyw = new NativePyEngineWorker(this.insight.getUser(), ps);
		pyw.run();
		String strOutput = "Evaluating";
		PayloadStruct output = null;
		try
		{
			output = pyw.getOutput();
			output.payloadClasses = null;
			strOutput = gson.toJson(output);
		}catch (Exception ex)
		{
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			strOutput = sw.toString(); // stack trace as a string
			ps.response = true;
			ps.ex = strOutput;
			strOutput = gson.toJson(ps);			
		}
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
    private PayloadStruct convertPayloadClasses(PayloadStruct input)
    {
    	if(input.payloadClassNames != null)
    	{
    		input.payloadClasses = new Class[input.payloadClassNames.length];
    		for(int classIndex = 0;classIndex < input.payloadClassNames.length;classIndex++)
    		{
    			try {
					String className = input.payloadClassNames[classIndex];
					input.payloadClasses[classIndex] = Class.forName(className);
					if(input.payloadClasses[classIndex] == Insight.class)
					{
						String insightId = "" + input.payload[classIndex]; 
						Insight insight = this.insight;
						input.payload[classIndex] = insight;
					}
				} catch (ClassNotFoundException e) 
    			{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	return input;
    }


}
