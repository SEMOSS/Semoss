package prerna.reactor.utils;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.ds.py.PyExecutorThread;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ValidatePyReactor extends AbstractReactor {
	
	public ValidatePyReactor() {
		this.keysToGet = new String[]{"script"};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		
		String result = "";
		
		try {
			String fileName = insight.getInsightFolder() + "/" + keyValue.get(keysToGet[0]);
			fileName = fileName.replaceAll("\\\\", "/");
			PyExecutorThread py = insight.getPy();
			
			Object lock = py.getMonitor();
			String command = "smssutil.canLoad(\"" + fileName + "\")";
			py.command = new String [] {"import smssutil", command};

			synchronized(lock)
			{
				try
				{
					lock.notify();
					lock.wait();
				}catch(InterruptedException ex)
				{
				}
					
				Hashtable output = py.response;
				ArrayList list = (ArrayList)output.get(command);
				
				if(list.size() == 0)
					result = keyValue.get(keysToGet[0]) + " : All Libraries available";
				else
				{
					StringBuilder library = new StringBuilder(keyValue.get(keysToGet[0])).append(":  Missing Libraries [");
					for(int arrIndex = 0;arrIndex < list.size();arrIndex++)
						library.append(list.get(arrIndex)).append(", ");
					result = library.substring(0,  library.length() -2) + "]";
					
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(result, PixelDataType.CONST_STRING);
	}
}
