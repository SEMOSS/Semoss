package prerna.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jep.Jep;
import jep.JepException;
import prerna.ds.py.PyExecutorThread;


public class JepTest implements Runnable {

	Jep jep = null;
	static Object pdFrame = null;
	String threadName = null;
	
	public static void main(String [] args) throws Exception
	{
		
		JepTest j1 = new JepTest();
		j1.threadName = "Thread 1";
		Thread t1 = new Thread(j1);
		JepTest j2 = new JepTest();
		j2.threadName = "Thread 2";
		Thread t2 = new Thread(j2);
		Thread t3 = new Thread(new JepTest());
		Thread t4 = new Thread(new JepTest());
		
		Jep jep = new Jep(false);
	    jep.eval("import pandas as pd");
	    jep.eval("\npath = r'C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv'");
	    
	    jep.eval("\nda = pd.read_csv(path)");
	    String data = (String)jep.getValue("\nda.head(3)");
	    pdFrame = jep.getValue("da");
		
	    j1.pdFrame = pdFrame;
	    j2.pdFrame = pdFrame;
		t1.start();
		//Thread.sleep(3000);
		//t2.start();
		//t3.start();
		//t4.start();
		
	}

	public Jep getJep()
	{
		try {
			//if(this.jep == null)
				jep = new Jep(false);
		} catch (JepException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return jep;
	}
	
	public void run()
	{
		// run the commands on a per second basis
		String data = null;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		PyExecutorThread py = new PyExecutorThread();
		Object monitor = py.getMonitor();
		Thread pyThread = new Thread(py);
		pyThread.start();
		
		try {
			while((data = br.readLine()) != null)
			{
				System.out.println(threadName + "  Enter Next Command: ");
				if(!data.equalsIgnoreCase("stop"))
				{
					py.command = new String[]{data};
					synchronized(monitor)
					{
						monitor.notify();
						
						monitor.wait();
						Object newData = py.response.get(data);
						noodleData(newData);
						System.out.println("Got data.. " + newData);
						
					}
				}
				else
				{
					py.process = "stop";
					synchronized(monitor)
					{
						py.command = new String[]{"print 'end'"};
						monitor.notify();					
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void noodleData(Object response)
	{
		if(response instanceof Map)
		{
			HashMap map = (HashMap)response;
			
			ArrayList columns = (ArrayList)map.get("columns");
			
			for(int colIndex = 0;colIndex < columns.size();colIndex++)
			{
				StringBuilder output = new StringBuilder("");
				Object element = columns.get(colIndex);
				System.out.println(element.getClass());
				
				System.out.println(element instanceof List);
				
				List eleList = (List)element;
				System.out.println(eleList.size());
				
				System.out.println(eleList.get(0) + "<<>>" + eleList.get(1));
				
				
				
				//java.util.Collections$UnmodifiableRandomAccessList eleList = (java.util.Collections$UnmodifiableRandomAccessList)element;
			}
			
		}	
	}
	
	public void run2()
	{
		getJep();
		try {
			long start = System.nanoTime();
			System.out.println("Time Start" + System.nanoTime());
		    jep.eval("from java.lang import System");
		    //jep.eval("from pandasql import *");
		    jep.eval("import pandas as pd");
		    jep.eval("import numpy as np");
		    jep.eval("s = 'Hello World'");
		    jep.eval("System.out.println(s)");
		    jep.eval("print(s)");
		    jep.eval("print(s[1:-1])");
		    /*jep.eval("\ndef foo():");
		    jep.eval("\n\t i=5*2");
		    jep.eval("\n\t print(i)");
		    jep.eval("\n\t print(i)");
		    jep.eval("\n null");
		    jep.eval("\nfoo()");
		    */
		    
		    jep.eval("a = np.array([2,3,4])");
		    jep.runScript("C:/Users/pkapaleeswaran/workspacej3/SemossDev/sample.py");
		    if(pdFrame == null)
		    {
			    //jep.eval("\npath = r'C:/Users/pkapaleeswaran/workspacej3/datasets/physican data.csv'");
			    jep.eval("\npath = r'C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv'");
			    
			    jep.eval("\nda = pd.read_csv(path)");
			    String data = (String)jep.getValue("\nda.head(3)");
			    pdFrame = jep.getValue("da");
			    System.out.println(">>>>");
			    System.out.println(data);
			    System.out.println("<<<<");

		    }
		    else
		    {
		    	System.out.println("Opening it through set");
		    	jep.set("da", pdFrame);
			    String data = (String)jep.getValue("\nda.head(3)");
			    System.out.println("This succeeded.. ");
			    System.out.println(">>>>");
			    System.out.println(data);
			    System.out.println("<<<<");
		    }
		    jep.eval("a");
		    
		    
		    
//		    
//		    jep.eval("\nmyvar = sqldf('select Nominated from da', locals())");
//		    System.out.println(jep.getValue("\nmyvar.head(3)"));
		    
		    
		    long end = System.nanoTime();
		    long micros = (end - start)/1000;
			System.out.println("Time Micros  " + micros);
			long millis = micros / 1000;
			System.out.println("Time Millis " + millis);
			long seconds = millis / 1000;
			System.out.println("Time Seconds " + seconds);
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
}
