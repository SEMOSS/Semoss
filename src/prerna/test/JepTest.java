package prerna.test;

import jep.Jep;

public class JepTest {

	public static void main(String [] args)
	{
		try(Jep jep = new Jep(false)) {
			long start = System.nanoTime();
			System.out.println("Time Start" + System.nanoTime());
		    jep.eval("from java.lang import System");
		    jep.eval("from pandasql import *");
		    jep.eval("import pandas as pd");
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
		    jep.runScript("C:/Users/pkapaleeswaran/workspacej3/SemossDev/sample.py");
		    //jep.eval("\npath = r'C:/Users/pkapaleeswaran/workspacej3/datasets/physican data.csv'");
		    jep.eval("\npath = r'C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv'");
		    
		    jep.eval("\nda = pd.read_csv(path)");
		    String data = (String)jep.getValue("\nda.head(3)");
		    
		    
		    System.out.println(">>>>");
		    System.out.println(data);
		    System.out.println("<<<<");
		    
		    
		    jep.eval("\nmyvar = sqldf('select Nominated from da', locals())");
		    System.out.println(jep.getValue("\nmyvar.head(3)"));
		    
		    
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
