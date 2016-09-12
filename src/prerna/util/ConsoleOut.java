package prerna.util;

public class ConsoleOut {

	public String output = "";
	public void println(Object object)
	{
		output = output + object + "\n";
		System.out.println("Console >>" + object);
	}
	
}
