package prerna.util;

public class ConsoleOut {

	public Object output = "";
	public void println(Object object)
	{
		if(output.toString().isEmpty()) {
			output = object;
		} else {
			output = output + "" +  object + "\n";
		}
		System.out.println("Console >>" + object);
	}
	
	public void println(int object)
	{
		if(output.toString().isEmpty()) {
			output = object;
		} else {
			output = output + "" +  object + "\n";
		}
		System.out.println("Console >>" + object);
	}
	
	public void println(long object)
	{
		if(output.toString().isEmpty()) {
			output = object;
		} else {
			output = output + "" +  object + "\n";
		}
		System.out.println("Console >>" + object);
	}
	
	public void println(double object)
	{
		if(output.toString().isEmpty()) {
			output = object;
		} else {
			output = output + "" +  object + "\n";
		}
		System.out.println("Console >>" + object);
	}
	
	public void println(float object)
	{
		if(output.toString().isEmpty()) {
			output = object;
		} else {
			output = output + "" +  object + "\n";
		}
		System.out.println("Console >>" + object);
	}
	
}
