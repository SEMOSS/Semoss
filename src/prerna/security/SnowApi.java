package prerna.security;

public class SnowApi {

	Snow snow = new Snow();
	
	public String encryptMessage(String message, String password, String inputFile, String outputFile)
	{
		//String [] args = {"-C", "-m", message, "-p", password, inputFile};//, outputFile};
		String [] args = {"-C", "-m", message, "-p", password, inputFile, outputFile};
		return snow.runSnow(args);
		
	}
	
	public String decryptMessage(String password, String outputFile)
	{
		// -C  -p "hola" prop2.txt output.txt
		String [] args = {"-C", "-p",password, outputFile};
		return snow.runSnow(args);
	}

	public void encryptFile(String fileToEncrypt, String password, String inputFile, String outputFile)
	{
		//-C -f prop.txt -p "hola" input.txt prop2.txt
		String [] args = {"-C", "-f", fileToEncrypt, "-p", password, inputFile, outputFile};
		snow.runSnow(args);
		
	}
	
	public void decryptFile(String fileToDecrypt, String password, String outputFile)
	{
		// -C  -p "hola" prop2.txt output.txt
		String [] args = {"-C", "-p",password, fileToDecrypt, outputFile};
		snow.runSnow(args);
	}

//	public static void main(String[] args)
//	{
//		SnowApi snow = new SnowApi();
//		
//		String encrypted = snow.encryptMessage("da monkey is here", "hello", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt", null);
//		System.out.println("Encrpted data is" + encrypted);
//		//snow.encryptMessage("da monkey is here", "hello", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
//		//snow.decryptMessage("password", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
//		
//		
//		// encrypt the file
//		//snow.encryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\message.txt", "password", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
//
//		SnowApi snow2 = new SnowApi();
//		//snow.decryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt", "hello", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\passout.txt");
//		System.out.println(snow.decryptMessage("hello", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt"));
//		
//	}	
}
