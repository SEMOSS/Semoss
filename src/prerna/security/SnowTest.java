package prerna.security;

public class SnowTest {

	Snow snow = new Snow();
	
	public void encryptMessage(String message, String password, String inputFile, String outputFile)
	{
		String [] args = {"-C", "-m", message, "-p", password, inputFile, outputFile};
		snow.runSnow(args);
		
	}
	
	public void decryptMessage(String password, String outputFile)
	{
		// -C  -p "hola" prop2.txt output.txt
		String [] args = {"-C", "-p",password, outputFile};
		snow.runSnow(args);
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
		String [] args = {"-C", "-p","password", fileToDecrypt, outputFile};
		snow.runSnow(args);
	}

//	public static void main(String[] args)
//	{
//		SnowTest snow = new SnowTest();
//		// encrypt the file
//		//snow.encryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\message.txt", "password", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
//
//		snow.decryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt", "password", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\decrypt.txt");
//		
//	}	
}
