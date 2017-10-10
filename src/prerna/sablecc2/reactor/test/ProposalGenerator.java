package prerna.sablecc2.reactor.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class ProposalGenerator {

	public static void main(String[] args) {
		
		int atomicsToCreate = 1000;
		int pkslsToCreate = 30000;
		
		PixelGenerator generator = new PixelGenerator();
		Map<String, String> aliases = generateAliases(atomicsToCreate);
		generator.setConstants(aliases.keySet().toArray(new String[0]));
		Map<String, String> pksls = generator.getRandomPixels(pkslsToCreate);
		
		String proposalName = "Custom";
		String headerLine = "Alias,Hashcode,Value,Type,ProposalName";
		BufferedWriter writer = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter("C:\\Workspace\\Semoss_Dev\\src\\prerna\\sablecc2\\reactor\\test\\ProposalTest.csv");
			writer = new BufferedWriter(fw);
			writer.write(headerLine+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String type = "Atomic";
		for(String alias : aliases.keySet()) {
			String value = aliases.get(alias);
			String nextLine = alias+","+alias+","+value+","+type+","+proposalName;
			try {
				writer.write(nextLine+"\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//write to file
			//alias is header and alias, value is value, all are atomic
		}
		
		type = "Formula";
		for(String pkslAlias : pksls.keySet()) {
			String pkslValue = pksls.get(pkslAlias);
			if(pkslValue.contains(",")) {
				pkslValue = "\""+pkslValue+"\"";
			}
			String nextLine = pkslAlias+","+pkslAlias+","+pkslValue+","+type+","+proposalName;
			try {
				writer.write(nextLine+"\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			writer.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done");
	}
	
	public static Map<String, String> generateAliases(int n) {
		Random random = new Random();
		DecimalFormat df2 = new DecimalFormat("#.##");
		Map<String, String> aliases = new HashMap<>();
		for(int i = 0; i < n; i++) {
			
			aliases.put("b"+i, df2.format(random.nextDouble()+1.1));
		}
		
		return aliases;
	}
}
