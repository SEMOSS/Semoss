package prerna.util;

import java.util.Comparator;

public class StringNumericComparator implements Comparator<String>{

	@Override
	public int compare(String str1, String str2) {

		// extract numeric portion out of the string and convert them to int
		// and compare them, roughly something like this
		int num1=0;
		int num2=0;
		if(str1.indexOf(".")>0 & str2.indexOf(".")>0){
			try{
				num1 = Integer.parseInt(str1.substring(0, str1.indexOf(".")));
				num2 = Integer.parseInt(str2.substring(0, str2.indexOf(".")));
				if(num1 != num2) return num1 - num2;
			}catch(Exception e){

			}
		}
		
		return str1.compareToIgnoreCase(str2);


	}
}