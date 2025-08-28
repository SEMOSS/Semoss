/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.test;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProposalGenerator {

  private static final Logger logger = LogManager.getLogger(ProposalGenerator.class);

  private static final String STACKTRACE = "StackTrace: ";

  /*
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
  		logger.error(STACKTRACE, e);
  	}

  	if (writer == null) {
  		throw new NullPointerException("Buffered writer cannot be null here.");
  	}

  	String type = "Atomic";
  	for(String alias : aliases.keySet()) {
  		String value = aliases.get(alias);
  		String nextLine = alias+","+alias+","+value+","+type+","+proposalName;
  		try {
  			writer.write(nextLine+"\n");
  		} catch (IOException e) {
  			logger.error(STACKTRACE, e);
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
  			logger.error(STACKTRACE, e);
  		}
  	}
  	try {
  		writer.close();
  		if (fw != null) {
  			fw.close();
  		}
  	} catch (IOException e) {
  		logger.error(STACKTRACE, e);
  	}
  	logger.info("Done");
  }
  */

  public static Map<String, String> generateAliases(int n) {
    Random random = new Random();
    DecimalFormat df2 = new DecimalFormat("#.##");
    Map<String, String> aliases = new HashMap<>();
    for (int i = 0; i < n; i++) {

      aliases.put("b" + i, df2.format(random.nextDouble() + 1.1));
    }

    return aliases;
  }
}
