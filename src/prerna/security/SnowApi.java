/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.security;

public class SnowApi {

  Snow snow = new Snow();

  public String encryptMessage(
      String message, String password, String inputFile, String outputFile) {
    // String [] args = {"-C", "-m", message, "-p", password, inputFile};//, outputFile};
    String[] args = {"-C", "-m", message, "-p", password, inputFile, outputFile};
    return snow.runSnow(args);
  }

  public String decryptMessage(String password, String outputFile) {
    // -C  -p "hola" prop2.txt output.txt
    String[] args = {"-C", "-p", password, outputFile};
    return snow.runSnow(args);
  }

  public void encryptFile(
      String fileToEncrypt, String password, String inputFile, String outputFile) {
    // -C -f prop.txt -p "hola" input.txt prop2.txt
    String[] args = {"-C", "-f", fileToEncrypt, "-p", password, inputFile, outputFile};
    snow.runSnow(args);
  }

  public void decryptFile(String fileToDecrypt, String password, String outputFile) {
    // -C  -p "hola" prop2.txt output.txt
    String[] args = {"-C", "-p", password, fileToDecrypt, outputFile};
    snow.runSnow(args);
  }

  //	public static void main(String[] args)
  //	{
  //		SnowApi snow = new SnowApi();
  //
  //		String encrypted = snow.encryptMessage("da monkey is here", "hello",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt", null);
  //		System.out.println("Encrpted data is" + encrypted);
  //		//snow.encryptMessage("da monkey is here", "hello",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
  //		//snow.decryptMessage("password", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
  //
  //
  //		// encrypt the file
  //		//snow.encryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\message.txt", "password",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\input.txt",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt");
  //
  //		SnowApi snow2 = new SnowApi();
  //		//snow.decryptFile("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt", "hello",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\passout.txt");
  //		System.out.println(snow.decryptMessage("hello",
  // "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\output.txt"));
  //
  //	}
}
