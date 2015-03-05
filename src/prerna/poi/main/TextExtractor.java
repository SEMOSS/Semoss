/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
//http://stackoverflow.com/questions/6112419/handling-the-tab-character-in-java
//http://stackoverflow.com/questions/10250617/java-apache-poi-can-i-get-clean-text-from-ms-word-doc-files
package prerna.poi.main;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public final class TextExtractor { 

	private TextExtractor() {
		
	}
	
	public static String websiteTextExtractor(String docin) throws IOException {
		final String url = docin;
		boolean knownwebsite = false;
		org.jsoup.nodes.Document doc = Jsoup.connect(url).get();
		String extractedtext = "";

		if(url.contains("nytimes.com")){
			knownwebsite = true;
			for( Element element : doc.select("p.story-body-text") )
			{
				if( element.hasText() ) // Skip those tags without text
				{
					extractedtext = extractedtext.concat(element.text().toString());
				}
			}
		}
		if(!knownwebsite){
			extractedtext = doc.text();
		}
		extractedtext = extractedtext.replace("\n", " @ ").replace("\r", " ");
		return extractedtext;
	}

	public static String fileTextExtractor(String filename) throws IOException, SAXException, TikaException {
		Metadata metadata = new Metadata();
		ParseContext context = new ParseContext();
		Detector detector = new DefaultDetector();
		Parser parser = new AutoDetectParser(detector);
		context.set(Parser.class, parser);

		URL url;
		File file = new File(filename);
		if(file.exists() && file.isFile()) {
			url = file.toURI().toURL();
		} else {
			url = new URL(filename);
		}
		
		InputStream input = TikaInputStream.get(url, metadata);
		OutputStream outputstream = new ByteArrayOutputStream();
		
		ContentHandler handler = new BodyContentHandler(outputstream);
		parser.parse(input, handler, metadata, context); 
		input.close();
		
		String extractedText = outputstream.toString();
		outputstream.close();
		
		extractedText = extractedText.replace("\n+|\r+", " ");
		return extractedText;
	}
	
	/**
	 * Reads in a file and stores it as a string
	 * @param path				The path of the file
	 * @param encoding			Definition of standard charset
	 * @return					The file as a string
	 * @throws IOException
	 */
	public static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
}