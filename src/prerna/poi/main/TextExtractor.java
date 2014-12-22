/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

class TextExtractor { 

	private OutputStream outputstream;
	private ParseContext context;
	private Detector detector;
	private Parser parser;
	private Metadata metadata;
	private String extractedText;

	public TextExtractor() {
		context = new ParseContext();
		detector = new DefaultDetector();
		parser = new AutoDetectParser(detector);
		context.set(Parser.class, parser);
		outputstream = new ByteArrayOutputStream();
		metadata = new Metadata();
	}

	public void process(String filename) throws IOException, SAXException, TikaException {
		URL url;
		File file = new File(filename);
		if (file.exists() && file.isFile()) {
			url = file.toURI().toURL();
		} else {
			url = new URL(filename);
		}
		InputStream input = TikaInputStream.get(url, metadata);
		ContentHandler handler = new BodyContentHandler(outputstream);
		parser.parse(input, handler, metadata, context); 
		input.close();
	}

	public String getString() throws IOException {
		//Get the text into a String object
		extractedText = outputstream.toString();
		extractedText = extractedText.replace("\n", " ").replace("\r", " "); //MUST COME BACK
		outputstream.close();
		return extractedText;
	}

	public String WebsiteTextExtractor(String docin) throws IOException {

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

	public String WorddocTextExtractor(String docin) throws IOException, SAXException, TikaException {
		TextExtractor textExtractor = new TextExtractor();
		textExtractor.process(docin);
		String extractedText = textExtractor.getString();
		return extractedText;
	}

	public String TextDocExtractor(String docin) throws IOException, SAXException, TikaException {
		TextExtractor textExtractor = new TextExtractor();
		textExtractor.process(docin);
		String extractedText = textExtractor.getString();
		return extractedText;
	}

	public String MasterResumeExtractor(String docin) throws IOException, SAXException, TikaException {
		TextExtractor textExtractor = new TextExtractor();
		textExtractor.process(docin);
		String extractedText = textExtractor.getString();
		return extractedText;

	}

}