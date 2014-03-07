/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.io.File;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;
import com.teamdev.jxbrowser.dom.DOMDocument;

//import org.apache.fop.apps.FOUserAgent;
//import org.apache.fop.apps.Fop;
//import org.apache.fop.apps.FopFactory;
//import org.apache.fop.apps.MimeConstants;

import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.specific.tap.CapabilityFactSheetListener;
import prerna.ui.main.listener.specific.tap.SysDupeHealthGridListener;

/**
 * This class creates the capability fact sheet.
 */
public class CapabilityFactSheet extends BrowserPlaySheet{

	Hashtable allHash = new Hashtable();
	Hashtable capabilityHash = new Hashtable();
	//keys are processed capabilities and values are semoss stored capabilities
	public Hashtable capabilityProcessed = new Hashtable();
	

	
	CapabilityFactSheetListener singleCapFactSheetCall = new CapabilityFactSheetListener();
	
	/**
	 * Constructor for CapabilityFactSheet. Generates the landing page for the capability fact sheets.
	 */
	public CapabilityFactSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
	}

	/**
	 * Processes all Sys Dupe queries and shows results in sysdupe.html format.
	 */
	@Override
	public void createView()
	{
		String workingDir = System.getProperty("user.dir");
		
		singleCapFactSheetCall.setCapabilityFactSheet(this);
		//browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/Capability Fact Sheet.html");
		//singleCapFactSheetCall.invoke(null);
		
		browser.addNavigationListener(new NavigationListener() {
    	    public void navigationStarted(NavigationEvent event) {
    	    	logger.info("event.getUrl() = " + event.getUrl());
    	    }

    	    public void navigationFinished(NavigationFinishedEvent event) {
   	    	browser.registerFunction("singleCapFactSheet",  singleCapFactSheetCall);
    			callIt();
    	    }
    	});
	       
		browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/index.html");
	//		createPDF();
	}
	
//
//	
//    public void convertDOM2PDF(Document xslfoDoc, File pdf) {
//        // configure fopFactory as desired
//    	FopFactory fopFactory = FopFactory.newInstance();
//    	
//        try {
//            FOUserAgent foUserAgent = fopFactory.newFOUserAgent();
//            // configure foUserAgent as desired
//
//            // Setup output
//            OutputStream out = new java.io.FileOutputStream(pdf);
//            out = new java.io.BufferedOutputStream(out);
//
//            try {
//                // Construct fop with desired output format and output stream
//                Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF, foUserAgent, out);
//
//                // Setup Identity Transformer
//                TransformerFactory factory = TransformerFactory.newInstance();
//                //Transformer transformer = factory.newTransformer(); // identity transformer
//                Transformer transformer = factory.newTransformer(); // identity transformer
//
//                // Setup input for XSLT transformation
//                Source src = new DOMSource(xslfoDoc);
//
//                // Resulting SAX events (the generated FO) must be piped through to FOP
//                Result res = new SAXResult(fop.getDefaultHandler());
//
//                // Start XSLT transformation and FOP processing
//                transformer.transform(src, res);
//            } finally {
//                out.close();
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace(System.err);
//            System.exit(-1);
//        }
//
//    }
//
//	
//    
//    
//	public void createPDF()
//	{
//        DOMDocument domDocument = (DOMDocument)browser.getDocument();
//        Document document = browser.getDocument();
//        NodeList children = document.getChildNodes();
//        DOMImplementation domImple = domDocument.getImplementation();
//        
//        String workingDir = System.getProperty("user.dir");
//        String filePath =  workingDir + "/export";
//        File baseDir = new File(filePath);
//        File outDir = new File(baseDir, "out");
//
//        outDir.mkdirs();
//
//        //Setup output file
//        File pdffile = new File(outDir, "ResultDOM2PDF.pdf");
//        System.out.println("PDF Output File: " + pdffile);
//        System.out.println();
//
//        Document foDoc;
//        Document foDoc2;
//		try {
//			foDoc = buildDOMDocument();
//			//foDoc2 = (Document)(browser.getDocument().getImplementation());
//
//			for(int i=0;i<children.getLength();i++)
//			{
//				foDoc.adoptNode(children.item(i));
//			}
//	        convertDOM2PDF(foDoc, pdffile);
//
//	        System.out.println("Success!"); 
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//   
//        
//	}
//	
//    /** xsl-fo namespace URI */
//    protected static String foNS = "http://www.w3.org/1999/XSL/Format";
//
//	
//    /**
//     * Builds the example FO document as a DOM in memory.
//     * @return the FO document
//     * @throws ParserConfigurationException In case there is a problem creating a DOM document
//     */
//    private static Document buildDOMDocument() throws ParserConfigurationException {
//        // Create a sample XSL-FO DOM document
//    	
//        Document foDoc = null;
//        Element root = null, ele1 = null, ele2 = null, ele3 = null;
//
//        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
//        dbf.setNamespaceAware(true);
//        DocumentBuilder db = dbf.newDocumentBuilder();
//        foDoc = db.newDocument();
//
//        root = foDoc.createElementNS(foNS, "fo:root");
//        foDoc.appendChild(root);
//
//        ele1 = foDoc.createElementNS(foNS, "fo:layout-master-set");
//        root.appendChild(ele1);
//        ele2 = foDoc.createElementNS(foNS, "fo:simple-page-master");
//        ele1.appendChild(ele2);
//        ele2.setAttributeNS(null, "master-name", "letter");
//        ele2.setAttributeNS(null, "page-height", "11in");
//        ele2.setAttributeNS(null, "page-width", "8.5in");
//        ele2.setAttributeNS(null, "margin-top", "1in");
//        ele2.setAttributeNS(null, "margin-bottom", "1in");
//        ele2.setAttributeNS(null, "margin-left", "1in");
//        ele2.setAttributeNS(null, "margin-right", "1in");
//        ele3 = foDoc.createElementNS(foNS, "fo:region-body");
//        ele2.appendChild(ele3);
//        ele1 = foDoc.createElementNS(foNS, "fo:page-sequence");
//        root.appendChild(ele1);
//        ele1.setAttributeNS(null, "master-reference", "letter");
//        ele2 = foDoc.createElementNS(foNS, "fo:flow");
//        ele1.appendChild(ele2);
//        ele2.setAttributeNS(null, "flow-name", "xsl-region-body");
//        addElement(ele2, "fo:block", "Hello World!");
//        return foDoc;
//    }
//
//    /**
//     * Adds an element to the DOM.
//     * @param parent parent node to attach the new element to
//     * @param newNodeName name of the new node
//     * @param textVal content of the element
//     */
//    protected static void addElement(Node parent, String newNodeName,
//                                String textVal) {
//        if (textVal == null) {
//            return;
//        }  // use only with text nodes
//        Element newElement = parent.getOwnerDocument().createElementNS(
//                                        foNS, newNodeName);
//        Text elementText = parent.getOwnerDocument().createTextNode(textVal);
//        newElement.appendChild(elementText);
//        parent.appendChild(newElement);
//    }
//
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable Includes the data series.*/
	public Hashtable processQueryData()
	{
		addPanel();
		ArrayList dataArrayList = new ArrayList();
		String[] var = wrapper.getVariables(); 		
		for (int i=0; i<list.size(); i++)
		{	
			Object[] listElement = list.get(i);
			for (int j = 0; j < var.length; j++) 
			{	
					String text = (String) listElement[j];
					String processedText = text.replaceAll("\\[", "(").replaceAll("\\]", ")").replaceAll(",", "").replaceAll("&", "").replaceAll("\'","").replaceAll("’", "");
					capabilityProcessed.put(processedText,text);
					dataArrayList.add(processedText);
			}			
		}

		capabilityHash.put("dataSeries", dataArrayList);
		
		return capabilityHash;
	}
	
	public Hashtable processNewCapability(String capability)
	{		
		CapabilityFactSheetPerformer performer = new CapabilityFactSheetPerformer();
		Hashtable<String,Object> dataSeries = new Hashtable<String,Object>();
		
//		updateProgressBar("50%...Processing Systems", 50);
//		Hashtable<String, Object> systemSheet = performer.processSystemQueries(capability);
//		dataSeries.put("SystemSheet", systemSheet);
//
		updateProgressBar("10%...Processing Capability Dupe", 10);
		Hashtable<String, Object> capabilityDupeSheetHash = performer.processCapabilityDupeSheet(capability);
		dataSeries.put("CapabilityDupeSheet", capabilityDupeSheetHash);
		
		updateProgressBar("30%...Processing Tasks and BPs", 30);
		Hashtable<String, Object> taskAndBPSheetHash = performer.processTaskandBPQueries(capability);
		dataSeries.put("TaskAndBPSheet", taskAndBPSheetHash);
		
		updateProgressBar("40%...Processing Requirements and Standards", 40);
		Hashtable<String, Object> reqAndStandardSheet = performer.processRequirementsAndStandardsQueries(capability);
		dataSeries.put("ReqAndStandardSheet", reqAndStandardSheet);

		updateProgressBar("65%...Processing Data Objects", 65);
		Hashtable<String, Object> dataSheet = performer.processDataSheetQueries(capability);
		dataSeries.put("DataSheet", dataSheet);
		
		updateProgressBar("70%...Processing BLUs", 70);
		Hashtable<String, Object> bluSheet = performer.processBLUSheetQueries(capability);
		dataSeries.put("BLUSheet", bluSheet);
		
		updateProgressBar("75%...Processing FunctionalGaps", 75);
		Hashtable<String, Object> funtionalGapSheet = performer.processFunctionalGapSheetQueries(capability);
		dataSeries.put("FunctionalGapSheet", funtionalGapSheet);
		
		updateProgressBar("80%...Processing Capability Overview", 80);
		Hashtable<String, Object> firstSheetHash = performer.processFirstSheetQueries(capability);
		dataSeries.put("CapabilityOverviewSheet", firstSheetHash);

		allHash.put("dataSeries", dataSeries);
		allHash.put("capability", capability);

	//	callItAllHash();
		updateProgressBar("100%...Capability Fact Sheet Generation Complete", 100);
		return allHash;
	}
	
	public void callIt()
	{
		Gson gson = new Gson();
//		browser.executeScript("capabilityList('" + gson.toJson(capabilityHash) + "');");
		browser.executeScript("start('" + gson.toJson(capabilityHash) + "');");
		System.out.println(gson.toJson(capabilityHash));
	}
	
	public void callItAllHash()
	{
		Gson gson = new Gson();
//		browser.executeScript("capabilityData('" + gson.toJson(allHash) + "');");
		String workingDir = System.getProperty("user.dir");
		browser.navigate("file://" + workingDir + "/html/MHS-FactSheets/index.html#/cap");
		browser.executeScript("start('" + gson.toJson(allHash) + "');");
	}
	
}


