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
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathEvaluationResult;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

/**
 * Static methods that the bytecode of a project's custom reactors calls in
 * place of, or right before, the JDK's XML calls. They keep XML parsing from
 * reading a file outside the user's folder or fetching a network address:
 *
 * <ul>
 * <li>A project gets the JDK's own factories, with external DTDs, entities,
 * schemas and stylesheets, XInclude and XSLT extension functions off.</li>
 * <li>Those settings are applied again right before every use of a factory,
 * parser, reader or validator, so a setting the project changed in between has
 * no effect.</li>
 * <li>A system id that names a document to read or write follows the path rule
 * of {@link SandboxedJavaExecution}, and one that names a network address is
 * refused.</li>
 * </ul>
 */
public final class SandboxedXml {

	private static final Logger classLogger = LogManager.getLogger(SandboxedXml.class);

	private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
	private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
	private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";
	private static final List<String> EXTENSION_FUNCTIONS = List.of("jdk.xml.enableExtensionFunctions",
			"http://www.oracle.com/xml/jaxp/properties/enableExtensionFunctions");
	private static final String NO_ACCESS = "";

	private SandboxedXml() {
	}

	// --------------------------------------------------------------- factories

	/**
	 * Stands in for the {@code DocumentBuilderFactory} lookups.
	 *
	 * @return the JDK's factory, hardened
	 */
	public static DocumentBuilderFactory documentBuilderFactory() {
		return hardened(DocumentBuilderFactory.newDefaultInstance());
	}

	/**
	 * Stands in for the namespace aware {@code DocumentBuilderFactory} lookups.
	 *
	 * @return the JDK's namespace aware factory, hardened
	 */
	public static DocumentBuilderFactory documentBuilderFactoryNS() {
		return hardened(DocumentBuilderFactory.newDefaultNSInstance());
	}

	/**
	 * Stands in for the {@code SAXParserFactory} lookups.
	 *
	 * @return the JDK's factory, hardened
	 */
	public static SAXParserFactory saxParserFactory() {
		return hardened(SAXParserFactory.newDefaultInstance());
	}

	/**
	 * Stands in for the namespace aware {@code SAXParserFactory} lookups.
	 *
	 * @return the JDK's namespace aware factory, hardened
	 */
	public static SAXParserFactory saxParserFactoryNS() {
		return hardened(SAXParserFactory.newDefaultNSInstance());
	}

	/**
	 * Stands in for the {@code TransformerFactory} lookups.
	 *
	 * @return the JDK's factory, hardened
	 */
	public static TransformerFactory transformerFactory() {
		return hardened(TransformerFactory.newDefaultInstance());
	}

	/**
	 * Stands in for {@code SchemaFactory.newDefaultInstance()}.
	 *
	 * @return the JDK's XML Schema factory, hardened
	 */
	public static SchemaFactory schemaFactory() {
		return hardened(SchemaFactory.newDefaultInstance());
	}

	/**
	 * Stands in for the {@code SchemaFactory.newInstance} lookups.
	 *
	 * @param schemaLanguage the schema language asked for
	 * @return the JDK's XML Schema factory, hardened
	 * @throws IllegalArgumentException if the language is not W3C XML Schema
	 */
	public static SchemaFactory schemaFactory(String schemaLanguage) {
		if (!XMLConstants.W3C_XML_SCHEMA_NS_URI.equals(schemaLanguage)) {
			throw new IllegalArgumentException("No schema factory for the schema language " + schemaLanguage);
		}
		return schemaFactory();
	}

	/**
	 * Stands in for the {@code XMLInputFactory} lookups.
	 *
	 * @return the JDK's factory, hardened
	 */
	public static XMLInputFactory xmlInputFactory() {
		return hardened(XMLInputFactory.newDefaultFactory());
	}

	/**
	 * Stands in for the {@code XPathFactory} lookups.
	 *
	 * @return the JDK's factory, hardened
	 */
	public static XPathFactory xpathFactory() {
		return hardened(XPathFactory.newDefaultInstance());
	}

	/**
	 * Stands in for {@code XMLReaderFactory.createXMLReader}.
	 *
	 * @return a reader from the JDK's parser that stays hardened
	 */
	public static XMLReader xmlReader() {
		try {
			return xmlReader(saxParserFactoryNS().newSAXParser());
		} catch (SecurityException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Could not create an XML reader", e);
		}
	}

	/**
	 * Stands in for {@code SAXParser.getXMLReader()}: the parser's reader, behind a
	 * reader that applies the settings again after every setting a project makes
	 * and before every parse. A filter or a transformer that is handed the reader
	 * parses through it, so the settings hold there as well.
	 *
	 * @param parser the parser the project holds
	 * @return the parser's reader, or a reader that keeps it hardened
	 * @throws SAXException if the parser cannot give its reader
	 */
	public static XMLReader xmlReader(SAXParser parser) throws SAXException {
		XMLReader reader = hardened(parser).getXMLReader();
		return isProjectsOwn(parser) || !isJdk(reader) ? reader : new HardenedReader(reader);
	}

	// ---------------------------------------------------------- hardened uses

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static DocumentBuilderFactory hardened(DocumentBuilderFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
			factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
			factory.setFeature(LOAD_EXTERNAL_DTD, false);
			factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, NO_ACCESS);
			factory.setXIncludeAware(false);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		return factory;
	}

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static SAXParserFactory hardened(SAXParserFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
			factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
			factory.setFeature(LOAD_EXTERNAL_DTD, false);
			factory.setXIncludeAware(false);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		return factory;
	}

	/**
	 * Applies the settings to a parser right before a project uses it.
	 *
	 * @param parser the parser the project holds
	 * @return the same parser
	 */
	public static SAXParser hardened(SAXParser parser) {
		if (isProjectsOwn(parser)) {
			return parser;
		}
		requireJdk(parser);
		try {
			parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, NO_ACCESS);
			hardened(parser.getXMLReader());
		} catch (SecurityException e) {
			throw e;
		} catch (Exception e) {
			throw refused(parser, e);
		}
		return parser;
	}

	/**
	 * Applies the settings to a reader right before a project uses it. A reader the
	 * project wrote itself runs inside the sandbox already and is left as it is.
	 *
	 * @param reader the reader the project holds
	 * @return the same reader
	 */
	public static XMLReader hardened(XMLReader reader) {
		if (reader instanceof XMLFilter filter) {
			hardened(filter.getParent());
		}
		if (reader == null || isProjectsOwn(reader) || !isJdk(reader)) {
			return reader;
		}
		try {
			reader.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
			reader.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
			reader.setFeature(LOAD_EXTERNAL_DTD, false);
			reader.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			reader.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, NO_ACCESS);
		} catch (Exception e) {
			if (reader.getClass().getName().startsWith("org.xml.sax.helpers.")) {
				return reader;
			}
			throw refused(reader, e);
		}
		return reader;
	}

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static TransformerFactory hardened(TransformerFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, NO_ACCESS);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		for (String extensionFunctions : EXTENSION_FUNCTIONS) {
			try {
				factory.setFeature(extensionFunctions, false);
			} catch (TransformerConfigurationException e) {
				classLogger.debug("The transformer factory does not know the feature {}", extensionFunctions);
			}
		}
		return factory;
	}

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static SchemaFactory hardened(SchemaFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
			factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, NO_ACCESS);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		return factory;
	}

	/**
	 * Applies the settings to a validator right before a project uses it.
	 *
	 * @param validator the validator the project holds
	 * @return the same validator
	 */
	public static Validator hardened(Validator validator) {
		if (isProjectsOwn(validator)) {
			return validator;
		}
		requireJdk(validator);
		try {
			validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
			validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, NO_ACCESS);
		} catch (Exception e) {
			throw refused(validator, e);
		}
		return validator;
	}

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static XMLInputFactory hardened(XMLInputFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
			factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, NO_ACCESS);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		return factory;
	}

	/**
	 * Applies the settings to a factory right before a project uses it.
	 *
	 * @param factory the factory the project holds
	 * @return the same factory
	 */
	public static XPathFactory hardened(XPathFactory factory) {
		if (isProjectsOwn(factory)) {
			return factory;
		}
		requireJdk(factory);
		try {
			factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
		} catch (Exception e) {
			throw refused(factory, e);
		}
		return factory;
	}

	// ------------------------------------------------------------- system ids

	/**
	 * A system id a project passed as the document to read or write. A path or a
	 * {@code file:} address follows the path rule; any other address is refused.
	 *
	 * @param systemId the system id the project passed
	 * @return the system id to use
	 * @throws SecurityException if the system id names anything but a local file
	 */
	public static String systemId(String systemId) {
		if (systemId == null || systemId.isBlank()) {
			return systemId;
		}
		int colon = systemId.indexOf(':');
		boolean hasScheme = colon > 1 && systemId.substring(0, colon).chars()
				.allMatch(c -> Character.isLetterOrDigit(c) || c == '+' || c == '-' || c == '.');
		String path = systemId;
		if (hasScheme) {
			if (!systemId.regionMatches(true, 0, "file:", 0, 5)) {
				throw new SecurityException("An XML document is read from a local file or a stream in project "
						+ "reactors; fetch '" + systemId.substring(0, colon) + "' addresses with HttpHelperUtility");
			}
			try {
				path = new File(new URI(systemId)).getPath();
			} catch (URISyntaxException | IllegalArgumentException e) {
				throw new SecurityException("Not a local file: " + systemId, e);
			}
		}
		String sandboxed = SandboxedJavaExecution.sandboxedPath(path);
		return sandboxed.equals(path) ? systemId : new File(sandboxed).toURI().toString();
	}

	/**
	 * A schema location a project passed as a URL, under the same rule as
	 * {@link #systemId(String)}.
	 *
	 * @param location the location the project passed
	 * @return the location to use
	 * @throws SecurityException if the location names anything but a local file
	 */
	public static URL systemUrl(URL location) {
		if (location == null) {
			return null;
		}
		String original = location.toString();
		String sandboxed = systemId(original);
		try {
			return sandboxed.equals(original) ? location : URI.create(sandboxed).toURL();
		} catch (Exception e) {
			throw new SecurityException("Not a local file: " + original, e);
		}
	}

	/**
	 * An input source a project is about to hand to a parser, or return from a
	 * resolver: its system id follows {@link #systemId(String)}.
	 *
	 * @param source the input source the project holds
	 * @return the same input source
	 * @throws SecurityException if its class answers for the system id itself
	 */
	public static InputSource inputSource(InputSource source) {
		if (source == null) {
			return null;
		}
		requireOwnGetter(source, InputSource.class, "getSystemId");
		String systemId = source.getSystemId();
		String sandboxed = systemId(systemId);
		if (sandboxed != null && !sandboxed.equals(systemId)) {
			source.setSystemId(sandboxed);
		}
		return source;
	}

	/**
	 * A source a project is about to hand to a parser, transformer or validator, or
	 * return from a resolver: its system id follows {@link #systemId(String)}.
	 *
	 * @param source the source the project holds
	 * @return the same source
	 * @throws SecurityException if its class answers for the system id itself
	 */
	public static Source source(Source source) {
		if (source instanceof StreamSource stream) {
			requireOwnGetter(stream, StreamSource.class, "getSystemId");
			setSystemId(stream, stream.getSystemId());
		} else if (source instanceof SAXSource sax) {
			requireOwnGetter(sax, SAXSource.class, "getSystemId");
			requireOwnGetter(sax, SAXSource.class, "getInputSource");
			requireOwnGetter(sax, SAXSource.class, "getXMLReader");
			inputSource(sax.getInputSource());
			hardened(sax.getXMLReader());
		} else if (source instanceof DOMSource dom) {
			requireOwnGetter(dom, DOMSource.class, "getSystemId");
			setSystemId(dom, dom.getSystemId());
		}
		return source;
	}

	/**
	 * The sources a project is about to hand to a schema factory, each under
	 * {@link #source(Source)}.
	 *
	 * @param sources the sources the project holds
	 * @return the same array
	 */
	public static Source[] sources(Source[] sources) {
		if (sources != null) {
			for (Source source : sources) {
				source(source);
			}
		}
		return sources;
	}

	/**
	 * A result a project is about to hand to a transformer or validator: its system
	 * id follows {@link #systemId(String)}.
	 *
	 * @param result the result the project holds
	 * @return the same result
	 * @throws SecurityException if its class answers for the system id itself
	 */
	public static Result result(Result result) {
		if (result instanceof StreamResult || result instanceof DOMResult || result instanceof SAXResult) {
			Class<?> declared = result instanceof StreamResult ? StreamResult.class
					: result instanceof DOMResult ? DOMResult.class : SAXResult.class;
			requireOwnGetter(result, declared, "getSystemId");
			String systemId = result.getSystemId();
			String sandboxed = systemId(systemId);
			if (sandboxed != null && !sandboxed.equals(systemId)) {
				result.setSystemId(sandboxed);
			}
		}
		return result;
	}

	// ------------------------------------------------------------------ XPath

	/**
	 * Stands in for {@code XPath.evaluate(String, InputSource, QName)}: parses the
	 * source with a hardened parser, then evaluates.
	 *
	 * @param xpath      the XPath object
	 * @param expression the expression
	 * @param source     the document to parse
	 * @param returnType the result type
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static Object evaluate(XPath xpath, String expression, InputSource source, QName returnType)
			throws XPathExpressionException {
		return xpath.evaluate(expression, parse(source), returnType);
	}

	/**
	 * Stands in for {@code XPath.evaluate(String, InputSource)}.
	 *
	 * @param xpath      the XPath object
	 * @param expression the expression
	 * @param source     the document to parse
	 * @return the result as text
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static String evaluate(XPath xpath, String expression, InputSource source) throws XPathExpressionException {
		return xpath.evaluate(expression, parse(source));
	}

	/**
	 * Stands in for {@code XPath.evaluateExpression(String, InputSource, Class)}.
	 *
	 * @param <T>        the result type
	 * @param xpath      the XPath object
	 * @param expression the expression
	 * @param source     the document to parse
	 * @param type       the result type
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static <T> T evaluateExpression(XPath xpath, String expression, InputSource source, Class<T> type)
			throws XPathExpressionException {
		return xpath.evaluateExpression(expression, parse(source), type);
	}

	/**
	 * Stands in for {@code XPath.evaluateExpression(String, InputSource)}.
	 *
	 * @param xpath      the XPath object
	 * @param expression the expression
	 * @param source     the document to parse
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static XPathEvaluationResult<?> evaluateExpression(XPath xpath, String expression, InputSource source)
			throws XPathExpressionException {
		return xpath.evaluateExpression(expression, parse(source));
	}

	/**
	 * Stands in for {@code XPathExpression.evaluate(InputSource, QName)}.
	 *
	 * @param expression the compiled expression
	 * @param source     the document to parse
	 * @param returnType the result type
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static Object evaluate(XPathExpression expression, InputSource source, QName returnType)
			throws XPathExpressionException {
		return expression.evaluate(parse(source), returnType);
	}

	/**
	 * Stands in for {@code XPathExpression.evaluate(InputSource)}.
	 *
	 * @param expression the compiled expression
	 * @param source     the document to parse
	 * @return the result as text
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static String evaluate(XPathExpression expression, InputSource source) throws XPathExpressionException {
		return expression.evaluate(parse(source));
	}

	/**
	 * Stands in for {@code XPathExpression.evaluateExpression(InputSource, Class)}.
	 *
	 * @param <T>        the result type
	 * @param expression the compiled expression
	 * @param source     the document to parse
	 * @param type       the result type
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static <T> T evaluateExpression(XPathExpression expression, InputSource source, Class<T> type)
			throws XPathExpressionException {
		return expression.evaluateExpression(parse(source), type);
	}

	/**
	 * Stands in for {@code XPathExpression.evaluateExpression(InputSource)}.
	 *
	 * @param expression the compiled expression
	 * @param source     the document to parse
	 * @return the result
	 * @throws XPathExpressionException if the document or the expression fails
	 */
	public static XPathEvaluationResult<?> evaluateExpression(XPathExpression expression, InputSource source)
			throws XPathExpressionException {
		return expression.evaluateExpression(parse(source));
	}

	// ---------------------------------------------------------------- private

	/**
	 * Parses a document for an XPath evaluation with a hardened parser.
	 *
	 * @param source the document
	 * @return the parsed document
	 * @throws XPathExpressionException if the document cannot be parsed
	 */
	private static Document parse(InputSource source) throws XPathExpressionException {
		if (source == null) {
			throw new NullPointerException("The source to evaluate cannot be null");
		}
		try {
			return documentBuilderFactoryNS().newDocumentBuilder().parse(inputSource(source));
		} catch (SecurityException e) {
			throw e;
		} catch (Exception e) {
			throw new XPathExpressionException(e);
		}
	}

	/**
	 * Puts a source's system id under {@link #systemId(String)}.
	 *
	 * @param source   the source
	 * @param systemId its system id
	 */
	private static void setSystemId(Source source, String systemId) {
		String sandboxed = systemId(systemId);
		if (sandboxed != null && !sandboxed.equals(systemId)) {
			source.setSystemId(sandboxed);
		}
	}

	/**
	 * Refuses an object whose class overrides a getter of the JDK class it extends,
	 * since the parser would read what the override answers.
	 *
	 * @param value    the object the project holds
	 * @param declared the JDK class that declares the getter
	 * @param getter   the getter's name
	 * @throws SecurityException if a subclass overrides the getter
	 */
	private static void requireOwnGetter(Object value, Class<?> declared, String getter) {
		try {
			if (value.getClass().getMethod(getter).getDeclaringClass() != declared) {
				throw new SecurityException("A subclass of " + declared.getName() + " that overrides " + getter
						+ " is not permitted in project reactors");
			}
		} catch (NoSuchMethodException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * True for an object of a class the project wrote, which runs inside the
	 * sandbox already.
	 *
	 * @param value a factory, parser or reader
	 * @return true if its class is project code
	 */
	private static boolean isProjectsOwn(Object value) {
		return SandboxedJavaExecution.isProjectClass(value.getClass());
	}

	/**
	 * True for an object of the JDK's own XML implementation.
	 *
	 * @param value a factory, parser or reader
	 * @return true if its class is in the JDK's XML module
	 */
	private static boolean isJdk(Object value) {
		return value.getClass().getModule() == DocumentBuilderFactory.class.getModule();
	}

	/**
	 * Refuses a factory, parser or validator that is not the JDK's, since the
	 * settings are the JDK's and another implementation may not follow them.
	 *
	 * @param value a factory, parser or validator
	 * @throws SecurityException if it is not the JDK's
	 */
	private static void requireJdk(Object value) {
		if (!isJdk(value)) {
			throw new SecurityException("Project reactors use the JDK's XML parsers; " + value.getClass().getName()
					+ " is not one of them");
		}
	}

	/**
	 * The error for an object the settings could not be applied to.
	 *
	 * @param value the factory, parser or reader
	 * @param cause what the setting failed with
	 * @return the error to throw
	 */
	private static SecurityException refused(Object value, Exception cause) {
		return new SecurityException(
				"The XML settings for project reactors could not be applied to " + value.getClass().getName(), cause);
	}

	/**
	 * A reader over the JDK's reader that a project cannot leave unhardened: the
	 * settings are applied again after each feature or property it sets and before
	 * each parse.
	 */
	private static final class HardenedReader implements XMLReader {

		private final XMLReader reader;

		/**
		 * Wraps the JDK's reader.
		 *
		 * @param reader the reader to keep hardened
		 */
		HardenedReader(XMLReader reader) {
			this.reader = reader;
		}

		@Override
		public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
			return this.reader.getFeature(name);
		}

		@Override
		public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
			this.reader.setFeature(name, value);
			hardened(this.reader);
		}

		@Override
		public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
			return this.reader.getProperty(name);
		}

		@Override
		public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
			this.reader.setProperty(name, value);
			hardened(this.reader);
		}

		@Override
		public void setEntityResolver(EntityResolver resolver) {
			this.reader.setEntityResolver(resolver);
		}

		@Override
		public EntityResolver getEntityResolver() {
			return this.reader.getEntityResolver();
		}

		@Override
		public void setDTDHandler(DTDHandler handler) {
			this.reader.setDTDHandler(handler);
		}

		@Override
		public DTDHandler getDTDHandler() {
			return this.reader.getDTDHandler();
		}

		@Override
		public void setContentHandler(ContentHandler handler) {
			this.reader.setContentHandler(handler);
		}

		@Override
		public ContentHandler getContentHandler() {
			return this.reader.getContentHandler();
		}

		@Override
		public void setErrorHandler(ErrorHandler handler) {
			this.reader.setErrorHandler(handler);
		}

		@Override
		public ErrorHandler getErrorHandler() {
			return this.reader.getErrorHandler();
		}

		@Override
		public void parse(InputSource input) throws IOException, SAXException {
			hardened(this.reader).parse(inputSource(input));
		}

		@Override
		public void parse(String systemId) throws IOException, SAXException {
			hardened(this.reader).parse(systemId(systemId));
		}
	}
}
