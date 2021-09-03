///*******************************************************************************
// * Copyright 2015 Defense Health Agency (DHA)
// *
// * If your use of this software does not include any GPLv2 components:
// * 	Licensed under the Apache License, Version 2.0 (the "License");
// * 	you may not use this file except in compliance with the License.
// * 	You may obtain a copy of the License at
// *
// * 	  http://www.apache.org/licenses/LICENSE-2.0
// *
// * 	Unless required by applicable law or agreed to in writing, software
// * 	distributed under the License is distributed on an "AS IS" BASIS,
// * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * 	See the License for the specific language governing permissions and
// * 	limitations under the License.
// * ----------------------------------------------------------------------------
// * If your use of this software includes any GPLv2 components:
// * 	This program is free software; you can redistribute it and/or
// * 	modify it under the terms of the GNU General Public License
// * 	as published by the Free Software Foundation; either version 2
// * 	of the License, or (at your option) any later version.
// *
// * 	This program is distributed in the hope that it will be useful,
// * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
// * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * 	GNU General Public License for more details.
// *******************************************************************************/
//package prerna.security;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.OutputStream;
//import java.security.KeyStore;
//import java.security.MessageDigest;
//import java.security.cert.CertificateException;
//import java.security.cert.X509Certificate;
//
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLException;
//import javax.net.ssl.SSLSocket;
//import javax.net.ssl.SSLSocketFactory;
//import javax.net.ssl.TrustManager;
//import javax.net.ssl.TrustManagerFactory;
//import javax.net.ssl.X509TrustManager;
//
//public class InstallCert {
//
//	//	private static final Logger logger = LogManager.getLogger(InstallCert.class);
//	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
//
//	public static void main(String[] args) throws Exception {
//		String host;
//		int port;
//		char[] passphrase;
//		if ((args.length == 1) || (args.length == 2)) {
//			String[] c = args[0].split(":");
//			host = c[0];
//			port = (c.length == 1) ? 443 : Integer.parseInt(c[1]);
//			String p = (args.length == 1) ? "changeit" : args[1];
//			passphrase = p.toCharArray();
//		} else {
//			System.out.println("Usage: java InstallCert <host>[:port] [passphrase]");
//			//		    logger.info("Usage: java InstallCert <host>[:port] [passphrase]");
//			return;
//		}
//
//		File file = new File("C:\\Java\\jdk1.8.0_161\\jre\\lib\\security\\cacerts");
//		if (file.isFile() == false) {
//			char SEP = FILE_SEPARATOR.toCharArray()[0];
//			File dir = new File(System.getProperty("java.home") + SEP
//					+ "lib" + SEP + "security");
//			file = new File(dir, "C:\\Java\\jdk1.8.0_161\\jre\\lib\\security\\cacerts");
//			if (file.isFile() == false) {
//				file = new File(dir, "C:\\Java\\jdk1.8.0_161\\jre\\lib\\security\\cacerts");
//			}
//		}
//		System.out.println("Loading KeyStore " + file + "...");
//		//		logger.info("Loading KeyStore " + file + "...");
//		InputStream in = null;
//		SSLSocket socket = null;
//		try {
//			in = new FileInputStream(file);
//			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
//			ks.load(in, passphrase);
//			in.close();
//
//			SSLContext context = SSLContext.getInstance("TLS");
//			TrustManagerFactory tmf =
//					TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
//			tmf.init(ks);
//			X509TrustManager defaultTrustManager = (X509TrustManager)tmf.getTrustManagers()[0];
//			SavingTrustManager tm = new SavingTrustManager(defaultTrustManager);
//			context.init(null, new TrustManager[] {tm}, null);
//			SSLSocketFactory factory = context.getSocketFactory();
//
//			System.out.println("Opening connection to " + host + ":" + port + "...");
//			//		logger.info("Opening connection to " + host + ":" + port + "...");
//			socket = (SSLSocket)factory.createSocket(host, port);
//			socket.setSoTimeout(10000);
//			try {
//				System.out.println("Starting SSL handshake...");
//				//		    logger.info("Starting SSL handshake...");
//				socket.startHandshake();
//				socket.close();
//				System.out.println("No errors, certificate is already trusted");
//				//		    logger.info("No errors, certificate is already trusted");
//			} catch (SSLException e) {
//				e.printStackTrace();
//				//			logger.error("StackTrace: ", e);
//			}
//
//
//			X509Certificate[] chain = tm.chain;
//			if (chain == null) {
//				System.out.println("Could not obtain server certificate chain");
//				//		    logger.info("Could not obtain server certificate chain");
//				return;
//			}
//
//
//			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//
//			System.out.println("Server sent " + chain.length + " certificate(s):");
//			//		logger.info("Server sent " + chain.length + " certificate(s):");
//			MessageDigest sha1 = MessageDigest.getInstance("SHA1");
//			MessageDigest md5 = MessageDigest.getInstance("MD5");
//			for (int i = 0; i < chain.length; i++) {
//				X509Certificate cert = chain[i];
//				System.out.println(" " + (i + 1) + " Subject " + cert.getSubjectDN());
//				System.out.println("   Issuer  " + cert.getIssuerDN());
//				//		    logger.info(" " + (i + 1) + " Subject " + cert.getSubjectDN());
//				//		    logger.info("   Issuer  " + cert.getIssuerDN());
//				sha1.update(cert.getEncoded());
//				System.out.println("   sha1    " + toHexString(sha1.digest()));
//				//		    logger.info("   sha1    " + toHexString(sha1.digest()));
//				md5.update(cert.getEncoded());
//				System.out.println("   md5     " + toHexString(md5.digest()));
//				//		    logger.info("   md5     " + toHexString(md5.digest()));
//			}
//
//
//			System.out.println("Enter certificate to add to trusted keystore or 'q' to quit: [1]");
//			//		logger.info("Enter certificate to add to trusted keystore or 'q' to quit: [1]");
//			String line = reader.readLine().trim();
//			int k;
//			try {
//				k = (line.length() == 0) ? 0 : Integer.parseInt(line) - 1;
//			} catch (NumberFormatException e) {
//				System.out.println("KeyStore not changed");
//				//		    logger.info("KeyStore not changed");
//				return;
//			}
//
//
//			X509Certificate cert = chain[k];
//			String alias = host + "-" + (k + 1);
//			ks.setCertificateEntry(alias, cert);
//
//			OutputStream out = new FileOutputStream("C:\\Java\\jdk1.8.0_161\\jre\\lib\\security\\cacerts");
//			ks.store(out, passphrase);
//			out.close();
//
//			System.out.println(cert);
//			System.out.println("Added certificate to keystore 'jssecacerts' using alias '" + alias + "'");
//		}finally {
//
//		      try {
//		          if(in != null) {
//		        	  in.close();
//		               }
//		       } catch (IOException e) {
//		               e.printStackTrace();
//		       }
//		      
//		      try {
//		          if(socket != null) {
//		        	  socket.close();
//		               }
//		       } catch (IOException e) {
//		               e.printStackTrace();
//		       }
//		}
//		//		logger.info(cert);
//		//		logger.info("Added certificate to keystore 'jssecacerts' using alias '" + alias + "'");
//	}
//
//	private static final char[] HEXDIGITS = "0123456789abcdef".toCharArray();
//
//	private static String toHexString(byte[] bytes) {
//		StringBuilder sb = new StringBuilder(bytes.length * 3);
//		for (int b : bytes) {
//			b &= 0xff;
//			sb.append(HEXDIGITS[b >> 4]);
//			sb.append(HEXDIGITS[b & 15]);
//			sb.append(' ');
//		}
//		return sb.toString();
//	}
//
//	private static class SavingTrustManager implements X509TrustManager {
//
//		private final X509TrustManager tm;
//		private X509Certificate[] chain;
//
//		SavingTrustManager(X509TrustManager tm) {
//			this.tm = tm;
//		}
//
//		public X509Certificate[] getAcceptedIssuers() {
//			throw new UnsupportedOperationException();
//		}
//
//		public void checkClientTrusted(X509Certificate[] chain, String authType)
//				throws CertificateException {
//			throw new UnsupportedOperationException();
//		}
//
//		public void checkServerTrusted(X509Certificate[] chain, String authType)
//				throws CertificateException {
//			this.chain = chain;
//			tm.checkServerTrusted(chain, authType);
//		}
//	}
//
//}
