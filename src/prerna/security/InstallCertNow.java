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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Utility;

public class InstallCertNow {

	private static final Logger logger = LogManager.getLogger(InstallCertNow.class);

	private static final String STACKTRACE = "StackTrace: ";

	public static void main(String [] args)
	{
		try {
			InstallCertNow.please("https://www.google.com/drive", null, null);
		} catch (Exception e) {
			logger.error(STACKTRACE, e);
		}
	}
	public static void please(String site, String javaLoc, String pass) throws Exception {
		  // need a couple of different parameters
		  // the URL for the site to get the certificate from
		  // the location of java
		  // and possibly the passphrase
		  // java home looks like this - C:\Java\jdk1.8.0_161

		  if(javaLoc == null)
		    // see if you can get it from environment variable
		    javaLoc = System.getenv("JAVA_HOME");

		  if(site == null || javaLoc == null)
		    return;

		  // mod the java locl
		  javaLoc = Utility.normalizePath(javaLoc) + "/jre/lib/security/cacerts";

		  if(pass == null)
		    pass = "changeit";

		  String host;
		  int port;
		  char[] passphrase;
		  String[] c = site.split(":");
		  host = c[0];
		  port = (c.length == 1) ? 443 : Integer.parseInt(c[1]);
		  passphrase = pass.toCharArray();

		  File file = new File(javaLoc);

		  logger.debug("Loading KeyStore " + file + "...");
		  InputStream in = null;
		  SSLSocket socket = null;
		  try {
		    in = new FileInputStream(file);
		    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		    ks.load(in, passphrase);
		    in.close();

		    SSLContext context = SSLContext.getInstance("TLSv1.2");
		    TrustManagerFactory tmf =
		        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

		    tmf.init(ks);
		    X509TrustManager defaultTrustManager = (X509TrustManager)tmf.getTrustManagers()[0];
		    SavingTrustManager tm = new SavingTrustManager(defaultTrustManager);
		    context.init(null, new TrustManager[] {tm}, null);
		    SSLSocketFactory factory = context.getSocketFactory();

		    logger.debug("Opening connection to " + host + ":" + port + "...");
		    socket = (SSLSocket)factory.createSocket(host, port);
		    socket.setSoTimeout(10000);

		    try {
		      logger.debug("Starting SSL handshake...");
		      socket.startHandshake();
		      socket.close();
		      logger.debug("No errors, certificate is already trusted");
		    } catch (SSLException e) {
		      logger.error(STACKTRACE, e);
		    }

		    X509Certificate[] chain = tm.chain;
		    if (chain == null) {
		      logger.warn("Could not obtain server certificate chain");
		      return;
		    }

		    logger.debug("Server sent " + chain.length + " certificate(s):");
		    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
		    for (int i = 0; i < chain.length; i++) {
		      X509Certificate cert = chain[i];
		      logger.debug(" " + (i + 1) + " Subject " + cert.getSubjectDN());
		      logger.debug("   Issuer  " + cert.getIssuerDN());
		      sha256.update(cert.getEncoded());
		      logger.debug("   sha256  " + toHexString(sha256.digest()));
		    }

		    String line = "1";
		    int k;
		    try {
		      k = (line.length() == 0) ? 0 : Integer.parseInt(line) - 1;
		    } catch (NumberFormatException e) {
		      logger.error("KeyStore not changed - StackTrace: ", e);
		      return;
		    }

		    X509Certificate cert = chain[k];
		    String alias = host + "-" + (k + 1);

		    // there can't be more than 20 netskope hosts for the cert
		    for(int i = 0;i < 20 && ks.containsAlias(alias);i++)
		    {
		      alias = host + "-" + i;
		    }
		    ks.setCertificateEntry(alias, cert);

		    OutputStream out = new FileOutputStream(javaLoc);
		    ks.store(out, passphrase);
		    out.close();

		    logger.debug(cert);
		    logger.debug("Added certificate to keystore 'jssecacerts' using alias '" + alias + "'");
		  } finally {
		    try {
		      if(in != null) {
		        in.close();
		      }
		    } catch (IOException e) {
		      logger.error(Constants.STACKTRACE, e);
		    }

		    try {
		      if(socket != null) {
		        socket.close();
		      }
		    } catch (IOException e) {
		    	logger.error(Constants.STACKTRACE, e);
		    }
		  }
		}


	private static final char[] HEXDIGITS = "0123456789abcdef".toCharArray();

	private static String toHexString(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length * 3);
		for (int b : bytes) {
			b &= 0xff;
			sb.append(HEXDIGITS[b >> 4]);
			sb.append(HEXDIGITS[b & 15]);
			sb.append(' ');
		}

		return sb.toString();
	}

	private static class SavingTrustManager implements X509TrustManager {
		private final X509TrustManager tm;
		private X509Certificate[] chain;

		SavingTrustManager(X509TrustManager tm) {
			this.tm = tm;
		}

		public X509Certificate[] getAcceptedIssuers() {
			throw new UnsupportedOperationException();
		}

		public void checkClientTrusted(X509Certificate[] chain, String authType)
				throws CertificateException {
			throw new UnsupportedOperationException();
		}

		public void checkServerTrusted(X509Certificate[] chain, String authType)
				throws CertificateException {
			this.chain = chain;
			tm.checkServerTrusted(chain, authType);
		}
	}
}
