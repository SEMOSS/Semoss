//package prerna.cluster.util;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.NoSuchElementException;
//
//import net.lingala.zip4j.core.ZipFile;
//import net.lingala.zip4j.exception.ZipException;
//import net.lingala.zip4j.model.ZipParameters;
//import net.lingala.zip4j.util.Zip4jConstants;
//
//import org.apache.commons.io.FileUtils;
//import org.jclouds.ContextBuilder;
//import org.jclouds.blobstore.BlobStoreContext;
//import org.jclouds.blobstore.domain.Blob;
//
//import com.google.common.io.Files;
//
//public class LMArchiver {
//
//	// things I need to do here
//	// get to the local master
//	// zip it up with the password
//	// Methods
//
//	// a. Store Local Master
//	// b. Pull and explode local master - we need to be able to start without a
//	// local master now
//	// c. This has to be internal
//	
//	String dbFolder = "/opt/semosshome/db/";
//
//	public enum PROVIDER {
//		S3, AZURE
//	};
//
//	public void archiveLM(String username, String password, String name,
//			String access) {
//		// need to stop the local master
//		// add this to a zip with the password
//		// based on provider move the blob to that provider
//
//		prepareLM(true);
//
//		
//		String zipName = zipLM(username, password);
//
//		username = generateDigest(username);
//
//		pushBlob(username, zipName, name, access);
//	}
//	
//	public void unArchiveLM(String username, String password, String name,
//			String access) {
//		// need to stop the local master
//		// add this to a zip with the password
//		// based on provider move the blob to that provider
//
//		username = generateDigest(username);
//
//		pullBlob(username, name, access);
//		
//		unzipLM(username, password);
//
//		prepareLM(false);
//	}
//
//
//	public void prepareLM(boolean zip) {
//		// move the local master
//		File from = new File(dbFolder + "LocalMasterDatabase.smss");
//		File to = new File(
//				dbFolder + "LocalMasterDatabase/LocalMasterDatabase.smss");
//		try {
//			if (zip)
//				Files.move(from, to);
//			else
//				Files.move(to, from);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//
//	}
//	
//	public String generateDigest(String user)
//	{
//		try {
//			MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
//			
//			String output = messageDigest.digest(user.getBytes()).toString();
//			
//			return output;
//		} catch (NoSuchAlgorithmException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		return null;
//	}
//
//	public String zipLM(String username, String password) {
//		try {
//			String zipFileName = dbFolder + username + ".zip";
//
//			ZipFile zipFile = new ZipFile(zipFileName);
//
//			// Initiate Zip Parameters which define various properties
//			ZipParameters parameters = new ZipParameters();
//			parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE); // set
//																			// compression
//																			// method
//																			// to
//																			// deflate
//																			// compression
//
//			// DEFLATE_LEVEL_FASTEST - Lowest compression level but higher speed
//			// of compression
//			// DEFLATE_LEVEL_FAST - Low compression level but higher speed of
//			// compression
//			// DEFLATE_LEVEL_NORMAL - Optimal balance between compression
//			// level/speed
//			// DEFLATE_LEVEL_MAXIMUM - High compression level with a compromise
//			// of speed
//			// DEFLATE_LEVEL_ULTRA - Highest compression level but low speed
//			parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_ULTRA);
//
//			// Set the encryption flag to true
//			parameters.setEncryptFiles(true);
//
//			// Set the encryption method to AES Zip Encryption
//			parameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_AES);
//
//			// AES_STRENGTH_128 - For both encryption and decryption
//			// AES_STRENGTH_192 - For decryption only
//			// AES_STRENGTH_256 - For both encryption and decryption
//			// Key strength 192 cannot be used for encryption. But if a zip file
//			// already has a
//			// file encrypted with key strength of 192, then Zip4j can decrypt
//			// this file
//			parameters.setAesKeyStrength(Zip4jConstants.AES_STRENGTH_256);
//
//			// Set password
//			parameters.setPassword(password);
//
//			// Now add files to the zip file
//			// zipFile.addFiles(filesToAdd, parameters);
//			// zipFile.setPassword("howtodoinjava");
//			// zipFile.extractAll("c:/temp"); //, uparameters);
//			zipFile.addFolder(dbFolder + "LocalMasterDatabase",
//					parameters);
//
//			return zipFileName;
//		} catch (ZipException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//
//		return null;
//	}
//
//	public void unzipLM(String zipFileName, String password) {
//		try {
//			ZipFile zipFile = new ZipFile(zipFileName);
//			zipFile.setPassword(password);
//			zipFile.extractAll(dbFolder);
//
//		} catch (Exception ex) {
//			classLogger.error(Constants.STACKTRACE, ex);
//		}
//	}
//
//	public void pushBlob(String blobName, String zipName, String acName, String acKey) {
//		BlobStoreContext context = ContextBuilder.newBuilder("azureblob")
//				.credentials(acName, acKey).buildView(BlobStoreContext.class);
//
//		// Access the BlobStore
//		org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();
//		
//		// try to see if the username exists as the container
//		// if not create the username container and then plug this in
//
//		if(!blobStore.containerExists(blobName))
//			blobStore.createContainerInLocation(null, blobName);
//
//		Blob blob = context.getBlobStore().blobBuilder("data")
//				.payload(Files.asByteSource(new File(zipName)))
//				// .contentLength(payload.size())
//				.build();
//		
//		blobStore.putBlob(blobName, blob);
//
//		context.close();
//	}
//	
//	public void pullBlob(String blobName, String acName, String acKey)
//	{
//		try {
//			BlobStoreContext context = ContextBuilder.newBuilder("azureblob")
//			        .credentials(acName, acKey)
//			        .buildView(BlobStoreContext.class);
//
//			// Access the BlobStore
//			org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();
//			
//			Blob blob = blobStore.getBlob(blobName, "data");
//			
//			File targetFile= new File(dbFolder + blobName + ".zip");
//			InputStream inp = blob.getPayload().openStream();
//			
//			FileUtils.copyInputStreamToFile(inp, targetFile);
//		} catch (NoSuchElementException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}		
//
//	}
//}
