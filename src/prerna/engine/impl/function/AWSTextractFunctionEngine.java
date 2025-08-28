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
package prerna.engine.impl.function;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IStorageEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTextractFunctionEngine extends AbstractFunctionEngine {

  private static final Logger classLogger = LogManager.getLogger(AWSTextractFunctionEngine.class);

  public static final String ACCESS_KEY = "ACCESS_KEY";
  public static final String SECRET_KEY = "SECRET_KEY";
  public static final String REGION = "REGION";
  public static final String BUCKETNAME = "BUCKETNAME";

  private String accessKey;
  private String secretKey;
  private String region;
  private String bucketPath;

  private AmazonTextract textractClient = null;

  @Override
  public void open(Properties smssProp) throws Exception {
    super.open(smssProp);

    this.accessKey = smssProp.getProperty(ACCESS_KEY);
    this.secretKey = smssProp.getProperty(SECRET_KEY);
    this.region = smssProp.getProperty(REGION);
    this.bucketPath = smssProp.getProperty(BUCKETNAME);

    if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
      throw new RuntimeException("Must define the requiredParameters");
    }
    if (this.accessKey == null || this.accessKey.isEmpty()) {
      throw new RuntimeException("Must pass in an access key");
    }
    if (this.secretKey == null || this.secretKey.isEmpty()) {
      throw new RuntimeException("Must pass in a secret key");
    }
    if (this.region == null || this.region.isEmpty()) {
      throw new RuntimeException("Must pass in a region");
    }
    if (this.bucketPath == null || this.bucketPath.isEmpty()) {
      throw new RuntimeException("Must pass in a S3BucketPath");
    }
    try {
      BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
      this.textractClient =
          AmazonTextractClientBuilder.standard()
              .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
              .withRegion(this.region)
              .build();
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
  }

  @Override
  public Object execute(Map<String, Object> parameterValues) {
    Insight executingInsight = (Insight) parameterValues.remove(Constants.INSIGHT);

    Object output = null;
    String documentKeyName = null;
    String S3BucketEngineId = null;

    // validate all the required keys are set
    if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
      Set<String> missingPs = new HashSet<>();
      for (String requiredP : this.requiredParameters) {
        if (!parameterValues.containsKey(requiredP)) {
          missingPs.add(requiredP);
        }
      }
      if (!missingPs.isEmpty()) {
        throw new IllegalArgumentException("Must define required keys = " + missingPs);
      }
    }

    try {
      for (String k : parameterValues.keySet()) {
        if (k.contains("filepathInS3")) {
          File file = new File(parameterValues.get(k).toString());
          documentKeyName = file.getName();
          String filePath = parameterValues.get(k).toString();
          int startIndex = filePath.indexOf('/') + 1;
          int endIndex = filePath.lastIndexOf('/');
          String folderPath;
          if (startIndex <= endIndex && startIndex < filePath.length()) {
            folderPath = filePath.substring(startIndex, endIndex);
            folderPath += "/" + documentKeyName;
          } else {
            folderPath = documentKeyName; // Handle the case where there is no subfolder
          }
          System.out.println("folderName: " + folderPath);

          int endIndex1 = filePath.indexOf('/');
          String bucketname = filePath.substring(0, endIndex1);
          System.out.println("bucketname" + bucketname);

          boolean identifyBucket = listObjects(bucketname, folderPath);
          if (identifyBucket) {
            output = textractFromDocument(documentKeyName, bucketname);
          } else {
            output = "Must provide the valid path";
            throw new RuntimeException("Must provide the valid path");
          }

        } else if (k.contains("uploadedfilepath") && k.contains("S3BucketEngineId")) {
          if (parameterValues.containsKey("uploadedfilepath")) {
            File file = new File(parameterValues.get(k).toString());
            documentKeyName = file.getName(); // The name of the file in the bucket
          } else if (parameterValues.containsKey("S3BucketEngineId")) {
            S3BucketEngineId = parameterValues.get(k).toString();
          }

          /*
               BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
               AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                          .withRegion(this.region)
                          .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                          .build();

                  // Upload the file to the bucket
                  s3Client.putObject(new PutObjectRequest(this.S3BucketName, documentKeyName, file));
          */

          int startIndex = this.bucketPath.indexOf('/') + 1;
          int endIndex = this.bucketPath.lastIndexOf('/');
          String folderS3 = null;
          String folderPath;
          if (startIndex <= endIndex && startIndex < this.bucketPath.length()) {
            folderPath = this.bucketPath.substring(startIndex, endIndex);
            folderS3 = folderPath;
            folderPath += "/" + documentKeyName;
          } else {
            folderPath = documentKeyName; // Handle the case where there is no subfolder
          }
          System.out.println("folderName: " + folderPath);

          int endIndex1 = this.bucketPath.indexOf('/');
          String bucketname = this.bucketPath.substring(0, endIndex1);
          System.out.println("bucketname" + bucketname);
          boolean identifyBucket = listObjects(bucketname, folderPath);

          IStorageEngine storage = Utility.getStorage(S3BucketEngineId);
          Map<String, Object> map = new HashMap<>();
          map.put("functionalityUsed", documentKeyName + "-textract_functionality");

          if (identifyBucket) {
            storage.syncLocalToStorage(folderPath, bucketname, map);
            output = textractFromDocument(documentKeyName, bucketname);
          } else {
            createFolderinS3(bucketname, folderS3);
            storage.syncLocalToStorage(folderPath, bucketname, map);
            output = textractFromDocument(documentKeyName, bucketname);
          }
        }
      }
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    return output;
  }

  public List<String> textractFromDocument(String documentName, String S3BucketPath) {
    List<String> extractedTextFromDoc = new ArrayList<String>();
    try {
      // Create the StartDocumentTextDetection request
      StartDocumentTextDetectionRequest request =
          new StartDocumentTextDetectionRequest()
              .withDocumentLocation(
                  new DocumentLocation()
                      .withS3Object(
                          new S3Object().withBucket(S3BucketPath).withName(documentName)));

      // Start text detection
      StartDocumentTextDetectionResult result =
          this.textractClient.startDocumentTextDetection(request);

      // results
      GetDocumentTextDetectionRequest getRequest =
          new GetDocumentTextDetectionRequest().withJobId(result.getJobId());
      GetDocumentTextDetectionResult getResult;
      String nextToken = null;

      do {
        getRequest.setNextToken(nextToken);
        do {
          getResult = this.textractClient.getDocumentTextDetection(getRequest);
        } while (!getResult.getJobStatus().equals("SUCCEEDED"));
        nextToken = getResult.getNextToken();

        for (Block block : getResult.getBlocks()) {
          if ("PAGE".equals(block.getBlockType())) {
            int pageNumber = block.getPage();
            StringBuilder pageText = new StringBuilder();
            for (Block item : getResult.getBlocks()) {
              if (item.getPage() == pageNumber && "LINE".equals(item.getBlockType())) {
                pageText.append(item.getText());
              }
            }
            extractedTextFromDoc.add(pageText.toString());
          }
        }
      } while (nextToken != null);

      System.out.println(extractedTextFromDoc.size());
      System.out.println(extractedTextFromDoc);
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      e.printStackTrace();
    }
    return extractedTextFromDoc;
  }

  public Boolean listObjects(String bucketName, String folderPath) {
    BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
    AmazonS3 s3Client =
        AmazonS3ClientBuilder.standard()
            .withRegion(this.region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();
    boolean result = false;
    boolean bucketExists = doesBucketExist(s3Client, bucketName);
    System.out.println("Bucket exists: " + bucketExists);

    if (bucketExists) {
      try {
        // Check if the object exists
        s3Client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, folderPath));
        System.out.println("File exists.");
        result = true;
      } catch (com.amazonaws.services.s3.model.AmazonS3Exception e) {
        if (e.getStatusCode() == 404) {
          result = false;
        } else {
          e.printStackTrace();
        }
      }
    } else {
      result = false;
    }

    return result;
  }

  private static boolean doesBucketExist(AmazonS3 s3Client, String bucketName) {
    try {
      s3Client.headBucket(new HeadBucketRequest(bucketName));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void createFolderinS3(String bucketName, String folderPath) {
    BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
    AmazonS3 s3Client =
        AmazonS3ClientBuilder.standard()
            .withRegion(this.region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build();

    ByteArrayInputStream emptyInputStream = new ByteArrayInputStream(new byte[0]);
    // Create an empty object (folder) in S3
    s3Client.putObject(new PutObjectRequest(bucketName, folderPath, emptyInputStream, null));
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getCatalogSubType(Properties smssProp) {
    return "AWS";
  }
}
