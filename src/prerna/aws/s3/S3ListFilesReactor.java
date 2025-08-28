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
package prerna.aws.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/** Deprecated on March 21st 2025. Please use StorageEngine Directly */
@Deprecated
public class S3ListFilesReactor extends AbstractReactor {

  private static final Logger logger = LogManager.getLogger(S3ListFilesReactor.class);

  private static final String BUCKET = "bucket";
  private static final String PATH = "path";
  private static final String RECURSIVE = "recursive";
  private static final String LIMIT = "limit";

  public S3ListFilesReactor() {
    this.keysToGet = S3Utils.addCommonS3Keys(new String[] {BUCKET, PATH, RECURSIVE, LIMIT});
  }

  @Override
  public String getDescriptionForKey(String key) {
    if (key.equals(BUCKET)) {
      return "S3 bucket name";
    } else if (key.equals(PATH)) {
      return "S3 path to list files from";
    } else if (key.equals(RECURSIVE)) {
      return "Boolean flag to indicate if objects should be listed recursively (default false)";
    } else if (key.equals(LIMIT)) {
      return "Max number of results to list (default 100). Use -1 for unlimited";
    } else {
      String commonDescription = S3Utils.getDescriptionForCommonS3Key(key);
      if (commonDescription != null) {
        return commonDescription;
      }
    }
    return super.getDescriptionForKey(key);
  }

  @Override
  public String getReactorDescription() {
    return "List the files in an S3 bucket. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String bucketName = this.keyValue.get(BUCKET);
    String path = this.keyValue.get(PATH);
    if (bucketName == null || bucketName.isEmpty() || path == null) {
      throw new IllegalArgumentException("Bucket and Path cannot be empty");
    }

    int limit = 100;
    String limitString = this.keyValue.get(LIMIT);
    if (limitString != null && !limitString.isEmpty()) {
      try {
        limit = Integer.parseInt(limitString);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Limit must be an integer");
      }
    }

    List<Map<String, Object>> output = new ArrayList<Map<String, Object>>();
    try {
      AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);

      Boolean isTopLevel = false;
      String delimiter = "/";
      if (path.equals("") || path.equals("/")) {
        isTopLevel = true;
      }
      if (!path.endsWith(delimiter)) {
        path += delimiter;
      }

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
      if (!Boolean.parseBoolean(this.keyValue.get(RECURSIVE))) {
        listObjectsRequest.setDelimiter(delimiter);
      }

      if (!isTopLevel) {
        listObjectsRequest.setPrefix(path);
      }

      if (limit > 0) {
        listObjectsRequest.setMaxKeys(limit);
      }

      com.amazonaws.services.s3.model.ObjectListing objects =
          s3Client.listObjects(listObjectsRequest);

      List<String> folders = objects.getCommonPrefixes();
      List<S3ObjectSummary> files = objects.getObjectSummaries();

      for (String s : folders) {
        Map<String, Object> folderMap = new HashMap<>();
        folderMap.put("name", s);
        folderMap.put("type", "folder");
        output.add(folderMap);
        System.out.println(s);
      }
      // the base path is being added as a file, manually remove on the ret map
      for (S3ObjectSummary o : files) {
        String fileName = o.getKey();
        if (fileName.equals(path)) {
          continue;
        }
        Map<String, Object> fileMap = new HashMap<>();
        System.out.println(o.getKey());
        fileMap.put("name", o.getKey());
        fileMap.put("type", "file");
        output.add(fileMap);
      }
    } catch (SdkClientException e) {
      logger.error(Constants.STACKTRACE, e);
      return getError("Error occurred listing files: " + e.getMessage());
    }

    NounMetadata noun =
        new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
    return noun;
  }
}
