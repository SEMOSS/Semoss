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
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.File;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;
import prerna.util.Utility;

/** Deprecated on March 21st 2025. Please use StorageEngine Directly */
@Deprecated
public class S3FileRetrieverReactor extends AbstractQueryStructReactor {

  private static final Logger logger = LogManager.getLogger(S3FileRetrieverReactor.class);

  private static final String BUCKET = "bucket";
  private static final String PATH = "path";

  public S3FileRetrieverReactor() {
    this.keysToGet = S3Utils.addCommonS3Keys(new String[] {BUCKET, PATH});
  }

  @Override
  public String getDescriptionForKey(String key) {
    if (key.equals(BUCKET)) {
      return "S3 bucket name";
    } else if (key.equals(PATH)) {
      return "S3 path to download from";
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
    return "Download and load a csv file from an S3 bucket. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
  }

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    organizeKeys();
    String bucketName = this.keyValue.get(this.keysToGet[0]);
    String path = this.keyValue.get(this.keysToGet[1]);

    if (bucketName == null || bucketName.length() <= 0) {
      throw new IllegalArgumentException("Need to specify bucket name");
    }
    if (path == null || path.length() <= 0) {
      throw new IllegalArgumentException("Need to give file path on s3");
    }

    String filePath =
        Utility.getInsightCacheDir() + DIR_SEPARATOR + Utility.getCsvInsightCacheDir();
    filePath += DIR_SEPARATOR + Utility.getRandomString(10) + ".csv";

    try {
      AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);

      File localFile = new File(filePath);
      s3Client.getObject(new GetObjectRequest(bucketName, path), localFile);
    } catch (SdkClientException e) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      logger.error(Constants.STACKTRACE, e);
    }

    CSVFileHelper helper = new CSVFileHelper();
    helper.setDelimiter(',');
    helper.parse(filePath);
    Map[] predictionMaps =
        FileHelperUtil.generateDataTypeMapsFromPrediction(
            helper.getHeaders(), helper.predictTypes());
    Map<String, String> dataTypes = predictionMaps[0];
    Map<String, String> additionalDataTypes = predictionMaps[1];
    CsvQueryStruct qs = new CsvQueryStruct();
    for (String keys : dataTypes.keySet()) {
      qs.addSelector("DND", keys);
    }
    helper.clear();
    qs.merge(this.qs);
    qs.setFilePath(filePath);
    qs.setDelimiter(',');
    qs.setColumnTypes(dataTypes);
    qs.setAdditionalTypes(additionalDataTypes);

    return qs;
  }
}
