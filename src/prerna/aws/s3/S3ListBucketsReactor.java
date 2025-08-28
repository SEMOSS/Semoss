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
import com.amazonaws.services.s3.model.Bucket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/** Deprecated on March 21st 2025. Please use StorageEngine Directly */
@Deprecated
public class S3ListBucketsReactor extends AbstractReactor {

  private static final Logger logger = LogManager.getLogger(S3ListBucketsReactor.class);

  public S3ListBucketsReactor() {
    this.keysToGet = S3Utils.addCommonS3Keys(null);
  }

  @Override
  public String getDescriptionForKey(String key) {
    String commonDescription = S3Utils.getDescriptionForCommonS3Key(key);
    if (commonDescription != null) {
      return commonDescription;
    }
    return super.getDescriptionForKey(key);
  }

  @Override
  public String getReactorDescription() {
    return "List the bucket names accessible in S3. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    List<HashMap<String, Object>> bucketList = new ArrayList<HashMap<String, Object>>();
    try {
      AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);

      List<Bucket> buckets = s3Client.listBuckets();
      for (Bucket b : buckets) {
        HashMap<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put("name", b.getName());
        bucketList.add(tempMap);
        logger.debug("* " + b.getName());
      }
    } catch (SdkClientException e) {
      logger.error(Constants.STACKTRACE, e);
      return getError("Error occurred listing buckets: " + e.getMessage());
    }

    NounMetadata noun =
        new NounMetadata(bucketList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
    return noun;
  }
}
