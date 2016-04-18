/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.collection;

import java.util.UUID;

public final class UUIDRegionKey implements java.io.Serializable, Comparable<UUIDRegionKey> {
  private final UUID uuid;
  private final int bucketId;

  public UUIDRegionKey(int bucketId) {
    this.uuid = UUID.randomUUID();
    this.bucketId = bucketId;
  }

  public UUIDRegionKey(int bucketId, UUID batchID) {
    this.uuid = batchID;
    this.bucketId = bucketId;
  }

  public static Object parseAndGetBucketId(String uuidStr) {
    final int colonIdx = uuidStr.lastIndexOf(":");
    if (colonIdx < 0 || uuidStr.split("-").length != 5) {
      return uuidStr;
    }

    return Integer.parseInt(uuidStr.substring(colonIdx + 1));
  }

  @Override
  public int compareTo(UUIDRegionKey other) {
    final int uuidCompare = uuid.compareTo(other.uuid);
    return (uuidCompare != 0 ? uuidCompare :
        this.bucketId < other.bucketId ? -1 :
            this.bucketId > other.bucketId ? 1 :
                0);
  }

  public String toString() {
    return uuid.toString() + ":" + this.bucketId;
  }

  public UUID getUUID() {
    return uuid;
  }

  public int getBucketId() {
    return bucketId;
  }
}
