package org.apache.lucene.spatial;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Responsible for encoding points to bytes, and also to getting the distance to
 * the nearest point from a given point.
 */
public class MultiPointEncoding {

  private static final int POINT_LEN = 4 * 2;

  static final Comparator<? super Point> POINT_COMPARATOR = new Comparator<Point>() {
    @Override
    public int compare(Point o1, Point o2) {
      int result = Double.compare(o1.getX(),o2.getX());
      if (result == 0)
        result = Double.compare(o1.getY(),o2.getY());
      return result;
    }
  };

  public static BytesRef pointsToBytes(List<Point> points) {
    if (points.size() > 1) {
      Collections.sort(points, POINT_COMPARATOR);
    }
    BytesRef bytes = new BytesRef(points.size() * POINT_LEN);
    int offset = 0;
    for (Point point : points) {
      offset += writeFloat(point.getX(), bytes.bytes, offset);
      offset += writeFloat(point.getY(), bytes.bytes, offset);
    }
    bytes.length = bytes.bytes.length;
    assert offset == bytes.length;
    return bytes;
  }

  private static int writeFloat(double vDbl, byte[] bytes, int offset) {
    //ported from DataOutputStream.writeFloat()
    writeInt(Float.floatToIntBits((float) vDbl), bytes, offset);
    return 4;
  }

  private static void writeInt(int v, byte[] bytes, int offset) {
    bytes[offset + 0] = (byte) ((v >>> 24) & 0xFF);
    bytes[offset + 1] = (byte) ((v >>> 16) & 0xFF);
    bytes[offset + 2] = (byte) ((v >>> 8) & 0xFF);
    bytes[offset + 3] = (byte) ((v >>> 0) & 0xFF);
  }

  public static double calcDistance(Point point, BytesRef bytes, SpatialContext ctx) {
    float[] floats = bytesToFloats(bytes);//x y pair order
    double minDist = Double.MAX_VALUE;
    for (int i = 0; i < floats.length; i += 2) {
      float x = floats[i];
      float y = floats[i + 1];
      double dist = ctx.getDistCalc().distance(point, x, y);
      minDist = Math.min(minDist, dist);
    }
    return minDist;
  }

  static float[] bytesToFloats(BytesRef bytes) {
    float[] floats = new float[bytes.length / POINT_LEN * 2];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = readFloat(bytes.bytes, bytes.offset + i * 4);
    }
    return floats;
  }

  private static float readFloat(byte[] bytes, int offset) {
    return Float.intBitsToFloat(readInt(bytes, offset));
  }

  private static int readInt(byte[] bytes, int offset) {
    ByteBuffer bb = ByteBuffer.wrap(bytes, offset, 4);
    bb.getInt();
    //ported from DataInputStream.readInt()
    int ch1 = bytes[offset + 0] & 0xff;
    int ch2 = bytes[offset + 1] & 0xff;
    int ch3 = bytes[offset + 2] & 0xff;
    int ch4 = bytes[offset + 3] & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }
}
