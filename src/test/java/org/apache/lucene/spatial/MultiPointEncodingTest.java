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
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.util.Arrays;

public class MultiPointEncodingTest extends LuceneTestCase {

  SpatialContext ctx = SpatialContext.GEO;

  @Test
  public void testEncoding() {
    Point[] points = new Point[random().nextInt(4)];
    for (int i = 0; i < points.length; i++) {
      points[i] = ctx.makePoint(randomFloatIn(-180, 180), randomFloatIn(-90, 90));
    }
    BytesRef bytes = MultiPointEncoding.pointsToBytes(Arrays.asList(points));
    float[] output = MultiPointEncoding.bytesToFloats(bytes);
    Arrays.sort(points, MultiPointEncoding.POINT_COMPARATOR);
    float[] inputs = new float[points.length * 2];
    for (int i = 0; i < points.length; i++) {
      inputs[i*2] = (float) points[i].getX();
      inputs[i*2 + 1] = (float) points[i].getY();
    }
    assertArrayEquals(inputs, output, 0.0f);
  }

  private float randomFloatIn(float min, float max) {
    float delta = max - min;
    return delta * random().nextFloat() + min;
  }
}
