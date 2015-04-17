package org.apache.solr.search.function.distance;

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
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.io.ParseUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.ReciprocalFloatFunction;
import org.apache.lucene.spatial.MultiPointEncoding;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.MultiPointDocValuesField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/** Yields a pseudo-distance substitute for sorting or relevancy boosting. Only
 * works with {@link MultiPointDocValuesField}.
 */
public class MultiPointDistanceValueSourceParser extends ValueSourceParser {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    String fieldName = fp.parseId();
    SchemaField field = fp.getReq().getSchema().getField(fieldName);
    FieldType type = field.getType();
    if (!(type instanceof MultiPointDocValuesField))
      throw new SyntaxError("This function only supports fields of type "+
          MultiPointDocValuesField.class.getName()+", not "+type.getClass().getName());
    MultiPointDocValuesField mpdvFieldType = (MultiPointDocValuesField) type;

    double[] parsedLatLong = null;
    try {
      parsedLatLong = ParseUtils.parseLatitudeLongitude(fp.parseArg());
    } catch (InvalidShapeException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    double y = parsedLatLong[0];
    double x = parsedLatLong[1];

    SpatialContext ctx = mpdvFieldType.getCtx();
    Point point = ctx.makePoint(x, y);

    String score = fp.getLocalParams().get("score", "distance");
    ValueSource valueSource = new MultiPointDistanceValueSource(fieldName, point, ctx);

    if ("distance".equals(score)) {
      return valueSource;
    }
    else if ("recipDistance".equals(score)) {
      int shift = fp.getLocalParams().getInt("shift", 100);
      int maxScore = fp.getLocalParams().getInt("maxScore", 10);
      return new ReciprocalFloatFunction(valueSource, maxScore, shift, shift);
    }
    else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'score' local-param must be one of 'distance', or 'recipDistance'");
    }
  }

  private class MultiPointDistanceValueSource extends ValueSource {
    private final String fieldName;
    private final Point point;
    private final SpatialContext ctx;

    public MultiPointDistanceValueSource(String fieldName, Point point, SpatialContext ctx) {
      this.fieldName = fieldName;
      this.point = point;
      this.ctx = ctx;
    }

    @Override
    public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
      final AtomicReader reader = readerContext.reader();
      final BinaryDocValues docValues = reader.getBinaryDocValues(fieldName);
      if (docValues == null)
        return null;

      return new DoubleDocValues(this) {
        public BytesRef scratch = null;

        //TODO why is the value being requested twice per doc?!
        int lastDoc = -1;
        double lastVal;

        @Override
        public double doubleVal(int doc) {
          if (doc == lastDoc)
            return lastVal;

          //sanity check; shouldn't be necessary
          if (doc < 0 || doc >= reader.maxDoc())
            throw new IllegalStateException("Bad doc "+doc+" for reader "+reader);

          BytesRef bytes = null;
          try {//shouldn't be necessary
            scratch = docValues.get(doc);
            bytes = scratch;
          } catch (ArrayIndexOutOfBoundsException e) {
            if (log.isErrorEnabled())
              log.error("DocValues index corruption for docid "+doc+" reader "+reader);//don't log 'e'
          }
          if (bytes != null)
            lastVal = MultiPointEncoding.calcDistance(point, bytes, ctx);
          else
            lastVal = 1;//1 degree away, 111.2km

          lastDoc = doc;
          return lastVal;
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MultiPointDistanceValueSource that = (MultiPointDistanceValueSource) o;

      if (!fieldName.equals(that.fieldName)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return fieldName.hashCode();
    }

    @Override
    public String description() {
      return "mpdv("+fieldName+")";
    }
  }
}
