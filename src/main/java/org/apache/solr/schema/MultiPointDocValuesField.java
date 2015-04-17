package org.apache.solr.schema;

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
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.spatial.MultiPointEncoding;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.util.MapListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A spatial FieldType for storing a variable number of points in DocValues. It
 * is expressly for sorting / boosting.
 * <p/>
 * I wanted to extend {@link AbstractSpatialFieldType} but createField() is
 * final, which is unfortunate to get around the multi-value limitations in Solr
 * FieldType since I'd like to pass it an IndexableField via an
 * UpdateRequestProcessor but I can't customize it.
 */
public class MultiPointDocValuesField extends FieldType {

  private SpatialContext ctx;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    //COPIED FROM AbstractSpatialFieldType:
    //Solr expects us to remove the parameters we've used.
    MapListener<String, String> argsWrap = new MapListener<String, String>(args);
    ctx = SpatialContextFactory.makeSpatialContext(argsWrap, schema.getResourceLoader().getClassLoader());
    args.keySet().removeAll(argsWrap.getSeenKeys());
  }

  public SpatialContext getCtx() {
    return ctx;
  }

  /**
   * Normally called by Solr's {@link org.apache.solr.update.DocumentBuilder}.
   * It will also be called by {@link org.apache.solr.update.processor.MultiValUpdateRequestProcessorFactory}
   * given a SolrInputField which has access to multiple values. This is
   * arranged to circumvent DocumentBuilder's limitation.
   */
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (field.stored())
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "This field" +
          "cannot be configured as stored: " + field);
    List<Point> points;
    if (value instanceof SolrInputField) {
      SolrInputField inputField = ((SolrInputField) value);
      points = new ArrayList<Point>(inputField.getValueCount());
      for (Object iVal : inputField.getValues()) {
        points.add(pointFromValue(iVal));
      }
    } else if (value instanceof IndexableField) {//result of MultiValUpdateRequestProcessorFactory
      return (IndexableField) value;
    } else {
      points = Collections.singletonList(pointFromValue(value));
    }

    BytesRef bytes = MultiPointEncoding.pointsToBytes(points);
    return new BinaryDocValuesField(field.getName(), bytes);
  }

  private Point pointFromValue(Object value) {
    Point point;
    if (value instanceof Point) {
      point = (Point) value;
    } else {
      point = (Point) ctx.readShape(value.toString());
    }
    return point;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    //do-nothing; not supported; this is not a stored field
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    throw new UnsupportedOperationException("Can't get ValueSource on this field type.");
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new UnsupportedOperationException("Can't sort on this field type.");
  }

}
