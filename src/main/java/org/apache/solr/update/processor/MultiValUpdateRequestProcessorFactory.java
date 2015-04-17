package org.apache.solr.update.processor;

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

import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.MultiPointDocValuesField;
import org.apache.solr.schema.SchemaField;

public class MultiValUpdateRequestProcessorFactory extends FieldMutatingUpdateProcessorFactory {

  @Override
  public void init(NamedList args) {
    args.add("typeClass", MultiPointDocValuesField.class.getName());
    super.init(args);
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req,
                                            SolrQueryResponse rsp,
                                            UpdateRequestProcessor next) {
    final IndexSchema schema = req.getSchema();
    return new FieldMutatingUpdateProcessor(getSelector(), next) {
      @Override
      protected SolrInputField mutate(SolrInputField src) {
        if (src.getValueCount() <= 1)
          return src;//short circuit single value
        SchemaField field = schema.getField(src.getName());
        FieldType ft = field.getType();
        IndexableField result = ft.createField(field, src, src.getBoost());
        if (result == null)
          return null;//remove
        src.setValue(result, src.getBoost());
        return src;
      }
    };
  }
}
