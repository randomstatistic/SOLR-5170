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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MultiPointFieldTest extends SolrTestCaseJ4 {

  private final String fieldName = "pointsDV";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spatial.xml", "schema-spatial.xml");
  }

  @Test
  public void test() throws Exception {
    clearIndex();
    assertU(adoc("id", "100", fieldName, "1,2"));//1 point
    assertU(adoc("id", "101", fieldName, "4,-1", fieldName, "3,5"));//2 points, 2nd is pretty close to query point
    assertU(commit());


    assertJQ(req(
        "q", "{!func}distDV("+ fieldName +",\"3,4\")",//lat,lon order
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-4
        , "/response/docs/[0]/id=='101'"
        , "/response/docs/[0]/score==0.99862987"//dist to 3,5
    );
  }

  @Test
  public void testEmptyIndex() throws Exception {
    clearIndex();
    assertU(commit());

    assertJQ(req(
        "q", "{!func}distDV("+ fieldName +",\"3,4\")",//lat,lon order
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-4
        , "/response/numFound==0"
    );
  }


  @Test @Ignore
  public void testLargeDocument() throws Exception {
    clearIndex();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "100");
    for (int i = 0; i < 4097; i++) {
      doc.addField(fieldName, "40,-90");
    }
    assertU(adoc(doc));
    assertU(commit());


    assertJQ(req(
        "q", "{!func}distDV("+ fieldName +",\"40,-90\")",//lat,lon order
        "fl","id,score",
        "sort","score asc")//want ascending due to increasing distance
        , 1e-4
        , "/response/docs/[0]/id=='100'"
    );
  }

  @Test
  public void testRecip() throws Exception {
    clearIndex();
    assertU(adoc("id", "100", fieldName, "1,2"));//1 point
    assertU(adoc("id", "101", fieldName, "4,-1", fieldName, "3,5"));//2 points, 2nd is pretty close to query point
    assertU(commit());


    assertJQ(req(
        "q", "{!func score=recipDistance v=distDV("+ fieldName +",\"3,4\")}",//lat,lon order
        "fl","id,score",
        "sort","score desc")
        , 1e-4
        , "/response/docs/[0]/id=='101'"
        , "/response/docs/[0]/score==0.9092042"
    );
  }

}
