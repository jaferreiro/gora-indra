/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.mock.persistency;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.Tombstone;
import org.apache.gora.persistency.impl.PersistentBase;

public class MockPersistent extends PersistentBase {

  public static final String FOO = "foo";
  public static final String BAZ = "baz";
  
  public static final String[] _ALL_FIELDS = {FOO, BAZ};

  private static final Map<String, Integer> FIELD2INDEX_MAP ;
  private static final Map<Integer, String> INDEX2FIELD_MAP ;

  static {
      Map<String,Integer> field2Index = new HashMap<String,Integer>(6) ;
              field2Index.put("foo", 0) ;
              field2Index.put("baz", 1) ;
              FIELD2INDEX_MAP = Collections.unmodifiableMap(field2Index) ;
      
      Map<Integer, String> index2Field = new HashMap<Integer,String>(6) ;
              index2Field.put(0, "foo") ;
              index2Field.put(1, "baz") ;
              INDEX2FIELD_MAP = Collections.unmodifiableMap(index2Field) ;
    }
  
  /**
   * Gets the total field count.
   */
  public int getFieldsCount() {
    return MockPersistent._ALL_FIELDS.length;
  }

  private int foo;
  private int baz;
  
  public MockPersistent() {
  }
  
  @Override
  public Map<String, Integer> getField2IndexMapping() {
    return FIELD2INDEX_MAP ;
  }

  @Override
  public Map<Integer, String> getIndex2FieldMapping() {
    return INDEX2FIELD_MAP ;
  }
  
  @Override
  public Object get(int field) {
    switch(field) {
      case 0: return foo;
      case 1: return baz;
    }
    return null;
  }

  @Override
  public void put(int field, Object value) {
    switch(field) {
      case 0:  foo = (Integer)value;
      case 1:  baz = (Integer)value;
    }
  }

  @Override
  public Schema getSchema() {
    Schema.Parser parser = new Schema.Parser();
    return parser.parse("{\"type\":\"record\",\"name\":\"MockPersistent\",\"namespace\":\"org.apache.gora.mock.persistency\",\"fields\":[{\"name\":\"foo\",\"type\":\"int\"},{\"name\":\"baz\",\"type\":\"int\"}]}");
  }
  
  public void setFoo(int foo) {
    this.foo = foo;
  }
  
  public void setBaz(int baz) {
    this.baz = baz;
  }
  
  public int getFoo() {
    return foo;
  }
  
  public int getBaz() {
    return baz;
  }

  @Override
  public Tombstone getTombstone() {
    return new Tombstone(){};
  }

  @Override
  public Persistent newInstance() {
    return new MockPersistent();
  }

}
