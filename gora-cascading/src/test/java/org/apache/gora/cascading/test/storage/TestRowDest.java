/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */

package org.apache.gora.cascading.test.storage;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class TestRowDest extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"TestRowDest\",\"namespace\":\"org.apache.gora.cascading.test.storage\",\"fields\":[{\"name\":\"defaultLong1\",\"type\":\"long\",\"default\":1},{\"name\":\"defaultStringEmpty\",\"type\":\"string\"},{\"name\":\"columnLong\",\"type\":[\"null\",\"long\"]},{\"name\":\"unionRecursive\",\"type\":[\"null\",\"TestRowDest\"]},{\"name\":\"unionString\",\"type\":[\"null\",\"string\"]},{\"name\":\"unionLong\",\"type\":[\"null\",\"long\"]},{\"name\":\"unionDefNull\",\"type\":[\"null\",\"long\"]},{\"name\":\"family2\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");
  public static enum Field {
    DEFAULT_LONG1(0,"defaultLong1"),
    DEFAULT_STRING_EMPTY(1,"defaultStringEmpty"),
    COLUMN_LONG(2,"columnLong"),
    UNION_RECURSIVE(3,"unionRecursive"),
    UNION_STRING(4,"unionString"),
    UNION_LONG(5,"unionLong"),
    UNION_DEF_NULL(6,"unionDefNull"),
    FAMILY2(7,"family2"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"defaultLong1","defaultStringEmpty","columnLong","unionRecursive","unionString","unionLong","unionDefNull","family2",};
  static {
    PersistentBase.registerFields(TestRowDest.class, _ALL_FIELDS);
  }
  private long defaultLong1;
  private Utf8 defaultStringEmpty;
  private Long columnLong;
  private TestRowDest unionRecursive;
  private Utf8 unionString;
  private Long unionLong;
  private Long unionDefNull;
  private Map<Utf8,Utf8> family2;
  public TestRowDest() {
    this(new StateManagerImpl());
  }
  public TestRowDest(StateManager stateManager) {
    super(stateManager);
    family2 = new StatefulHashMap<Utf8,Utf8>();
  }
  public TestRowDest newInstance(StateManager stateManager) {
    return new TestRowDest(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return defaultLong1;
    case 1: return defaultStringEmpty;
    case 2: return columnLong;
    case 3: return unionRecursive;
    case 4: return unionString;
    case 5: return unionLong;
    case 6: return unionDefNull;
    case 7: return family2;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:defaultLong1 = (Long)_value; break;
    case 1:defaultStringEmpty = (Utf8)_value; break;
    case 2:columnLong = (Long)_value; break;
    case 3:unionRecursive = (TestRowDest)_value; break;
    case 4:unionString = (Utf8)_value; break;
    case 5:unionLong = (Long)_value; break;
    case 6:unionDefNull = (Long)_value; break;
    case 7:family2 = (Map<Utf8,Utf8>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public long getDefaultLong1() {
    return (Long) get(0);
  }
  public void setDefaultLong1(long value) {
    put(0, value);
  }
  public Utf8 getDefaultStringEmpty() {
    return (Utf8) get(1);
  }
  public void setDefaultStringEmpty(Utf8 value) {
    put(1, value);
  }
  public void setDefaultStringEmpty(String value) {
    Utf8 valueUtf8 = (value==null)? null : new Utf8(value) ; 
    setDefaultStringEmpty(valueUtf8);
  }
  public Long getColumnLong() {
    return (Long) get(2);
  }
  public void setColumnLong(Long value) {
    put(2, value);
  }
  public TestRowDest getUnionRecursive() {
    return (TestRowDest) get(3);
  }
  public void setUnionRecursive(TestRowDest value) {
    put(3, value);
  }
  public Utf8 getUnionString() {
    return (Utf8) get(4);
  }
  public void setUnionString(Utf8 value) {
    put(4, value);
  }
  public void setUnionString(String value) {
    Utf8 valueUtf8 = (value==null)? null : new Utf8(value) ; 
    setUnionString(valueUtf8);
  }
  public Long getUnionLong() {
    return (Long) get(5);
  }
  public void setUnionLong(Long value) {
    put(5, value);
  }
  public Long getUnionDefNull() {
    return (Long) get(6);
  }
  public void setUnionDefNull(Long value) {
    put(6, value);
  }
  public Map<Utf8, Utf8> getFamily2() {
    return (Map<Utf8, Utf8>) get(7);
  }
  public Utf8 getFromFamily2(Utf8 key) {
    if (family2 == null) { return null; }
    return family2.get(key);
  }
  public void putToFamily2(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 7);
    family2.put(key, value);
  }
  public Utf8 removeFromFamily2(Utf8 key) {
    if (family2 == null) { return null; }
    getStateManager().setDirty(this, 7);
    return family2.remove(key);
  }
  public Utf8 getFromFamily2(String key) {
    Utf8 keyUtf8 = (key==null)? null : new Utf8(key) ; 
    return getFromFamily2(keyUtf8);
  }
  public void putToFamily2(String key, Utf8 value) {
    Utf8 keyUtf8 = (key==null)? null : new Utf8(key) ; 
    putToFamily2(keyUtf8, value);
  }
  public void putToFamily2(Utf8 key, String value) {
    Utf8 valueUtf8 = (value==null)? null : new Utf8(value) ; 
    putToFamily2(key, valueUtf8);
  }
  public void putToFamily2(String key, String value) {
    Utf8 keyUtf8 = (key==null)? null : new Utf8(key) ; 
    Utf8 valueUtf8 = (value==null)? null : new Utf8(value) ; 
    putToFamily2(keyUtf8, valueUtf8);
  }
  public Utf8 removeFromFamily2(String key) {
    Utf8 keyUtf8 = (key==null)? null : new Utf8(key) ; 
    return removeFromFamily2(keyUtf8);
  }
}
