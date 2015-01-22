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

package org.apache.gora.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.FakeResolvingDecoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;

/**
 * PersistentDatumReader reads, fields' dirty and readable information.
 */
public class PersistentDatumReader<T extends PersistentBase>
  extends SpecificDatumReader<T> {

  private Schema rootSchema;
  private T cachedPersistent; // for creating objects

  private WeakHashMap<Decoder, ResolvingDecoder> decoderCache
    = new WeakHashMap<Decoder, ResolvingDecoder>();

  private boolean readDirtyBytes = true;

  public PersistentDatumReader() {
  }

  public PersistentDatumReader(Schema schema) {
    this(schema, true) ;
  }

  public PersistentDatumReader(Schema schema, boolean readDirtyBits) {
    this.readDirtyBytes = readDirtyBits;
    setSchema(schema);
  }

  @Override
  public void setSchema(Schema actual) {
    this.rootSchema = actual;
    super.setSchema(actual);
  }

  @SuppressWarnings("unchecked")
  public T newPersistent() {
    if(cachedPersistent == null) {
      cachedPersistent = (T) this.getData().newRecord(null, rootSchema);
      return cachedPersistent; //we can return the cached object
    }
    return (T)cachedPersistent.newInstance();
  }

  @Override
  protected Object newRecord(Object old, Schema schema) {
    if(old != null) {
      return old;
    }

    if(schema.equals(rootSchema)) {
      return newPersistent();
    } else {
      return this.getData().newRecord(old, schema);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(T reuse, Decoder in) throws IOException {
    return (T) read(reuse, rootSchema, in);
  }

  public Object read(Object reuse, Schema schema, Decoder decoder)
    throws IOException {
    return super.read(reuse, schema, getResolvingDecoder(decoder));
  }

  protected ResolvingDecoder getResolvingDecoder(Decoder decoder)
  throws IOException {
    ResolvingDecoder resolvingDecoder = decoderCache.get(decoder);
    if(resolvingDecoder == null) {
      resolvingDecoder = new FakeResolvingDecoder(rootSchema, decoder);
      decoderCache.put(decoder, resolvingDecoder);
    }
    return resolvingDecoder;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {

    Object record = newRecord(old, expected);

    T persistent = (T)record;
    persistent.clear();

    ByteBuffer dirtyBytes = null ;
    if (this.readDirtyBytes) {
      int dirtyBytesLength = in.readInt() ;
      dirtyBytes = ByteBuffer.allocate(dirtyBytesLength) ;
      in.readFixed(dirtyBytes.array()) ;
    }

    //since ResolvingDecoder.readFieldOrder is final, we cannot override it
    //so this is a copy of super.readReacord, with the readFieldOrder change

    for (Field f : expected.getFields()) {
      int pos = f.pos();
      Object oldDatum = (old != null) ? ((PersistentBase)record).get(pos) : null;
      ((PersistentBase)record).put(pos, read(oldDatum, f.schema(), in));
    }

    if (this.readDirtyBytes) {
      persistent.setDirtyBytes(dirtyBytes) ;
    }
    
    return record;
  }
  
  public Persistent clone(Persistent persistent, Schema schema) {
    Persistent cloned = (PersistentBase)persistent.newInstance();
    List<Field> fields = schema.getFields();
    for(Field field: fields) {
      int pos = field.pos();
      switch(field.schema().getType()) {
        case MAP    :
        case ARRAY  :
        case RECORD : 
        case STRING : ((PersistentBase)cloned).put(pos, cloneObject(
            field.schema(), ((PersistentBase)persistent).get(pos), ((PersistentBase)cloned).get(pos))); break;
        case NULL   : break;
        default     : ((PersistentBase)cloned).put(pos, ((PersistentBase)persistent).get(pos)); break;
      }
    }
    
    return cloned;
  }
  
  @SuppressWarnings("unchecked")
  protected Object cloneObject(Schema schema, Object toClone, Object cloned) {
    if(toClone == null) {
      return null;
    }
    
    switch(schema.getType()) {
      case MAP    :
        Map<CharSequence, Object> map = (Map<CharSequence, Object>)newMap(cloned, 0);
        for(Map.Entry<CharSequence, Object> entry: ((Map<CharSequence, Object>)toClone).entrySet()) {
          map.put((CharSequence)createString(entry.getKey().toString())
              , cloneObject(schema.getValueType(), entry.getValue(), null));
        }
        return map;
      case ARRAY  :
        List<Object> array = (List<Object>) new ArrayList<Object>();
        for(Object element: (List<Object>)toClone) {
          array.add(cloneObject(schema.getElementType(), element, null));
        }
        return array;
      case RECORD : return clone((Persistent)toClone, schema);
      case STRING : return createString(toClone.toString());
      default     : return toClone; //shallow copy is enough
    }
  }
}
