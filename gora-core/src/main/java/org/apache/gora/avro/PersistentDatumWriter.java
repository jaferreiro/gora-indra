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

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.gora.persistency.impl.PersistentBase;

/**
 * PersistentDatumWriter writes, fields' dirty and readable information.
 */
public class PersistentDatumWriter<T extends PersistentBase>
  extends SpecificDatumWriter<T> {

  private T persistent = null;

  private boolean writeDirtyBytes = true;

  public PersistentDatumWriter() {
  }

  public PersistentDatumWriter(Schema schema) {
    this(schema, true) ;
  }
  
  public PersistentDatumWriter(Schema schema, boolean writeDirtyBits) {
    setSchema(schema);
    this.writeDirtyBytes = writeDirtyBits;
  }

  public void setPersistent(T persistent) {
    this.persistent = persistent;
  }

  @Override
  /**exposing this function so that fields can be written individually*/
  public void write(Schema schema, Object datum, Encoder out)
      throws IOException {
    super.write(schema, datum, out);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void writeRecord(Schema schema, Object datum, Encoder out)
      throws IOException {

    if(persistent == null) {
      persistent = (T) datum;
    }

    if (writeDirtyBytes) {
      //write readable fields and dirty fields info
      ByteBuffer dirtyBytes = persistent.getDirtyBytes() ;
      out.writeInt(dirtyBytes.capacity()) ;
      out.writeFixed(dirtyBytes) ;
    }
    super.writeRecord(schema, datum, out);
  }

}
