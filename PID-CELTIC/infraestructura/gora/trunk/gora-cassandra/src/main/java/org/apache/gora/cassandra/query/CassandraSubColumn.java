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

package org.apache.gora.cassandra.query;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.serializers.GenericArraySerializer;
import org.apache.gora.cassandra.serializers.GoraSerializerTypeInferer;
import org.apache.gora.cassandra.serializers.StatefulHashMapSerializer;
import org.apache.gora.cassandra.serializers.TypeUtils;
import org.apache.gora.cassandra.store.CassandraStore;
import org.apache.gora.persistency.StatefulHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSubColumn extends CassandraColumn {
  public static final Logger LOG = LoggerFactory.getLogger(CassandraSubColumn.class);

  private static final String ENCODING = "UTF-8";
  
  private static CharsetEncoder charsetEncoder = Charset.forName(ENCODING).newEncoder();;


  /**
   * Key-value pair containing the raw data.
   */
  private HColumn<ByteBuffer, ByteBuffer> hColumn;

  public ByteBuffer getName() {
    return hColumn.getName();
  }

  /**
   * Deserialize a String into an typed Object, according to the field schema.
   * @see org.apache.gora.cassandra.query.CassandraColumn#getValue()
   */
  public Object getValue() {
    Field field = getField();
    Schema fieldSchema = field.schema();
    ByteBuffer byteBuffer = hColumn.getValue();
    if (byteBuffer == null) {
      return null;
    }

    // XXX TOP LEVEL UNION
    // If schema is an optional field (this is ["null","type"]), consider it as ["type"]        
    if (fieldSchema.getType().equals(Type.UNION)) {
      if (fieldSchema.getTypes().size() == 2) {
        
        // schema [type0, type1]
        Type type0 = fieldSchema.getTypes().get(0).getType() ;
        Type type1 = fieldSchema.getTypes().get(1).getType() ;
        
        // Check if types are different and there's a "null", like ["null","type"] or ["type","null"]
        if (!type0.equals(type1) && type0.equals(Schema.Type.NULL)) {
          // Schema 0 = [null], so schema will be 1.
          fieldSchema = fieldSchema.getTypes().get(1) ;
        } else if (!type0.equals(type1) && type1.equals(Schema.Type.NULL)) {
          // Schema 1 = [null], so schema will be 0.
          fieldSchema = fieldSchema.getTypes().get(0) ;
        }
      }
    }

    return fromByteBuffer(fieldSchema, byteBuffer) ;
  }

  public void setValue(HColumn<ByteBuffer, ByteBuffer> hColumn) {
    this.hColumn = hColumn;
  }
}
