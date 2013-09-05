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
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.serializers.Utf8Serializer;
import org.apache.gora.persistency.ListGenericArray;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.BeanFactoryImpl;
import org.apache.gora.persistency.impl.PersistentBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSuperColumn extends CassandraColumn {
  public static final Logger LOG = LoggerFactory.getLogger(CassandraSuperColumn.class);

  private HSuperColumn<String, ByteBuffer, ByteBuffer> hSuperColumn;
  
  public ByteBuffer getName() {
    return StringSerializer.get().toByteBuffer(hSuperColumn.getName());
  }

  public Object getValue() {
    Field field = getField();
    Schema fieldSchema = field.schema();
    Type type = fieldSchema.getType();
    
    switch (type) {
      case ARRAY:
        ListGenericArray<Object> array = new ListGenericArray<Object>(fieldSchema.getElementType());
        
        for (HColumn<ByteBuffer, ByteBuffer> hColumn : this.hSuperColumn.getColumns()) {
          Object memberValue = fromByteBuffer(fieldSchema.getElementType(), hColumn.getValue());
          array.add(memberValue);      
        }
        return array ;

      case MAP:
        Map<Utf8, Object> map = new StatefulHashMap<Utf8, Object>();
        
        for (HColumn<ByteBuffer, ByteBuffer> hColumn : this.hSuperColumn.getColumns()) {
          Object memberValue = fromByteBuffer(fieldSchema.getValueType(), hColumn.getValue());
          map.put(Utf8Serializer.get().fromByteBuffer(hColumn.getName()), memberValue);      
        }
        return map ;

      case UNION:
        // In supercolumns, unions MUST be ["NULL","TYPE"], since 3-types should be serialized in a column (and then will be subcolumn)
        // TODO EXCEPTION or LOG+NULL? At this point seems to be returning NULLs for invalid values. To comment at jira

        // If no column was retrieved, then is null
        if (this.hSuperColumn.getSize() == 0) {
          return null ;
        }

        // Else record. Get the proper schema
        Type type0 = fieldSchema.getTypes().get(0).getType() ;
            
        // Precondition: schema = ["null","type"] or ["type","null"]
        if (type0.equals(Schema.Type.NULL)) {
          // Schema 0 = [null], so schema will be 1.
          fieldSchema = fieldSchema.getTypes().get(1) ;
        } else {
          // Schema 1 = [null], so schema will be 0.
          fieldSchema = fieldSchema.getTypes().get(0) ;
        }
        // We continue as a record
        
      case RECORD:
        PersistentBase newRecord;
        try {
          newRecord = BeanFactoryImpl.newPersistent(fieldSchema);
        } catch (Exception e) {
          LOG.warn("Error creating an instance of {}", fieldSchema.getFullName(), e) ;
          return null ;
        }
  
        for (HColumn<ByteBuffer, ByteBuffer> hColumn : this.hSuperColumn.getColumns()) {
          String memberName = StringSerializer.get().fromByteBuffer(hColumn.getName());
          if (memberName == null || memberName.length() == 0) {
            LOG.warn("member name is null or empty for hColumn.");
            continue;
          }
          Field memberField = fieldSchema.getField(memberName);
          CassandraSubColumn cassandraColumn = new CassandraSubColumn();
          cassandraColumn.setField(memberField);
          cassandraColumn.setValue(hColumn);
          newRecord.put(newRecord.getFieldIndex(memberName), cassandraColumn.getValue());
        }
        return newRecord ;

      default:
        LOG.info("Type not supported: " + type);
        return null ;
    }

  }

  public void setValue(HSuperColumn<String, ByteBuffer, ByteBuffer> hSuperColumn) {
    this.hSuperColumn = hSuperColumn;
  }

}
