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

package org.apache.gora.cassandra.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.query.CassandraQuery;
import org.apache.gora.cassandra.query.CassandraResult;
import org.apache.gora.cassandra.query.CassandraResultSet;
import org.apache.gora.cassandra.query.CassandraRow;
import org.apache.gora.cassandra.query.CassandraSubColumn;
import org.apache.gora.cassandra.query.CassandraSuperColumn;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.impl.DataStoreBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {
  public static final Logger LOG = LoggerFactory.getLogger(CassandraStore.class);

  private CassandraClient<K, T>  cassandraClient = new CassandraClient<K, T>();

  /**
   * The values are Avro fields pending to be stored.
   *
   * We want to iterate over the keys in insertion order.
   * We don't want to lock the entire collection before iterating over the keys, since in the meantime other threads are adding entries to the map.
   */
  private Map<K, T> buffer = Collections.synchronizedMap(new LinkedHashMap<K, T>());
  
  public CassandraStore() throws Exception {
    // this.cassandraClient.initialize();
  }

  public void initialize(Class<K> keyClass, Class<T> persistent, Properties properties) {
    try {
      super.initialize(keyClass, persistent, properties);
      this.cassandraClient.initialize(keyClass, persistent);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.error(e.getStackTrace().toString());
    }
  }

  @Override
  public void close() {
    LOG.debug("close");
    flush();
  }

  @Override
  public void createSchema() {
    LOG.debug("creating Cassandra keyspace");
    this.cassandraClient.checkKeyspace();
  }

  @Override
  public boolean delete(K key) {
    LOG.debug("delete " + key);
    return false;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) {
    LOG.debug("delete by query " + query);
    return 0;
  }

  @Override
  public void deleteSchema() {
    LOG.debug("delete schema");
    this.cassandraClient.dropKeyspace();
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) {
    
    Map<String, List<String>> familyMap = this.cassandraClient.getFamilyMap(query);
    Map<String, String> reverseMap = this.cassandraClient.getReverseMap(query);
    
    CassandraQuery<K, T> cassandraQuery = new CassandraQuery<K, T>();
    cassandraQuery.setQuery(query);
    cassandraQuery.setFamilyMap(familyMap);
    
    CassandraResult<K, T> cassandraResult = new CassandraResult<K, T>(this, query);
    cassandraResult.setReverseMap(reverseMap);

    CassandraResultSet<K> cassandraResultSet = new CassandraResultSet<K>();
    
    // We query Cassandra keyspace by families.
    for (String family : familyMap.keySet()) {
      if (family == null) {
        continue;
      }
      if (this.cassandraClient.isSuper(family)) {
        addSuperColumns(family, cassandraQuery, cassandraResultSet);
         
      } else {
        addSubColumns(family, cassandraQuery, cassandraResultSet);
      }
    }
    
    cassandraResult.setResultSet(cassandraResultSet);
    
    return cassandraResult;
  }

  private void addSubColumns(String family, CassandraQuery<K, T> cassandraQuery,
      CassandraResultSet cassandraResultSet) {
    // select family columns that are included in the query
    List<Row<K, ByteBuffer, ByteBuffer>> rows = this.cassandraClient.execute(cassandraQuery, family);
    
    for (Row<K, ByteBuffer, ByteBuffer> row : rows) {
      K key = row.getKey();
      
      // find associated row in the resultset
      CassandraRow<K> cassandraRow = cassandraResultSet.getRow(key);
      if (cassandraRow == null) {
        cassandraRow = new CassandraRow<K>();
        cassandraResultSet.putRow(key, cassandraRow);
        cassandraRow.setKey(key);
      }
      
      ColumnSlice<ByteBuffer, ByteBuffer> columnSlice = row.getColumnSlice();
      
      for (HColumn<ByteBuffer, ByteBuffer> hColumn : columnSlice.getColumns()) {
        CassandraSubColumn cassandraSubColumn = new CassandraSubColumn();
        cassandraSubColumn.setValue(hColumn);
        cassandraSubColumn.setFamily(family);
        cassandraRow.add(cassandraSubColumn);
      }
      
    }
  }

  private void addSuperColumns(String family, CassandraQuery<K, T> cassandraQuery, 
      CassandraResultSet cassandraResultSet) {
    
    List<SuperRow<K, String, ByteBuffer, ByteBuffer>> superRows = this.cassandraClient.executeSuper(cassandraQuery, family);
    for (SuperRow<K, String, ByteBuffer, ByteBuffer> superRow: superRows) {
      K key = superRow.getKey();
      CassandraRow<K> cassandraRow = cassandraResultSet.getRow(key);
      if (cassandraRow == null) {
        cassandraRow = new CassandraRow();
        cassandraResultSet.putRow(key, cassandraRow);
        cassandraRow.setKey(key);
      }
      
      SuperSlice<String, ByteBuffer, ByteBuffer> superSlice = superRow.getSuperSlice();
      for (HSuperColumn<String, ByteBuffer, ByteBuffer> hSuperColumn: superSlice.getSuperColumns()) {
        CassandraSuperColumn cassandraSuperColumn = new CassandraSuperColumn();
        cassandraSuperColumn.setValue(hSuperColumn);
        cassandraSuperColumn.setFamily(family);
        cassandraRow.add(cassandraSuperColumn);
      }
    }
  }

  /**
   * Flush the buffer. Write the buffered rows.
   * @see org.apache.gora.store.DataStore#flush()
   */
  @Override
  public void flush() {
    
    Set<K> keys = this.buffer.keySet();
    
    // this duplicates memory footprint
    K[] keyArray = (K[]) keys.toArray();
    
    // iterating over the key set directly would throw ConcurrentModificationException with java.util.HashMap and subclasses
    for (K key: keyArray) {
      T value = this.buffer.get(key);
      if (value == null) {
        LOG.info("Value to update is null for key " + key);
        continue;
      }
      Schema schema = value.getSchema();
      for (Field field: schema.getFields()) {
        if (value.isDirty(field.pos())) {
          addOrUpdateField(key, field, value.get(field.pos()));
        }
      }
    }
    
    // remove flushed rows
    for (K key: keyArray) {
      this.buffer.remove(key);
    }
  }

  @Override
  public T get(K key, String[] fields) {
    CassandraQuery<K,T> query = new CassandraQuery<K,T>();
    query.setDataStore(this);
    query.setKeyRange(key, key);
    query.setFields(fields);
    query.setLimit(1);
    Result<K,T> result = execute(query);
    boolean hasResult = false;
    try {
      hasResult = result.next();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return hasResult ? result.get() : null;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    // just a single partition
    List<PartitionQuery<K,T>> partitions = new ArrayList<PartitionQuery<K,T>>();
    partitions.add(new PartitionQueryImpl<K,T>(query));
    return partitions;
  }
  
  /**
   * In Cassandra Schemas are referred to as Keyspaces
   * @return Keyspace
   */
  @Override
  public String getSchemaName() {
    return this.cassandraClient.getKeyspaceName();
  }

  @Override
  public Query<K, T> newQuery() {
    Query<K,T> query = new CassandraQuery<K, T>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  /**
   * Duplicate instance to keep all the objects in memory till flushing.
   * @see org.apache.gora.store.DataStore#put(java.lang.Object, org.apache.gora.persistency.Persistent)
   */
  @Override
  public void put(K key, T value) {
    this.buffer.put(key, (T) value.clone()) ;
  }

  /**
   * Add a field to Cassandra according to its type.
   * @param key     the key of the row where the field should be added
   * @param field   the Avro field representing a datum
   * @param value   the field value
   */
  private void addOrUpdateField(K key, Field field, Object value) {
    Schema schema = field.schema();
    Type type = schema.getType();
    switch (type) {
      case STRING:
      case BOOLEAN:
      case INT:
      case LONG:
      case BYTES:
      case FLOAT:
      case DOUBLE:
      case FIXED:
        this.cassandraClient.addColumn(key, field.name(), schema, value);
        break;
      case RECORD:
        // A record is persistend in supercolumns
        // Each field
        if (value != null) {
          if (value instanceof PersistentBase) {
            PersistentBase persistentBase = (PersistentBase) value;
            for (Field member : schema.getFields()) {

              // TODO: hack, do not store empty arrays
              Object memberValue = persistentBase.get(member.pos());
              if (memberValue instanceof GenericArray<?>) {
                if (((GenericArray) memberValue).size() == 0) {
                  continue;
                }
              }

              Schema memberSchema = member.schema() ;
              
              // XXX TOP LEVEL UNION
              // If schema is an optional field (this is ["null","type"]), consider it as ["type"]
              if (memberSchema.getType().equals(Type.UNION)) {
                if (memberSchema.getTypes().size() == 2) {
  
                  // schema [type0, type1]
                  Type type0 = memberSchema.getTypes().get(0).getType();
                  Type type1 = memberSchema.getTypes().get(1).getType();
  
                  // Check if types are different and there's a "null", like
                  // ["null","type"] or ["type","null"]
                  if (!type0.equals(type1)
                      && (type0.equals(Schema.Type.NULL) || type1.equals(Schema.Type.NULL))) {
                    int index = GenericData.get().resolveUnion(memberSchema, memberValue);
                    memberSchema = memberSchema.getTypes().get(index);
                  }
                }
              }
              
              this.cassandraClient.addSubColumn(key, field.name(), member.name(), memberSchema, memberValue);
            }
          } else {
            LOG.info("Record not supported: " + value.toString());
          }
        }
        break;
      case MAP:
        if (value != null) {
          if (value instanceof StatefulHashMap<?, ?>) {
            this.cassandraClient.addStatefulHashMap(key, field.name(), schema.getValueType(), (StatefulHashMap<Utf8,Object>)value);
          } else {
            LOG.info("Map not supported: " + value.toString());
          }
        }
        break;
      case ARRAY:
        if (value != null) {
          if (value instanceof GenericArray<?>) {
            this.cassandraClient.addGenericArray(key, field.name(), schema.getElementType(),(GenericArray)value);
          } else {
            LOG.info("Array not supported: " + value.toString());
          }
        }
        break;
      case UNION:
        // XXX TOP LEVEL UNION
        // If schema is an optional field (this is ["null","type"]), consider it as ["type"]        
        if (schema.getTypes().size() == 2) {
          
          // schema [type0, type1]
          Type type0 = schema.getTypes().get(0).getType() ;
          Type type1 = schema.getTypes().get(1).getType() ;
          
          // Check if types are different and there's a "null", like ["null","type"] or ["type","null"]
          if (!type0.equals(type1)
              && (   type0.equals(Schema.Type.NULL)
                  || type1.equals(Schema.Type.NULL))) {

            int index = GenericData.get().resolveUnion(schema, value);
            schema = schema.getTypes().get(index) ;
            Field fakeField = new Field(field.name(), schema, field.doc(), field.defaultValue()) ;

            this.addOrUpdateField(key, fakeField, value) ;
            break ;
          }
          
        }
        // else
        //   serialize completely.
        
        this.cassandraClient.addColumn(key, field.name(), schema, value) ;
        break ;
      default:
        LOG.info("Type not considered: " + type.name());      
    }
  }

  @Override
  public boolean schemaExists() {
    LOG.info("schema exists");
    return cassandraClient.keyspaceExists();
  }

}
