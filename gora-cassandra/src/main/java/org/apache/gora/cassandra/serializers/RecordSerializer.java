package org.apache.gora.cassandra.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.Serializer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.gora.persistency.impl.BeanFactoryImpl;
import org.apache.gora.persistency.impl.PersistentBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordSerializer extends AbstractSerializer<PersistentBase> {

  public static final Logger log = LoggerFactory.getLogger(RecordSerializer.class);
  
  /** Map: schema hash -> AbstractSerializer */
  private static Map<Integer,RecordSerializer> instancesMap = new HashMap<Integer,RecordSerializer>();

  /** Schema that will be used to serialize/deserialize */
  protected Schema schema ;
  
  public static RecordSerializer get(Schema schema) {
    RecordSerializer serializer = RecordSerializer.instancesMap.get(schema.hashCode()) ;
    if (serializer == null) {
      serializer = new RecordSerializer(schema) ;
      RecordSerializer.instancesMap.put(schema.hashCode(), serializer) ;
    }
    return serializer;
  }
  
  private RecordSerializer(Schema schema) {
    this.schema = schema ;
  }
  
  @Override
  /**
   * Serializes a ByteBuffer using the schema from {@link #get(Schema)}  
   */
  public ByteBuffer toByteBuffer(PersistentBase persistentObject) {
    ByteBufferOutputStream out = new ByteBufferOutputStream();
	  
    // We will use constructor schema, and not the one of the parameter instance
	  for (Field field : this.schema.getFields()) {
	    Schema fieldSchema = field.schema() ;
	    Serializer<Object> serializer = GoraSerializerTypeInferer.getSerializer(fieldSchema) ;
	    out.write(serializer.toByteBuffer(persistentObject.get(field.pos()))) ;
    }
	  try {
      out.close() ;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out.getByteBuffer() ;
  }

  @Override
  public PersistentBase fromByteBuffer(ByteBuffer byteBuffer) {
    
    PersistentBase newRecord;
    try {
      newRecord = BeanFactoryImpl.newPersistent(this.schema);
    } catch (Exception e) {
      log.warn("Error creating an instance of {}", schema.getFullName(), e) ;
      return null ;
    }

    // We will use constructor schema, and not the one of the parameter instance
    for (Field field : this.schema.getFields()) {
      Schema fieldSchema = field.schema() ;
      Serializer<Object> serializer = GoraSerializerTypeInferer.getSerializer(fieldSchema) ;
      newRecord.put(field.pos(), serializer.fromByteBuffer(byteBuffer)) ;
    }
    
    return newRecord ;
    
  }

}
