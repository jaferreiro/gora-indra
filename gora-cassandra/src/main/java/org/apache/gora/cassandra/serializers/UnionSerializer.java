package org.apache.gora.cassandra.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.utils.ByteBufferOutputStream;
import me.prettyprint.hector.api.Serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class UnionSerializer extends AbstractSerializer<Object> {

  /** Map: schema hash -> AbstractSerializer */
  private static Map<Integer,UnionSerializer> instancesMap = new HashMap<Integer,UnionSerializer>();

  /** Schema that will be used to serialize/deserialize */
  protected Schema schema ;
  
  public static UnionSerializer get(Schema schema) {
    UnionSerializer serializer = UnionSerializer.instancesMap.get(schema.hashCode()) ;
    if (serializer == null) {
      serializer = new UnionSerializer(schema) ;
      UnionSerializer.instancesMap.put(schema.hashCode(), serializer) ;
    }
    return serializer;
  }
  
  private UnionSerializer(Schema schema) {
    this.schema = schema ;
  }
  
  @Override
  public ByteBuffer toByteBuffer(Object unionValue) {

    ByteBufferOutputStream unionStream = new ByteBufferOutputStream();

    // Write index
    int index = GenericData.get().resolveUnion(schema, unionValue);
    unionStream.write(IntegerSerializer.get().toByteBuffer(index));

    // Write data
    Schema valueSchema = this.schema.getTypes().get(index);
    Serializer<Object> valueSerializer = GoraSerializerTypeInferer.getSerializer(valueSchema);
    unionStream.write(valueSerializer.toByteBuffer(unionValue));

    try {
      unionStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return unionStream.getByteBuffer();
  }

  @Override
  public Object fromByteBuffer(ByteBuffer byteBuffer) {
    int index = IntegerSerializer.get().fromByteBuffer(byteBuffer) ;
    Schema valueSchema = this.schema.getTypes().get(index) ;
    Serializer<Object> valueSerializer = GoraSerializerTypeInferer.getSerializer(valueSchema) ;
    return valueSerializer.fromByteBuffer(byteBuffer) ;
  }

}
