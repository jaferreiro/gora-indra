package org.apache.gora.cassandra.serializers;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

/**
 * Creates a serializer that writes nothing and reads nothing
 * @author indra
 *
 */
public class NullSerializer extends AbstractSerializer<Object> {

  private static NullSerializer instance ;
  
  public static NullSerializer get() {
    if (NullSerializer.instance == null) {
      NullSerializer.instance = new NullSerializer() ;
    }
    return NullSerializer.instance;
  }
  
  private NullSerializer() {
    //Nothing
  }
  
  /**
   * Returns and empty ByteBuffer for any input object.
   */
  @Override
  public ByteBuffer toByteBuffer(Object o) {
    return ByteBufferUtil.EMPTY_BYTE_BUFFER ;
  }

  @Override
  /**
   * Returns null for any input ByteBuffer
   */
  public Object fromByteBuffer(ByteBuffer byteBuffer) {
    return null ;
  }

}
