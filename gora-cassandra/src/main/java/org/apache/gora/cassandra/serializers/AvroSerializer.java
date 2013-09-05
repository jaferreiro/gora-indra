package org.apache.gora.cassandra.serializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.gora.avro.PersistentDatumReader;
import org.apache.gora.avro.PersistentDatumWriter;
import org.apache.gora.persistency.impl.PersistentBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSerializer extends AbstractSerializer<Object> {

  public static final Logger log = LoggerFactory.getLogger(AvroSerializer.class);
  
  /** Map: schema hash -> AbstractSerializer */
  private static Map<Integer,AvroSerializer> instancesMap = new HashMap<Integer,AvroSerializer>();

  /** Schema that will be used to serialize/deserialize */
  protected Schema schema ;

  // encoder & decoder. IMPORTANTE: Access with getEncoder() & getDecoder()!!
  private BinaryEncoderWithStream  avroEncoder = null ;
  private BinaryDecoder            avroDecoder = null ;
  private PersistentDatumWriter<?> writer      = null ;
  private PersistentDatumReader<?> reader      = null ;
  
  protected BinaryEncoderWithStream getEncoder() {
    if (this.avroEncoder == null) this.avroEncoder = new BinaryEncoderWithStream(new ByteArrayOutputStream()) ;
    return this.avroEncoder ;
  }

  protected BinaryDecoder getNewDecoder(InputStream inputBytesStream) {
    return DecoderFactory.defaultFactory().createBinaryDecoder(inputBytesStream, this.avroDecoder) ;
  }
  
  protected BinaryDecoder getNewDecoder(byte[] dataBytes) {
    return DecoderFactory.defaultFactory().createBinaryDecoder(dataBytes, this.avroDecoder) ;
  }
  
  @SuppressWarnings("rawtypes")
  protected PersistentDatumWriter getDatumWriter() {
    if (this.writer == null) this.writer = new PersistentDatumWriter(this.schema,false) ;
    return this.writer ;
  }

  @SuppressWarnings("rawtypes")
  protected PersistentDatumReader getDatumReader() {
    if (this.reader == null) this.reader = new PersistentDatumReader(this.schema,false) ;
    return this.reader ;
  }
  
  /**
   * A BinaryEncoder that exposes the outputstream so that it can be reset
   * every time. (This is a workaround to reuse BinaryEncoder and the buffers,
   * normally provided be EncoderFactory, but this class does not exist yet 
   * in the current Avro version).
   */
  public static final class BinaryEncoderWithStream extends BinaryEncoder {
    public BinaryEncoderWithStream(OutputStream out) {
      super(out);
    }
    
    protected OutputStream getOut() {
      return out;
    }
  }
  
  public static AvroSerializer get(Schema schema) {
    AvroSerializer serializer = AvroSerializer.instancesMap.get(schema.hashCode()) ;
    if (serializer == null) {
      serializer = new AvroSerializer(schema) ;
      AvroSerializer.instancesMap.put(schema.hashCode(), serializer) ;
    }
    return serializer;
  }
  
  private AvroSerializer(Schema schema) {
    this.schema = schema ;
  }
  
  @Override
  /**
   * Serializes a ByteBuffer using the schema from {@link #get(Schema)}  
   */
  public ByteBuffer toByteBuffer(Object persistentObject) {
    // Reset the buffer
    ByteArrayOutputStream os = (ByteArrayOutputStream) this.getEncoder().getOut() ;
    os.reset() ;

    PersistentDatumWriter<?> writer = this.getDatumWriter() ;
    try {
      writer.write(this.schema, persistentObject, this.getEncoder()) ;
      this.getEncoder().flush() ;
    } catch (IOException e) {
      log.warn("Error serializing.", e) ;
      return null ;
    }
    return ByteBuffer.wrap(os.toByteArray()) ;
  }

  @Override
  public Object fromByteBuffer(ByteBuffer byteBuffer) {

    PersistentDatumReader<?> reader = this.getDatumReader() ;
    List<ByteBuffer> byteBuffers = new ArrayList<ByteBuffer>() ;
    byteBuffers.add(byteBuffer) ;
    BinaryDecoder decoder = this.getNewDecoder(new ByteBufferInputStream(byteBuffers)) ;
    try {
      return reader.read((Object) null, this.schema, decoder);
    } catch (IOException e) {
      log.warn("Error deserializing.", e) ;
      return null ;
    }
    
  }

}
