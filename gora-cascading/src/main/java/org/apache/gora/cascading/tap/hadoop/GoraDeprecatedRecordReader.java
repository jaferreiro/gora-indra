package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;

/**
 * {@link GoraRecordReader} subclass to use in Cascading.
 * 
 * This class implements {@link MapredInputFormatCompatible} to be used in Cascading.
 * Internally holds an instance for the key and the value that will hold the references to
 * the objects to copy the results to.
 *  
 * @see GoraDeprecatedRecordReader
 * @see MapredInputFormatCompatible
 */
public class GoraDeprecatedRecordReader<K, T extends PersistentBase>
    extends GoraRecordReader<K, T>
    implements MapredInputFormatCompatible<K, T>
{

    K key ;
    T value ;
    
    public GoraDeprecatedRecordReader(Query<K, T> query, TaskAttemptContext context) {
        super(query, context);
    }

    @Override
    public void setKeyValue(K key, T value) {
        this.key = key ;
        this.value = value ;
    }
    
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return key = result.getKey();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
      return (T) this.value.copyFrom(result.get());
    }


    
    
}
