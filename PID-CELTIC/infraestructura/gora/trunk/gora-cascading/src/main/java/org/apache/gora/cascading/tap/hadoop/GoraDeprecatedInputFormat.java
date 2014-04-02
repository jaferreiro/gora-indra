package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.input.MapredInputFormatCompatible;

/**
 * {@link GoraInputFormat} to fetch the input for Cascading Taps.
 * 
 * The diference is that the returned RecordReader by {@link #createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)}
 * is an instance of {@link GoraDeprecatedRecordReader}, that handles correctly the needs of {@link DeprecatedInputFormatWrapper} (hacks the needs for reusing
 * key and value instances).
 * 
 * @see GoraDeprecatedRecordReader
 * @see MapredInputFormatCompatible
 */
public class GoraDeprecatedInputFormat<K, T extends PersistentBase> extends GoraInputFormat<K, T> {

    @Override
    @SuppressWarnings("unchecked")
    public RecordReader<K, T> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      PartitionQuery<K,T> partitionQuery = (PartitionQuery<K, T>)
        ((GoraInputSplit)split).getQuery();

      setInputPath(partitionQuery, context);
      return new GoraDeprecatedRecordReader<K, T>(partitionQuery, context);
    }

}
