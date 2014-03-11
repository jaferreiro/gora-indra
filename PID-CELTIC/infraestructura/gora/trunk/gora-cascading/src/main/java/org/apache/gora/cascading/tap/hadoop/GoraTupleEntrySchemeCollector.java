package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

public class GoraTupleEntrySchemeCollector extends TupleEntrySchemeCollector {

    /** Field conf */
    private final JobConf conf;
    /** Field writer */
    private RecordWriter writer;
    /** Field flowProcess */
    private final FlowProcess<JobConf> hadoopFlowProcess;
    /** Field tap */
    private final Tap<JobConf, RecordReader, OutputCollector> tap;
    /** Field reporter */
    private final Reporter reporter = Reporter.NULL;

    /**
     * Constructor TapCollector creates a new TapCollector instance.
     *
     * @param flowProcess
     * @param tap
     * of type Tap
     * @throws IOException
     * when fails to initialize
     */
    public GoraTupleEntrySchemeCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) throws IOException {
            super(flowProcess, tap.getScheme());
            this.hadoopFlowProcess = flowProcess;
            this.tap = tap;
            this.conf = new JobConf(flowProcess.getConfigCopy());
            this.setOutput(this);
    }

    @Override
    public void prepare() {
            try {
                    initialize();
            } catch (IOException e) {
                    throw new RuntimeException(e);
            }

            super.prepare();
    }

    private void initialize() throws IOException {
            tap.sinkConfInit(hadoopFlowProcess, conf);
            OutputFormat outputFormat = conf.getOutputFormat();
            writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(),
                            Reporter.NULL);
            sinkCall.setOutput(this);
    }

    @Override
    public void close() {
            try {
                    writer.close(reporter);
            } catch (IOException exception) {
                    throw new TapException("exception closing HBaseTapCollector", exception);
            } finally {
                    super.close();
            }
    }

    /**
     * Method collect writes the given values to the {@link Tap} this instance
     * encapsulates.
     *
     * @param writableComparable
     * of type WritableComparable
     * @param writable
     * of type Writable
     * @throws IOException
     * when
     */
    public void collect(Object writableComparable, Object writable)
                    throws IOException {
            if (hadoopFlowProcess instanceof HadoopFlowProcess)
                    ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

            writer.write(writableComparable, writable);
    }
    
    
}
