package org.apache.gora.cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;

/**
 * Copied from HBaseTapCollector
 */
@SuppressWarnings("rawtypes")
public class GoraTupleEntrySchemeCollector extends TupleEntrySchemeCollector implements OutputCollector<Object, Object>{

    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(GoraTupleEntrySchemeCollector.class);

    /** Field conf */
    private final JobConf conf;
    /** Field writer */
    private RecordWriter writer;
    /** Field flowProcess */
    private final FlowProcess<JobConf> hadoopFlowProcess;
    /** Field tap */
    private final GoraTap tap;
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
    @SuppressWarnings("unchecked")
    public GoraTupleEntrySchemeCollector(FlowProcess<JobConf> flowProcess, GoraTap tap) throws IOException {
            super(flowProcess, tap.getScheme(), tap.getIdentifier());
            this.hadoopFlowProcess = flowProcess;
            this.tap = tap;
            this.conf = new JobConf(flowProcess.getConfigCopy());
            this.setOutput(this);
    }

    public GoraTap getTap() {
        return tap;
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

    public static class DumbStatusReporterProgressable extends StatusReporter implements Progressable {

        // Copied from PigReporter
        private TaskInputOutputContext context;
        private static DumbStatusReporterProgressable reporter = null;
        /**
         * Get singleton instance of the context
         */
        public static DumbStatusReporterProgressable getInstance() {
            if (reporter == null) {
                reporter = new DumbStatusReporterProgressable(null);
            }
            return reporter;
        }
        
        public static void setContext(TaskInputOutputContext context) {
            reporter = new DumbStatusReporterProgressable(context);
        }
        
        private DumbStatusReporterProgressable(TaskInputOutputContext context) {
            this.context = context;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Counter getCounter(Enum<?> name) {        
            return (context == null) ? null : context.getCounter(name);
        }

        @Override
        public Counter getCounter(String group, String name) {
            return (context == null) ? null : context.getCounter(group, name);
        }

        @Override
        public void progress() {
            if (context != null) {
                context.progress();
            }
        }

        @Override
        public void setStatus(String status) {
            if (context != null) {
                context.setStatus(status);
            }
        }

        public float getProgress() {
            return 0;
        }
        
    }
    
    @SuppressWarnings("unchecked")
    private void initialize() throws IOException {
            tap.sinkConfInit(hadoopFlowProcess, conf);
            OutputFormat outputFormat = conf.getOutputFormat();
            writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(), DumbStatusReporterProgressable.getInstance());
            sinkCall.setOutput(this);
    }

    @Override
    public void close() {
            try {
                    writer.close(reporter);
            } catch (IOException exception) {
                    throw new TapException("exception closing GoraTupleEntrySchemeCollector", exception);
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
    @SuppressWarnings("unchecked")
    @Override
    public void collect(Object writableComparable, Object writable)
                    throws IOException {
            if (hadoopFlowProcess instanceof HadoopFlowProcess)
                    ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

            writer.write(writableComparable, writable);
    }
    
}
