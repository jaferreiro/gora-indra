package org.apache.gora.pig;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.gora.examples.generated.Metadata;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoraStorageTest {

  /** The configuration */
  protected static Configuration            configuration;

  private static HBaseTestingUtility        utility;
  private static PigServer                  pigServer;
  private static DataStore<String, WebPage> dataStore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
    Configuration localExecutionConfiguration = new Configuration();
    localExecutionConfiguration.setStrings("hadoop.log.dir", localExecutionConfiguration.get("hadoop.tmp.dir"));
    utility = new HBaseTestingUtility(localExecutionConfiguration);
    utility.startMiniCluster(1);
    utility.startMiniMapReduceCluster(1);
    configuration = utility.getConfiguration();

    configuration.writeXml(new FileOutputStream("target/test-classes/core-site.xml"));

    Properties props = new Properties();
    props.setProperty("fs.default.name", configuration.get("fs.default.name"));
    props.setProperty("mapred.job.tracker", configuration.get("mapred.job.tracker"));
    pigServer = new PigServer(ExecType.MAPREDUCE, props);

    dataStore = DataStoreFactory.getDataStore(String.class, WebPage.class, configuration);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pigServer.shutdown();
    utility.shutdownMiniMapReduceCluster();
  }

  @Before
	public void setUp() throws Exception {
	  WebPage w = dataStore.getBeanFactory().newPersistent() ;
	  Metadata m ;
	  
	  w.setUrl("http://gora.apache.org") ;
	  w.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
	  w.addToParsedContent("elemento1") ;
    w.addToParsedContent("elemento2") ;
    w.putToOutlinks("k1", "v1") ;
    m = new Metadata() ;
    m.setVersion(3) ;
    w.setMetadata(m) ;
    
    dataStore.put("key1", w) ;
    
    w.clear() ;
    w.setUrl("http://www.google.com") ;
    w.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    w.addToParsedContent("elemento7") ;
    w.addToParsedContent("elemento15") ;
    w.putToOutlinks("k7", "v7") ;
    m = new Metadata() ;
    m.setVersion(7) ;
    w.setMetadata(m) ;
    
    dataStore.put("key7", w) ;
    dataStore.flush() ;
	}

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testLoadSubset() {

  }

  @Test
  public void testLoadAllFields() throws IOException {
    FileSystem fs = FileSystem.get(configuration);

    pigServer.setJobName("gora-pig test - load all fields");
    pigServer.registerJar("target/gora-pig-0.4-indra-SNAPSHOT.jar");
    pigServer.registerQuery("paginas = LOAD '.' using org.apache.gora.pig.GoraStorage (" +
    		"'java.lang.String'," +
    		"'org.apache.gora.examples.generated.WebPage'," +
    		"'*') ;",1);
    pigServer.registerQuery("resultado = FOREACH paginas GENERATE key, UPPER(url) as url, content, outlinks," +
    		"                                   {()} as parsedContent:{(chararray)}, () as metadata:(version:int, data:map[]) ;",2);
    pigServer.registerQuery("STORE resultado INTO '.' using org.apache.gora.pig.GoraStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'*') ;",3);
    
    WebPage webpageUpper = dataStore.get("key1") ;
    Assert.assertEquals("HTTP://GORA.APACHE.ORG", webpageUpper.getUrl().toString()) ;
    webpageUpper = dataStore.get("key2") ;
    Assert.assertEquals("HTTP://WWW.GOOGLE.COM", webpageUpper.getUrl().toString()) ;
  }

}
 