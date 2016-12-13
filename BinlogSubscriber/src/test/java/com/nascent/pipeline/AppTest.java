package com.nascent.pipeline;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.nascent.pipeline.subscriber.xmltags.EventRoot;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
    	super( testName );
        
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    public void testXml(){
    	String path = AppTest.class.getResource("/pipeline.xml").toString();
    	path=path.substring(6);
    	File xml = new File(path);
    	
    	EventRoot root = unmarshal(xml,EventRoot.class);
    	System.out.println(root.events.size());
    }
    
    public static <T> T unmarshal(File xmlFile, Class<T> clazz) {
        T result = null;
        try {
          Unmarshaller avm = JAXBContext.newInstance(clazz).createUnmarshaller();
          result = (T) avm.unmarshal(xmlFile);
        } catch (JAXBException e) {
          e.printStackTrace();
        }
        return result;
      }
}
