package org.gbif;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple OrphanDatasetScanner.
 */
public class OrphanDatasetScannerTest extends TestCase {

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public OrphanDatasetScannerTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(OrphanDatasetScannerTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp() {
    assertTrue(true);
  }
}
