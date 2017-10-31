package org.gbif;

import java.net.MalformedURLException;

import org.junit.Ignore;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for simple OrphanDatasetScanner.
 */
public class OrphanDatasetScannerTest {

  @Ignore
  public void testPingURL() throws MalformedURLException {
    assertEquals(true, OrphanDatasetScanner.pingURL("http://ipt.biologi.lipi.go.id/archive.do?r=hb-rcb"));
    assertEquals(true, OrphanDatasetScanner.pingURL("http://choreutidae.lifedesks.org/classification.tar.gz")); // redirects and is not downloadable!
    assertEquals(true, OrphanDatasetScanner.pingURL("https://nas.er.usgs.gov/ipt/archive.do?r=nas")); // 301 response code (moved permanently but downloadable)

    assertEquals(false, OrphanDatasetScanner.pingURL("http://rs.gbif.org/datasets/protected/fauna_europaea.zip"));
    assertEquals(false, OrphanDatasetScanner.pingURL("http://www.icimod.org:8080/hkh-bif/archive.do?r=toorsa_strict_nature_reserve"));
    assertEquals(false, OrphanDatasetScanner.pingURL("http://ctap.inhs.uiuc.edu/dmitriev/Export/DwCArchive_Delt.zip"));
    assertEquals(false, OrphanDatasetScanner.pingURL("http://digir.sunsite.utk.edu/digir/www/DiGIR.php"));
    assertEquals(false, OrphanDatasetScanner.pingURL("http://gnub.org/data/GNUB_DwC.zip"));
  }
}
