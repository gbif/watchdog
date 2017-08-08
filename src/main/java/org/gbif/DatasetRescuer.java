package org.gbif;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.watchdog.config.WatchdogModule;

import java.io.IOException;
import java.text.ParseException;

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class rescues orphan datasets by downloading them from GBIF.org in DwC-A format.
 */
public class DatasetRescuer {
  private static Logger LOG = LoggerFactory.getLogger(DatasetRescuer.class);

  DownloadRequestService downloadRequestService;

  DatasetRescuer(DownloadRequestService occurrenceDownloadWsClient) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey) {
    EqualsPredicate p = new EqualsPredicate(OccurrenceSearchParameter.DATASET_KEY, datasetKey);
    DownloadRequest request = new DownloadRequest(p, "Kyle Braak", Sets.newHashSet(), true, DownloadFormat.DWCA);
    String download = downloadRequestService.create(request);
    LOG.info("Download: " + download); // e.g. 0011461-170714134226665

    // TODO
    // check download succeeded (status = success)
    // retrieve download link, DOI and license

    // retrieve dataset metadata XML file from GBIF cache, e.g. http://api.gbif.org/v1/dataset/98333cb6-6c15-4add-aa0e-b322bf1500ba/document
    // ensure license is set, otherwise use license from download
    // add up-to-date point of contact (e.g. GBIF Helpdesk or me) thereby also fulfilling minimum requirement
    // add up-to-date creator (e.g. me) thereby also fulfilling minimum requirement
    // add up-to-date metadata provider (e.g. me) thereby also fulfilling minimum requirement
    // add external link to GBIF download (DwC-A format) that was used to rescue dataset - this must be preserved forever
    // add me with role processor to associated parties
    // ensure citation is set, otherwise take default citation from download.xml file
    // wipe resource logo, to avoid calling broken links

    // retrieve verbatim.txt file

    // retrieve meta.xml file
    // update static mapping for license, derived from dataset metadata XML file or download metadata.xml file

    // upload to IPT
    // ensure publishing organisation set (prerequisite being the organisation must be added to the IPT before it can be loaded)
    // ensure auto-generation of citation turned on
    // make its visibility public by default
    // ensure resource metadata validates
  }

  public static void main(String[] args) throws ParseException, IOException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    DatasetRescuer rescuer = new DatasetRescuer(injector.getInstance(DownloadRequestService.class));
    rescuer.rescue("98333cb6-6c15-4add-aa0e-b322bf1500ba");
  }
}
