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

/**
 * Class rescues orphan datasets by downloading them from GBIF.org in DwC-A format.
 */
public class DatasetRescuer {

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
    downloadRequestService.create(request);
  }

  public static void main(String[] args) throws ParseException, IOException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    DatasetRescuer rescuer = new DatasetRescuer(injector.getInstance(DownloadRequestService.class));
    rescuer.rescue("98333cb6-6c15-4add-aa0e-b322bf1500ba");
  }
}
