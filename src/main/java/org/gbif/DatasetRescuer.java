package org.gbif;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.metadata.eml.Agent;
import org.gbif.metadata.eml.Eml;
import org.gbif.metadata.eml.EmlFactory;
import org.gbif.metadata.eml.EmlWriter;
import org.gbif.metadata.eml.PhysicalData;
import org.gbif.metadata.eml.UserId;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.watchdog.config.WatchdogModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.text.ParseException;
import java.util.Map;
import java.util.UUID;
import javax.xml.parsers.ParserConfigurationException;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import freemarker.cache.ClassTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.FileUtils;
import org.apache.http.StatusLine;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * Class rescues orphan datasets by downloading them from GBIF.org in DwC-A format.
 */
public class DatasetRescuer {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescuer.class);

  // 10 second timeout
  private static final int CONNECTION_TIMEOUT_MSEC = 10000;
  private static final int MAX_CONNECTIONS = 1;
  private static final int MAX_PER_ROUTE = 1;
  private static final String GBIF_DOWNLOAD_EML = "metadata.xml";
  private static final String GBIF_DOWNLOAD_VERBATIM = "verbatim.txt";

  private static final String RESCUED_EML = "eml.xml";
  private static final String RESCUED_OCCURRENCE = "occurrence.txt";
  private static final String RESCUED_META = "meta.xml";
  private static final String RESCUED_META_PATH = "/meta.xml";

  private static final String IPT_RESOURCE_TEMPLATE = "resource.ftl";
  private static final Configuration FTL = provideFreemarker();
  private static final String IPT_RESOURCE = "/resource.xml";

  DownloadRequestService downloadRequestService;
  OccurrenceDownloadService occurrenceDownloadService;
  DatasetService datasetService;

  DatasetRescuer(DownloadRequestService occurrenceDownloadWsClient, OccurrenceDownloadService occurrenceDownloadService,
    DatasetService datasetService) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.datasetService = datasetService;
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey)
    throws IOException, ParserConfigurationException, SAXException, TemplateException {
    EqualsPredicate p = new EqualsPredicate(OccurrenceSearchParameter.DATASET_KEY, datasetKey);
    DownloadRequest request = new DownloadRequest(p, "Kyle Braak", Sets.newHashSet(), true, DownloadFormat.DWCA);
    //String download = downloadRequestService.create(request);
    //LOG.info("Download: " + download); // e.g. 0011461-170714134226665

    // TODO
    // check download succeeded (status = success)
    Download downloadMetadata = occurrenceDownloadService.get("0011461-170714134226665");
    LOG.info(downloadMetadata.getStatus().name());

    // retrieve download link, DOI and license
    LOG.info(downloadMetadata.getDoi().getDoiName());
    LOG.info(downloadMetadata.getDownloadLink());
    LOG.info(String.valueOf(downloadMetadata.getTotalRecords()));

    // retrieve dataset metadata XML file from GBIF cache, e.g. http://api.gbif.org/v1/dataset/98333cb6-6c15-4add-aa0e-b322bf1500ba/document
    Eml eml = EmlFactory.build(datasetService.getMetadataDocument(UUID.fromString(datasetKey)));

    DefaultHttpClient httpClient =
      HttpUtil.newMultithreadedClient(CONNECTION_TIMEOUT_MSEC, MAX_CONNECTIONS, MAX_PER_ROUTE);
    HttpUtil httpUtil = new HttpUtil(httpClient);

    File tmpDownloadDir = Files.createTempDir();
    File tmpDwca = new File(tmpDownloadDir, "rescued-dwca-" + datasetKey + ".zip");

    StatusLine status = httpUtil.download(downloadMetadata.getDownloadLink(), tmpDwca);
    LOG.info(status.getReasonPhrase() + " Check: " + tmpDwca.getAbsolutePath());

    File tmpDecompressDir = Files.createTempDir();
    CompressionUtil.decompressFile(tmpDecompressDir, tmpDwca, true);
    LOG.info("Unzipped to: " + tmpDecompressDir.getAbsolutePath());

    // retrieve dataset metadata XML file generated by GBIF
    InputStream emlGbifIs = new FileInputStream(new File(tmpDecompressDir, GBIF_DOWNLOAD_EML));
    Eml emlGbif = EmlFactory.build(emlGbifIs);
    LOG.info(emlGbif.getPhysicalData().toString());

    // ensure license is set, otherwise use license from download
    if (eml.parseLicenseUrl() == null && emlGbif.getIntellectualRights() != null) {
      eml.setIntellectualRights(emlGbif.getIntellectualRights());
    }

    Agent rescuer = new Agent();
    rescuer.setFirstName("Kyle");
    rescuer.setLastName("Braak");
    rescuer.setEmail("helpdesk@gbif.org");
    rescuer.setOrganisation("GBIFS");
    rescuer.setRole("processor");
    rescuer.addUserId(new UserId("http://orcid.org/", "0000-0002-3696-3496"));

    // add up-to-date point of contact (e.g. GBIF Helpdesk or me) thereby also fulfilling minimum requirement
    eml.addContact(rescuer);

    // add up-to-date creator (e.g. me) thereby also fulfilling minimum requirement
    eml.addCreator(rescuer);

    // add up-to-date metadata provider (e.g. me) thereby also fulfilling minimum requirement
    eml.addMetadataProvider(rescuer);

    // add external link to GBIF download (DwC-A format) that was used to rescue dataset - this must be preserved forever
    if (emlGbif.getPhysicalData().size() > 0) {
      PhysicalData physicalData = emlGbif.getPhysicalData().get(0);
      physicalData.setName("GBIF Occurrence Download");
      eml.addPhysicalData(physicalData);
    }

    // add me with role "processor" to associated parties
    eml.addAssociatedParty(rescuer);

    // ensure citation is set, otherwise take default citation from download.xml file
    if (eml.getCitationString() == null && emlGbif.getCitationString() != null) {
      eml.setCitation(emlGbif.getCitation());
    }

    // wipe resource logo, to avoid calling broken links
    eml.setLogoUrl(null);

    File tmpRescuedDir = Files.createTempDir();
    File updatedEml = new File(tmpRescuedDir, RESCUED_EML);
    EmlWriter.writeEmlFile(updatedEml, eml);
    LOG.info("Updated EML: " + updatedEml.getAbsolutePath());

    // retrieve verbatim.txt file
    FileUtils.moveFile(new File(tmpDecompressDir, GBIF_DOWNLOAD_VERBATIM), new File(tmpRescuedDir, RESCUED_OCCURRENCE));

    // retrieve meta.xml file
    File rescuedMeta = new File(tmpRescuedDir, RESCUED_META);
    FileUtils.copyInputStreamToFile(DatasetRescuer.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);

    // upload to IPT
    // ensure publishing organisation set (prerequisite being the organisation and user kbraak@gbif.org must be added to the IPT before it can be loaded)
    // ensure auto-generation of citation turned on
    // make its visibility registered by default
    // ensure resource metadata validates
    File iptResourceDir = Files.createTempDir();
    File resourceXml = new File(iptResourceDir, IPT_RESOURCE);
    Dataset dataset = datasetService.get(UUID.fromString(datasetKey));
    writeIptResourceFile(resourceXml, dataset, downloadMetadata);
    LOG.info("IPT Resource: " + resourceXml.getAbsolutePath());
  }

  public static void main(String[] args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    DatasetRescuer rescuer = new DatasetRescuer(injector.getInstance(DownloadRequestService.class),
      injector.getInstance(OccurrenceDownloadService.class), injector.getInstance(DatasetService.class));
    rescuer.rescue("98333cb6-6c15-4add-aa0e-b322bf1500ba");
  }

  /**
   * Writes an {@link Eml} object to an XML file using a Freemarker {@link Configuration}.
   *
   * @param f        the XML file to write to
   * @param dataset  the GBIF Dataset object
   * @param download the GBIF Download object
   */
  private void writeIptResourceFile(File f, Dataset dataset, Download download) throws IOException, TemplateException {
    Map<String, Object> map = Maps.newHashMap();
    map.put("dataset", dataset);
    map.put("download", download);
    writeFile(f, IPT_RESOURCE_TEMPLATE, map);
  }

  /**
   * Writes a map of data to a utf8 encoded file using a Freemarker {@link Configuration}.
   */
  private void writeFile(File f, String template, Object data) throws IOException, TemplateException {
    String result = processTemplateIntoString(FTL.getTemplate(template), data);
    Writer out = org.gbif.utils.file.FileUtils.startNewUtf8File(f);
    out.write(result);
    out.close();
  }

  private String processTemplateIntoString(Template template, Object model) throws IOException, TemplateException {
    StringWriter result = new StringWriter();
    template.process(model, result);
    return result.toString();
  }

  /**
   * Provides a freemarker template loader. It is configured to access the utf8 ipt folder on the classpath.
   */
  private static Configuration provideFreemarker() {
    TemplateLoader tl = new ClassTemplateLoader(DatasetRescuer.class, "/ipt");
    Configuration fm = new Configuration();
    fm.setDefaultEncoding("utf8");
    fm.setTemplateLoader(tl);
    return fm;
  }
}
