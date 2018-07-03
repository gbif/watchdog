package org.gbif;

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
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.NodeType;
import org.gbif.metadata.eml.Agent;
import org.gbif.metadata.eml.Eml;
import org.gbif.metadata.eml.EmlFactory;
import org.gbif.metadata.eml.EmlWriter;
import org.gbif.metadata.eml.PhysicalData;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.gbif.DatasetRescuer.Mode.IPT_EXPORT;
import static org.gbif.DatasetRescuer.Mode.RESCUE;

/**
 */
public class GbifFranceDatasetRescuer {

  private static Logger LOG = LoggerFactory.getLogger(GbifFranceDatasetRescuer.class);

  private static final String RESCUED_EML = "eml.xml";
  private static final String RESCUED_OCCURRENCE = "occurrence.txt";
  private static final String RESCUED_META = "meta.xml";
  private static final String RESCUED_META_PATH = "/frMeta.xml";

  private static final String IPT_RESOURCE_TEMPLATE = "resource.ftl";
  private static final Configuration FTL = provideFreemarker();
  private static final String IPT_RESOURCE = "/resource.xml";
  private static final String IPT_SOURCES = "sources";
  private static final String VERSIONED_EML = "eml-1.0.xml";
  private static final String VERSIONED_DWCA = "dwca-1.0.zip";

  DownloadRequestService downloadRequestService;
  OccurrenceDownloadService occurrenceDownloadService;
  DatasetService datasetService;
  OrganizationService organizationService;
  NodeService nodeService;

  GbifFranceDatasetRescuer(DownloadRequestService occurrenceDownloadWsClient, OccurrenceDownloadService occurrenceDownloadService,
                           DatasetService datasetService, OrganizationService organizationService, NodeService nodeService) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;

    FTL.setTimeZone(TimeZone.getTimeZone("UTC"));
    FTL.setDateTimeFormat("yyyy-MM-dd HH:mm:ss.000 zzz");
  }

  /**
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey) throws IOException, ParserConfigurationException, SAXException, TemplateException, NoSuchFieldException {

    String gbifFrKey = "4A9DDA1F-B879-3E13-E053-2614A8C02B7C";
    datasetKey = gbifFrKey;

    Agent rescuer = new Agent();
//    rescuer.setEmail("systems@gbif.org");
    rescuer.setEmail("connexion@gbif.fr");

    Organization organization = organizationService.get(UUID.fromString("1928bdf0-f5d2-11dc-8c12-b8a03c50a862"));

    File tmpDwca = new File("/home/mblissett/"+gbifFrKey+".zip");

    Path tmpDecompressDir = Files.createTempDirectory("orphan-decompress-");
    CompressionUtil.decompressFile(tmpDecompressDir.toFile(), tmpDwca, true);
    LOG.info("Unzipped to: {}", tmpDecompressDir);

    // Open EML file
    InputStream emlIs = new FileInputStream(new File(tmpDecompressDir.toFile(), "eml.xml"));
    Eml eml = EmlFactory.build(emlIs);

    // ensure license is set!
    if (eml.parseLicenseUrl() == null) {
      throw new NoSuchFieldException("License must always be set!");
    }

    eml.getGeospatialCoverages().get(0).setDescription("See map.");

    // publishing organisation
//    Agent publishingOrg = new Agent();
//    publishingOrg.setOrganisation(organization.getTitle());

//      // add up-to-date point of contact thereby also fulfilling minimum requirement
//      eml.setContacts(Arrays.asList(rescuer));
//
//      // add up-to-date creator thereby also fulfilling minimum requirement in order of priority high to low
//      eml.setCreators(Arrays.asList(publishingOrg, rescuer));
//
//      // add up-to-date metadata provider thereby also fulfilling minimum requirement
//      eml.setMetadataProviders(Arrays.asList(rescuer));

    // ensure specimen preservation methods are lowercase, otherwise IPT doesn't recognize method
    ListIterator<String> iterator = eml.getSpecimenPreservationMethods().listIterator();
    while (iterator.hasNext()) {
      iterator.set(iterator.next().toLowerCase());
    }

    {
      // reset version to 1.0
      eml.setEmlVersion(1, 0);
    }

    // make DwC-A folder
    File dwcaFolder = Files.createTempDirectory("orphan-dwca-").toFile();

    // write eml.xml file to DwC-A folder
    File updatedEml = new File(dwcaFolder, RESCUED_EML);
    EmlWriter.writeEmlFile(updatedEml, eml);

    // retrieve verbatim.txt file, and copy to DwC-A folder
    File rescuedOccurrence = new File(dwcaFolder, RESCUED_OCCURRENCE);
    FileUtils.copyFile(new File(tmpDecompressDir.toFile(), "data.txt"), rescuedOccurrence);
    long recordCount = FileUtils.readLines(rescuedOccurrence, StandardCharsets.UTF_8).size() - 1;

    // retrieve meta.xml file, and copy to DwC-A folder
    File rescuedMeta = new File(dwcaFolder, RESCUED_META);
    FileUtils.copyInputStreamToFile(GbifFranceDatasetRescuer.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);

    // make IPT resource directory
    File iptResourceDir = new File("/home/mblissett/gbif.fr/", datasetKey);
    iptResourceDir.mkdir();

    {
      // upload to IPT

      // ensure publishing organisation set (prerequisite being the organisation and user informatics@gbif.org must be added to the IPT before it can be loaded)
      // ensure auto-generation of citation turned on
      // make its visibility registered by default
      File resourceXml = new File(iptResourceDir, IPT_RESOURCE);
      writeIptResourceFile(resourceXml, organization.getKey(), rescuer, rescuedOccurrence.length(), recordCount);

      // make sources folder in IPT resource folder
      File sources = new File(iptResourceDir, IPT_SOURCES);
      sources.mkdir();

      // retrieve verbatim.txt file, and copy to IPT sources folder
      FileUtils.copyFile(rescuedOccurrence, new File(sources, RESCUED_OCCURRENCE));

      // write eml.xml file to IPT resource folder
      File iptEml = new File(iptResourceDir, RESCUED_EML);
      EmlWriter.writeEmlFile(iptEml, eml);

      // write eml.xml file version 1.0 to IPT resource folder
      File versionedEml = new File(iptResourceDir, VERSIONED_EML);
      EmlWriter.writeEmlFile(versionedEml, eml);
    }

    // write compressed (.zip) DwC-A file version 1.0 to IPT resource folder
    File versionedDwca = new File(iptResourceDir, VERSIONED_DWCA);
    CompressionUtil.zipDir(dwcaFolder, versionedDwca);

    LOG.info("IPT Resource / rescue folder: " + iptResourceDir.getAbsolutePath());

    FileUtils.deleteDirectory(tmpDecompressDir.toFile());
    FileUtils.deleteDirectory(dwcaFolder);
  }

  public static void main(String... args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException,
    NoSuchFieldException, InterruptedException, URISyntaxException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    GbifFranceDatasetRescuer rescuer = new GbifFranceDatasetRescuer(injector.getInstance(DownloadRequestService.class),
      injector.getInstance(OccurrenceDownloadService.class), injector.getInstance(DatasetService.class),
      injector.getInstance(OrganizationService.class), injector.getInstance(NodeService.class));

//    if (args.length < 1) {
//      System.err.println("Give dataset keys as argument");
//      System.exit(1);
//    }
//
//    for (String datasetKey : args) {
//      rescuer.rescue(datasetKey);
//    }
    rescuer.rescue("x");
  }

  /**
   * Writes an {@link Eml} object to an XML file using a Freemarker {@link Configuration}.
   */
  private void writeIptResourceFile(File f, UUID publishingOrganizationKey, Agent rescuer, long occurrenceFileSize, long totalRecords) throws IOException, TemplateException {
    Map<String, Object> map = new HashMap();
    map.put("publishingOrganizationKey", publishingOrganizationKey);
    map.put("rescuer", rescuer);
    map.put("occurrenceFileSize", occurrenceFileSize);
    map.put("created", new Date());
    map.put("totalRecords", totalRecords);
    writeFile(f, "frResource.ftl", map);
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
   * Provides a freemarker template loader. It is configured to access the UTF-8 IPT folder on the classpath.
   */
  private static Configuration provideFreemarker() {
    TemplateLoader tl = new ClassTemplateLoader(GbifFranceDatasetRescuer.class, "/ipt");
    Configuration fm = new Configuration();
    fm.setDefaultEncoding("utf8");
    fm.setTemplateLoader(tl);
    return fm;
  }
}
