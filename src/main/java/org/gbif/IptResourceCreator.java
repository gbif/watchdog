//package org.gbif;
//
//import freemarker.cache.ClassTemplateLoader;
//import freemarker.cache.TemplateLoader;
//import freemarker.template.Configuration;
//import freemarker.template.Template;
//import freemarker.template.TemplateException;
//import org.apache.commons.io.FileUtils;
//import org.gbif.api.model.occurrence.Download;
//import org.gbif.api.model.registry.Dataset;
//import org.gbif.api.model.registry.Node;
//import org.gbif.api.model.registry.Organization;
//import org.gbif.api.service.registry.DatasetService;
//import org.gbif.api.service.registry.NodeService;
//import org.gbif.api.service.registry.OrganizationService;
//import org.gbif.metadata.eml.Agent;
//import org.gbif.metadata.eml.Eml;
//import org.gbif.metadata.eml.EmlFactory;
//import org.gbif.metadata.eml.EmlWriter;
//import org.gbif.utils.file.CompressionUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.*;
//
///**
// * Generate an IPT resource directory from a DWCA.
// */
//public class IptResourceCreator {
//
//  private static Logger LOG = LoggerFactory.getLogger(IptResourceCreator.class);
//
//  private static final String GBIF_DOWNLOAD_EML = "metadata.xml";
//  private static final String GBIF_DOWNLOAD_VERBATIM = "verbatim.txt";
//
//  private static final String RESCUED_EML = "eml.xml";
//  private static final String RESCUED_OCCURRENCE = "occurrence.txt";
//  private static final String RESCUED_META = "meta.xml";
//  private static final String RESCUED_META_PATH = "/meta.xml";
//
//  private static final String IPT_RESOURCE_TEMPLATE = "resource.ftl";
//  private static final Configuration FTL = provideFreemarker();
//  private static final String IPT_RESOURCE = "/resource.xml";
//  private static final String IPT_SOURCES = "sources";
//  private static final String VERSIONED_EML = "eml-1.0.xml";
//  private static final String VERSIONED_DWCA = "dwca-1.0.zip";
//
//
//  DatasetService datasetService;
//  OrganizationService organizationService;
//  NodeService nodeService;
//
//  public IptResourceCreator(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService) throws IOException {
//    this.datasetService = datasetService;
//    this.organizationService = organizationService;
//    this.nodeService = nodeService;
//
//    FTL.setTimeZone(TimeZone.getTimeZone("GMT"));
//    FTL.setDateTimeFormat("yyyy-MM-dd HH:mm:ss zzz");
//  }
//
//  public void rescue(String datasetKey) throws Exception {
//    UUID uuid = UUID.fromString(datasetKey);
//    Dataset dataset = datasetService.get(uuid);
//    Organization organization = organizationService.get(dataset.getPublishingOrganizationKey());
//    Node node = nodeService.get(organization.getEndorsingNodeKey());
//
//    Agent rescuer = new Agent();
//
//    // Katia Cezón GBIF Spain
//    rescuer.setFirstName("Katia");
//    rescuer.setLastName("Cezón");
//    rescuer.setEmail("katia@gbif.es");
//    rescuer.setOrganisation("GBIF Spain");
//
//    // Extract EML from cached DWCA
//    Path tmpDwcaDir = Files.createTempDirectory("orphan-cached-dwca-");
//    File dwca = new File("./"+datasetKey+".zip");
//
//    Path tmpDecompressDir = Files.createTempDirectory("orphan-decompress-");
//    CompressionUtil.decompressFile(tmpDecompressDir.toFile(), dwca, true);
//    LOG.info("Unzipped to: {}", tmpDecompressDir);
//
//    InputStream emlGbifIs = new FileInputStream(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_EML));
//    Eml emlFromCachedDwca = EmlFactory.build(emlGbifIs);
//    LOG.info(emlFromCachedDwca.getPhysicalData().toString());
//
//    // ensure license is set!
//    if (emlFromCachedDwca.parseLicenseUrl() == null) {
//      throw new NoSuchFieldException("License must always be set!");
//    }
//
//    // publishing organisation
//    Agent publishingOrg = new Agent();
//    publishingOrg.setOrganisation(organization.getTitle());
//
//    // For genuine rescues which are like a republication.
//    if (false) {
//      // add up-to-date point of contact thereby also fulfilling minimum requirement
//      emlFromCachedDwca.setContacts(Arrays.asList(rescuer));
//
//      // add up-to-date creator thereby also fulfilling minimum requirement in order of priority high to low
//      emlFromCachedDwca.setCreators(Arrays.asList(publishingOrg, rescuer));
//
//      // add up-to-date metadata provider thereby also fulfilling minimum requirement
//      emlFromCachedDwca.setMetadataProviders(Arrays.asList(rescuer));
//    }
//    else if (false) {
//      // we haven't republished this
//      emlFromCachedDwca.setPubDate(null);
//    }
//
//    // wipe resource logo, to avoid calling broken links, if it is broken
////    if (eml.getLogoUrl() != null) {
////      String logoUrl = "";
////      try {
////        HttpUtil.Response logoResponse = httpUtil.get(eml.getLogoUrl());
////        if (logoResponse.getStatusCode() == 200) {
////          logoUrl = eml.getLogoUrl();
////        }
////      } catch (Exception e) {}
////      eml.setLogoUrl(logoUrl);
////    }
//
//    // reset version to 1.0
////    emlFromCachedDwca.setEmlVersion(1, 0);
//
//    // make DwC-A folder
//    File dwcaFolder = Files.createTempDirectory("orphan-dwca-").toFile();
//
//    // write eml.xml file to DwC-A folder
//    File updatedEml = new File(dwcaFolder, RESCUED_EML);
//    EmlWriter.writeEmlFile(updatedEml, emlFromCachedDwca);
//
//    // retrieve verbatim.txt file, and copy to DwC-A folder
//    File rescuedOccurrence = new File(dwcaFolder, RESCUED_OCCURRENCE);
//    FileUtils.copyFile(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_VERBATIM), rescuedOccurrence);
//
//    // retrieve meta.xml file, and copy to DwC-A folder
//    File rescuedMeta = new File(dwcaFolder, RESCUED_META);
////    FileUtils.copyInputStreamToFile(DatasetRescuerByDownload.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);
//
//    // make IPT resource directory
//    File iptResourceDir = new File("./", datasetKey);
//    iptResourceDir.mkdir();
//
//    // upload to IPT
//
//    // ensure publishing organisation set (prerequisite being the organisation and user informatics@gbif.org must be added to the IPT before it can be loaded)
//    // ensure auto-generation of citation turned on
//    // make its visibility registered by default
//    File resourceXml = new File(iptResourceDir, IPT_RESOURCE);
//    writeIptResourceFile(resourceXml, dataset, null, rescuer, rescuedOccurrence.length());
//
//    // make sources folder in IPT resource folder
//    File sources = new File(iptResourceDir, IPT_SOURCES);
//    sources.mkdir();
//
//    // retrieve verbatim.txt file, and copy to IPT sources folder
//    FileUtils.copyFile(rescuedOccurrence, new File(sources, RESCUED_OCCURRENCE));
//
//    // write eml.xml file to IPT resource folder
//    File iptEml = new File(iptResourceDir, RESCUED_EML);
//    EmlWriter.writeEmlFile(iptEml, emlFromCachedDwca);
//
//    // write eml.xml file version 1.0 to IPT resource folder
//    File versionedEml = new File(iptResourceDir, VERSIONED_EML);
//    EmlWriter.writeEmlFile(versionedEml, emlFromCachedDwca);
//
//
//    // write compressed (.zip) DwC-A file version 1.0 to IPT resource folder
//    File versionedDwca = new File(iptResourceDir, VERSIONED_DWCA);
//    CompressionUtil.zipDir(dwcaFolder, versionedDwca);
//
//    LOG.info("IPT Resource / rescue folder: " + iptResourceDir.getAbsolutePath());
//
//    FileUtils.deleteDirectory(tmpDecompressDir.toFile());
//    FileUtils.deleteDirectory(tmpDwcaDir.toFile());
//    FileUtils.deleteDirectory(dwcaFolder);
//  }
//
//
//  /**
//   * Writes an {@link Eml} object to an XML file using a Freemarker {@link Configuration}.
//   *
//   * @param f        the XML file to write to
//   * @param dataset  the GBIF Dataset object
//   * @param download the GBIF Download object
//   */
//  private void writeIptResourceFile(File f, Dataset dataset, Download download, Agent rescuer, long occurrenceFileSize) throws IOException, TemplateException {
//    Map<String, Object> map = new HashMap();
//    map.put("dataset", dataset);
//    map.put("download", download);
//    map.put("rescuer", rescuer);
//    map.put("occurrenceFileSize", occurrenceFileSize);
//    writeFile(f, IPT_RESOURCE_TEMPLATE, map);
//  }
//
//  /**
//   * Writes a map of data to a utf8 encoded file using a Freemarker {@link Configuration}.
//   */
//  private void writeFile(File f, String template, Object data) throws IOException, TemplateException {
//    String result = processTemplateIntoString(FTL.getTemplate(template), data);
//    Writer out = org.gbif.utils.file.FileUtils.startNewUtf8File(f);
//    out.write(result);
//    out.close();
//  }
//
//  private String processTemplateIntoString(Template template, Object model) throws IOException, TemplateException {
//    StringWriter result = new StringWriter();
//    template.process(model, result);
//    return result.toString();
//  }
//
//  /**
//   * Provides a freemarker template loader. It is configured to access the UTF-8 IPT folder on the classpath.
//   */
//  private static Configuration provideFreemarker() {
////    TemplateLoader tl = new ClassTemplateLoader(DatasetRescuerByDownload.class, "/ipt");
////    Configuration fm = new Configuration();
////    fm.setDefaultEncoding("utf8");
////    fm.setTemplateLoader(tl);
////    return fm;
//    return null;
//  }
//}
