package gr.athena.innovation.fagi.merger;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author nkarag
 */
public class ConfigParser {
    
    private static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger(ConfigParser.class);

    /**
     * Parses the configuration XML and produces the configuration object.
     * 
     * @param configurationPath the configuration file path.
     * @return the configuration object.
     * @throws WrongInputException indicates that something is wrong with the input.
     */
    public Configuration parse(String configurationPath) throws WrongInputException {

        LOG.info("Parsing configuration: " + configurationPath);
        Configuration configuration = Configuration.getInstance();

        try {

            File fXmlFile = new File(configurationPath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            doc.getDocumentElement().normalize();

            NodeList parts = doc.getElementsByTagName(Constants.XML.PARTITIONS);
            String partitions = parts.item(0).getTextContent();
            
            int partitionsNumber;
            
            try {
                partitionsNumber = Integer.parseInt(partitions);
            } catch (NumberFormatException e) {
                throw new WrongInputException("Number of partitions is not an integer.");
            }

            configuration.setPartitions(partitionsNumber);

            NodeList ua = doc.getElementsByTagName(Constants.XML.UNLINKED_A);
            String unlinkedA = ua.item(0).getTextContent();
            configuration.setUnlinkedA(unlinkedA);
            
            NodeList ub = doc.getElementsByTagName(Constants.XML.UNLINKED_B);
            String unlinkedB = ub.item(0).getTextContent();
            configuration.setUnlinkedB(unlinkedB);
            
            NodeList da = doc.getElementsByTagName(Constants.XML.DATASET_A);
            String datasetA = da.item(0).getTextContent();
            configuration.setDatasetA(datasetA);
            
            NodeList db = doc.getElementsByTagName(Constants.XML.DATASET_B);
            String datasetB = db.item(0).getTextContent();
            configuration.setDatasetB(datasetB);

            NodeList in = doc.getElementsByTagName(Constants.XML.INPUT_DIR);
            String inputDir = in.item(0).getTextContent();
            
            if(!new File(inputDir).isDirectory()){
                throw new WrongInputException("Input path is not a directory. Specify an existing directory.");
            }
            configuration.setInputDir(inputDir);

//            NodeList out = doc.getElementsByTagName(Constants.XML.OUTPUT_DIR);
//            String outputDir = out.item(0).getTextContent();
//            
//            if(!new File(outputDir).isDirectory()){
//                throw new WrongInputException("Output path is not a directory. Specify an existing directory path.");
//            }
//            configuration.setOutputDir(outputDir);

            NodeList partialOutputDirName = doc.getElementsByTagName(Constants.XML.PARTIAL_OUTPUT_DIR_NAME);
            String dirName = partialOutputDirName.item(0).getTextContent();

            configuration.setPartialOutputDirName(dirName);
            
            NodeList m = doc.getElementsByTagName(Constants.XML.FUSION_MODE);
            String modeString = m.item(0).getTextContent();
            EnumFusionMode mode = EnumFusionMode.fromString(modeString.toUpperCase());
            switch(mode) {
                case AA_MODE:
                case AB_MODE:
                case A_MODE:
                case BB_MODE:
                case BA_MODE:
                case B_MODE:
                case L_MODE:
                    configuration.setFusionMode(mode);
                    break;
                default:
                    LOG.info("Mode not supported.");
                    throw new UnsupportedOperationException("Wrong Output mode!");               
            }
            
            
            NodeList targetNodeList = doc.getElementsByTagName(Constants.XML.TARGET);
            Node targetNode = targetNodeList.item(0);
            NodeList targetChilds = targetNode.getChildNodes();
            for (int i = 0; i < targetChilds.getLength(); i++) {
                Node n = targetChilds.item(i);

                if (n.getNodeType() == Node.ELEMENT_NODE) {

                    if (n.getNodeName().equalsIgnoreCase(Constants.XML.OUTPUT_DIR)) {
                        NodeList out = doc.getElementsByTagName(Constants.XML.OUTPUT_DIR);
                        String outputDir = out.item(0).getTextContent();

                        if(!new File(outputDir).isDirectory()){
                            throw new WrongInputException("Output path is not a directory. Specify an existing directory path.");
                        }
                        configuration.setOutputDir(outputDir);

                    } else if (n.getNodeName().equalsIgnoreCase(Constants.XML.FUSED)) {
                        configuration.setFused(n.getTextContent());
                    } else if (n.getNodeName().equalsIgnoreCase(Constants.XML.REMAINING)) {
                        configuration.setRemaining(n.getTextContent());
                    } else if (n.getNodeName().equalsIgnoreCase(Constants.XML.AMBIGUOUS)) {
                        configuration.setAmbiguous(n.getTextContent());
                    } else if (n.getNodeName().equalsIgnoreCase(Constants.XML.STATISTICS)) {
                        configuration.setStatistics(n.getTextContent());
                    } else if (n.getNodeName().equalsIgnoreCase(Constants.XML.FUSION_LOG)) {
                        configuration.setFusionLog(n.getTextContent());
                    }
                }
                n.getNextSibling();
            }
            
        } catch (ParserConfigurationException | SAXException | IOException | DOMException e) {
            LOG.fatal("Exception occured while parsing the configuration: "
                    + configurationPath + "\n" + e);
            throw new WrongInputException(e.getMessage());
        }

        return configuration;
    }
}