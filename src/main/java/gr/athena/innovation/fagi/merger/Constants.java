package gr.athena.innovation.fagi.merger;

/**
 *
 * @author nkarag
 */
public class Constants {
    
    /**
     * Help message.
     */
    public static final String HELP = "Usage:\n java -jar merger.jar -config <configPath>";
    
    /**
     * Partition filename.
     */
    public static final String PARTITION = "partition";

    /**
     * Fused dataset filename.
     */
    public static final String FUSED = "fused.nt";
    
    /**
     * Remaining dataset filename.
     */
    public static final String REMAINING = "remaining.nt";
    
    /**
     * Ambiguous dataset filename.
     */
    public static final String AMBIGUOUS = "ambiguous.nt";
    
    /**
     * Links filename.
     */
    public static final String LINKS = "links.nt";
    
    /**
     * Statistics filename.
     */
    public static final String STATS = "stats.json";
    
    /**
     * Fusion properties filename.
     */
    public static final String FUSION_PROPERTIES = "fusion.properties";

    /**
     * Class for constants of the XML syntax.
     */
    public static class XML {

        /**
         * Filename for the configuration XML file.
         */
        public static final String CONFIG_XML = "config.xml";

        /**
         * Filename for the configuration XSD file that describes the configuration XML file.
         */
        public static final String CONFIG_XSD = "config.xsd";

        /**
         * Name for the merge tag in XML.
         */
        public static final String MERGE = "merge";

        /**
         * Name for fusion-mode tag in XML.
         */
        public static final String FUSION_MODE = "fusionMode";

        /**
         * Name for number of partitions tag in XML.
         */
        public static final String PARTITIONS = "partitions";

        /**
         * Name for input directory tag in XML.
         */
        public static final String INPUT_DIR = "inputDir";

        /**
         * Name for output directory tag in XML.
         */
        public static final String OUTPUT_DIR = "outputDir";
        
    }
}
