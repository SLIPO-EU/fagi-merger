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
     * Statistics filename.
     */
    public static final String STATS = "stats.json";
    
    /**
     * Temporary file extension.
     */
    public static final String TEMP = ".temp";
    
    /**
     * Initial value for property.
     */
    public static final String ZERO_VALUE = "0";
    
    /**
     * Fusion properties filename.
     */
    public static final String FUSION_PROPERTIES = "fusion.properties";
    
    /**
     * Property used in the "fusion.properties" file.
     */
    public static final String FUSED_PROPERTY = "fused";
    
    /**
     * Property used in the "fusion.properties" file.
     */
    public static final String REJECTED_PROPERTY = "rejected";

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
         * Name for the input file that contains the unlinked entities from dataset A.
         */
        public static final String UNLINKED_A = "unlinkedA";
        
        /**
         * Name for the input file that contains the unlinked entities from dataset B.
         */
        public static final String UNLINKED_B = "unlinkedB";
        
        /**
         * Name for the source dataset A.
         */
        public static final String DATASET_A = "datasetA";

        /**
         * Name for the source dataset B.
         */
        public static final String DATASET_B = "datasetB";

        /**
         * Name for input directory tag in XML.
         */
        public static final String INPUT_DIR = "inputDir";

        /**
         * Name for output directory tag in XML.
         */
        public static final String OUTPUT_DIR = "outputDir";
        
    }
    
    /**
     * Class of constants for statistics.
     */
    public static class Stats {

        /**
         * Value A field.
         */
        public static final String VALUE_A = "valueA";

        /**
         * Value B field.
         */
        public static final String VALUE_B = "valueB";
        
        /**
         * Value total field.
         */
        public static final String VALUE_TOTAL = "valueTotal";
        
        /**
         * Value both field.
         */
        public static final String VALUE_BOTH = "both";
        
        /**
         * Type field.
         */
        public static final String TYPE = "type";
        
        /**
         * Undefined value.
         */
        public static final String UNDEFINED = "UNDEFINED";
        
        /**
         * Class containing keys for statistics.
         */
        public static class Keys {
            
            /**
             * Key for percent statistics.
             */
            public static final String PERCENT = "Percent";
            
            /**
             * Key for full matching value statistics.
             */
            public static final String FULL = "full";
            
            /**
             * Longer value key.
             */
            public static final String LONGER = "longer";

            /**
             * Linked POIs key.
             */
            public static final String LINKED_POIS = "linkedPois";
            
            /**
             * Total empty properties key.
             */
            public static final String TOTAL_EMPTY_PROPERTIES = "totalEmptyProperties";
            
            /**
             * Total non-empty properties key.
             */
            public static final String TOTAL_NON_EMPTY_PROPERTIES = "totalNonEmptyProperties";

            /**
             * Linked vs total POIs key.
             */
            public static final String LINKED_VS_TOTAL = "linkedVsTotal";
            
            /**
             * Linked triples key.
             */
            public static final String LINKED_TRIPLES = "linkedTriples";

        }
    }
}
