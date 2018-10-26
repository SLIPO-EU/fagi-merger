package gr.athena.innovation.fagi.merger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Merging of output that come from multiple fagi instances.
 *
 * @author nkarag
 */
public class Merger {

    private static final Logger LOG = LogManager.getRootLogger();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        String config = null;

        String arg;
        String value;

        int i = 0;

        while (i < args.length) {
            arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-help")) {
                    LOG.info(Constants.HELP);
                    System.exit(-1);
                }
            }
            value = args[i + 1];
            if (arg.equals("-config")) {
                config = value;
                break;
            }
            i++;
        }

        try {

            MergerInstance partitioner = new MergerInstance();
            partitioner.run(config);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.info(Constants.HELP);
            System.exit(-1);
        }
    }

}
