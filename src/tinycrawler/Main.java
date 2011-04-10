/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import tinycrawler.crawler.Crawler;
import tinycrawler.crawler.CrawlBuilder;


/**
 *
 * @author igal
 */
public class Main {
    // Create and initialize the logger.

    private static final String LOG_FILENAME = "tinycrawler.log";
    private static final Logger logger = Logger.getLogger("TinyCrawler");

    static {
        try {
            FileHandler h = new FileHandler(LOG_FILENAME, false);
            h.setFormatter(new SimpleFormatter());
            h.setLevel(Level.ALL);
            logger.addHandler(h);

        } catch (IOException ex) {
            System.err.println("<!> Could not setup logger file !");
        } catch (SecurityException ex) {
            System.err.println("<!> Could not setup logger file !");
        }
    }

    public static void main(String[] args) throws URISyntaxException  {
        CrawlBuilder builder = new CrawlBuilder();

        // Configure the crawler.
        builder.
                setIndexDirectory(new File("./index")).
                setSetFollowRedirect(true).
                setMaxFollowRedirect(2).
                setThreadCount(2).
                setTotalConnectionsPerHost(4).
                setTotalMaximumConnections(100);

        // lunch it!
        Crawler crawler = builder.build();
        crawler.crawl("http://en.wikipedia.org/");
    }
}
