/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import tinycrawler.crawler.Crawler;


/**
 *
 * @author igal
 */
public class Main {
    // Create an initialize the logger.

    private static final String LOG_FILENAME = "tinycrawler.log";
    private static final Logger logger = Logger.getLogger("TinyCrawler");

    static {
        try {
            FileHandler h = new FileHandler(LOG_FILENAME, false);
            h.setLevel(Level.ALL);
            logger.addHandler(h);

        } catch (IOException ex) {
            System.err.println("<!> Could not setup logger file !");
        } catch (SecurityException ex) {
            System.err.println("<!> Could not setup logger file !");
        }
    }

    public static void main(String[] args) throws URISyntaxException  {
        Crawler crawler = new Crawler();
        crawler.crawl("http://en.wikipedia.org/");
    }
}
