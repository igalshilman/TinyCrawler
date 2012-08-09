/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler;

import java.io.File;
import java.net.URISyntaxException;
import com.igalshilman.tinycrawler.crawler.Crawler;
import com.igalshilman.tinycrawler.crawler.CrawlBuilder;
import org.apache.log4j.Logger;

/**
 *
 * @author igal
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) throws URISyntaxException  {
        CrawlBuilder builder = new CrawlBuilder();

        // Configure the crawler.
        builder.
                setIndexDirectory(new File("./index")).
                setSetFollowRedirect(true).
                setMaxFollowRedirect(2).
                setThreadCount(2).
                setTotalConnectionsPerHost(4).
                setTotalMaximumConnections(100).
                setMaxCrawlDepth(2);

        // lunch it!
        Crawler crawler = builder.build();
        crawler.crawl("http://en.wikipedia.org/");
    }
}
