/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler.crawler;

import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.extra.ThrottleRequestFilter;
import com.ning.http.client.filter.RequestFilter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

/**
 *
 * @author igal
 */
public class Crawler {
    private static final Logger logger = Logger.getLogger("TinyCrawler");

    // Configurable paramters vis builder, but are final here.
    private final File indexDirectory;
    private final int threadCount;
    private final int totalMaximumConnections;
    private final int totalConnectionsPerHost;
    private final boolean followRedirect;
    private final int maxFollowRedirect;
    private final String userAgent;
    private final int maxCrawlDepth;

    private TaskQueue taskQueue;
    private IndexWriter indexWriter;
    private ExecutorService threadPool;
    private DocumentFetcher documentFetcher;

    //
    // Packge private constructor.
    // Inorder to get a Crawler instance we need to use the builder.
    //
     Crawler(File indexDirectory, int threadCount, int totalMaximumConnections,
             int totalConnectionsPerHost, boolean followRedirect,
             int maxFollowRedirect,String userAgent,
             int maxCrawlDepth) {

       this.indexDirectory = indexDirectory;
       this.threadCount = threadCount;
       this.totalMaximumConnections = totalMaximumConnections;
       this.totalConnectionsPerHost = totalConnectionsPerHost;
       this.followRedirect = followRedirect;
       this.maxFollowRedirect = maxFollowRedirect;
       this.userAgent = userAgent;
       this.maxCrawlDepth = maxCrawlDepth;

       // setup the task queue
       taskQueue = new TaskQueue();
    }

      //

    /**
     * crawl - this function will start the crawling. A call to this function will block,
     * the currently executing thread until the crawling will end.
     * @param baseURI the starting URI.
     * @throws URISyntaxException incase this is malformed URI.
     */
    public void crawl(String baseURI) throws URISyntaxException {
        final URI uri = new URI(baseURI);
        // setup all the parameters and init the thread pool.
        initialize();
        // add the first URI to crawl and, start crawling.
        getTaskQueue().addDownloadTasks(Collections.singletonList(new Task(uri,0)));
        startCrawling();
    }
    
    private void initialize() {
        indexWriter = prepareIndexWriter();
        threadPool = spawnDocumentProcessors();
        documentFetcher = new DocumentFetcher(this , prepareHTTPClient());
    }
    
    
    /**
     * PrepareIndexWriter - Initialize an Index to store the crawled documents.
     * @return an instance of IndexWriter.
     * @throws IllegalArgumentException If for some reason the we are unable to prepare the index.
     */
    private IndexWriter prepareIndexWriter() {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
        IndexWriterConfig indexConfig = new IndexWriterConfig(Version.LUCENE_31, analyzer);

        IndexWriter writer = null;
        try {
            Directory fs = new NIOFSDirectory(this.indexDirectory);
            writer = new IndexWriter(fs, indexConfig);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Failed to create an IndexWriter. ");
            throw new IllegalArgumentException("Could not initialize the index, please check the log.");
        }
        
        return writer;
    }

    /**
     * spwanDocumentProcessors - Builds a DocumentProccessor workers and spawn
     * them in a thread pool.
     *
     * @param workerCount The number of workers needed. (should usually be number of CPUs - 1).
     * @param docQueue The blocking queue that holds the documents to be processed.
     *                  (the workers consume documents out of this queue).
     * @param uriQueue The blocking queued contains URIs to fetch and process.
     *                  (The workers produce links to this queue out of the processed docs).
     * @param writer    The IndexWriter that actually stores and index this documents.
     * @return an underlying thread pool that contains the workers.
     */
    private ExecutorService spawnDocumentProcessors() {
        final int n = threadCount;
        ExecutorService es = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            es.submit(new DocumentProcessor(this));
        }
        return es;
    }

    /**
     * prepareHTTPClient this configures the behavior of the AsyncHTTPClient.
     * @return an async-http-client configurable client.
     */
    private AsyncHttpClientConfig prepareHTTPClient() {
      //  RequestFilter throttle = new ThrottleRequestFilter(totalMaximumConnections);
        AsyncHttpClientConfig clientConfig = 
                new AsyncHttpClientConfig.Builder().
                setMaximumConnectionsPerHost(totalConnectionsPerHost).
                setMaximumConnectionsTotal(totalMaximumConnections).
                setAllowPoolingConnection(true).
                setFollowRedirects(followRedirect).
                setMaximumNumberOfRedirects(maxFollowRedirect).
                setUserAgent(userAgent).
        //        addRequestFilter(throttle).
                build();

        return clientConfig;
    }

    private void startCrawling() {
        documentFetcher.startFeatching();
        threadPool.shutdownNow();
        try {
            threadPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
        }

        try {
            indexWriter.close();
            logger.log(Level.INFO, "Index is closed.");
        } catch (CorruptIndexException ex) {
            logger.log(Level.SEVERE, "Failed closing the index. got{0}", ex.toString());
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Failed closing the index. {0}", ex.toString());
        }
        logger.log(Level.FINE, "done.");
    }

    //
    // This section, holds packge-private getters.
    // They are used by DocumentProcessor, and DocumentFetcher only.
    //
    // <!> These Getters are THREAD SAFE.
    //

    /**
     * @return the uriQueue
     */
    TaskQueue getTaskQueue() {
        return taskQueue;
    }
  
    /**
     * @return the indexWriter
     */
    IndexWriter getIndexWriter() {
        return indexWriter;

    }
    /**
     * @return max crawl depth
     */
    int getMaxCrawlDepth() {
        return maxCrawlDepth;
    }

    int getTotalMaximumConnections() {
        return totalMaximumConnections;
    }
}