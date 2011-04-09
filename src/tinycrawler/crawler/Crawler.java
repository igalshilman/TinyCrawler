/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package tinycrawler.crawler;

import com.ning.http.client.AsyncHttpClientConfig;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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

    public Crawler() {
        
    }

     public boolean crawl(String baseURI) throws URISyntaxException {
        URIQueue uriQueue = new URIQueue();
        BlockingQueue<Document> docQueue = new LinkedBlockingQueue<Document>();

        IndexWriter indexWriter = prepareIndexWriter();
        if (indexWriter == null) {
            System.err.println("Failed preparing IndexWriter, aborting.");
            return false;
        }

        ExecutorService threadPool =
                spawnDocumentProcessors(2, docQueue, uriQueue, indexWriter);

        DocumentFetcher documentFetcher =
                new DocumentFetcher(docQueue, uriQueue, asyncHttpClient());

        uriQueue.addAll(Collections.singletonList(new URI(baseURI)));

        documentFetcher.fetch();

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(2, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {

        }

        try {
            indexWriter.close();
        } catch (CorruptIndexException ex) {
            logger.log(Level.SEVERE, "Failed closing the index. got{0}", ex.toString());
        } catch (IOException ex) {
            logger.log(Level.SEVERE,"Failed closing the index. {0}", ex.toString());
        }

        logger.log(Level.FINE,"done.");
        return true;
    }

    /**
     * PrepareIndexWriter - Initialize an Index to store the crawled documents.
     * @return an instance of IndexWriter.
     */
    private IndexWriter prepareIndexWriter() {
        File indexDir = new File("./index");
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
        IndexWriterConfig indexConfig = new IndexWriterConfig(Version.LUCENE_31, analyzer);

        IndexWriter writer = null;
        try {
            Directory fs = new NIOFSDirectory(indexDir);
            writer = new IndexWriter(fs, indexConfig);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Failed to create an IndexWriter. ");
        }
        return writer;
    }

    /**
     * spwanDocumentProcessors - Builds a DocumentProccessor workers and spawn
     * them in a thred pool.
     *
     * @param workerCount The number of workers needed. (should usually be number of CPUs - 1).
     * @param docQueue The blocking queue that holds the documents to be processed.
     *                  (the workers consume documents out of this queue).
     * @param uriQueue The blocking queued contains URIs to fetch and process.
     *                  (The workers produce links to this queue out of the processed docs).
     * @param writer    The IndexWriter that actually stores and index this documents.
     * @return an underlying thread pool that contains the workers.
     */
    private ExecutorService spawnDocumentProcessors(int workerCount,
            BlockingQueue<Document> docQueue,
            URIQueue uriQueue,
            IndexWriter writer) {

        ExecutorService threadPool = Executors.newFixedThreadPool(workerCount);

        for (int i = 0; i < workerCount; i++) {
            threadPool.submit(new DocumentProcessor(docQueue, uriQueue, writer));
        }
        return threadPool;
    }

    /**
     * asyncHttpClient this configures the behavior of the AsyncHTTPClient.
     * @return an async-http-client configurable client.
     */
    private AsyncHttpClientConfig asyncHttpClient() {
        AsyncHttpClientConfig clientConfig = new AsyncHttpClientConfig.Builder().setMaximumConnectionsPerHost(4).
                setMaximumConnectionsTotal(100).
                setFollowRedirects(true).
                setMaximumNumberOfRedirects(3).
                setUserAgent("TinyCrawler").
                build();

        return clientConfig;
    }

}
