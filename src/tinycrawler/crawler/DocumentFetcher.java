/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler.crawler;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author igal
 */
public class DocumentFetcher {
    private static final Logger logger = Logger.getLogger("TinyCrawler");
    private final TaskQueue taskQueue;
    private final AsyncHttpClientConfig clientConfig;


    public DocumentFetcher(Crawler crawler, AsyncHttpClientConfig clientConfig) {
        this.taskQueue = crawler.getTaskQueue();
        this.clientConfig = clientConfig;
    }

    private boolean shouldTerminate(Task t) {
        if (t != null) {
            return false;
        }

        /* We have returned from uriQueue with NULL uri, that means
         * that no URIs were added to  the queue for some time,
         * therefore it will be a good stoping criteria, UNLESS:
         *  1. other downloads are pending.
         *  2. other document processing are pending.
         * 
         */
        if (taskQueue.getPendingDownloads() > 0 || taskQueue.getPendingProcessing() > 0) {
            return false;
        }
        /* Ok, the queue is empty, no downloads nor procsessing are awating,
         * that means that we are done.
         */
        return true;
    }

    public void startFeatching() {
        final AsyncHttpClient client = new AsyncHttpClient(clientConfig);

        for (;;) {
            try {
                final Task tsk = taskQueue.pollDownloadTask(30, TimeUnit.SECONDS);
                if (shouldTerminate(tsk)) {
                    break;
                }

                final URI uri = tsk.getURI();
                final String address = uri.toString();
                final RequestCallback cb = new RequestCallback(tsk,taskQueue);

                logger.log(Level.INFO, "Trying to fetch {0}",address);
                taskQueue.incrementPendingDownload();
                client.prepareGet(address).execute(cb);

            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, "Interrupted.");
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            } catch (IllegalArgumentException ex) {
               logger.log(Level.SEVERE, null, ex);
            }
        }

        // Okay, finished with downloading.
        logger.log(Level.INFO, "DocumentFetcher is done.");
    }
}
