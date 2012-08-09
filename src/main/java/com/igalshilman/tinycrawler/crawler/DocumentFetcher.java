/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler.crawler;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.concurrent.TimeUnit;


/**
 * @author igal
 */
public class DocumentFetcher {
  private static final Logger logger = Logger.getLogger(DocumentFetcher.class);
  private final TaskQueue taskQueue;
  private final AsyncHttpClientConfig clientConfig;
  private final int totalOpenConnections;
  private long sleepTime = 100;


  public DocumentFetcher(Crawler crawler, AsyncHttpClientConfig clientConfig) {
    this.taskQueue = crawler.getTaskQueue();
    this.clientConfig = clientConfig;
    this.totalOpenConnections = crawler.getTotalMaximumConnections();
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

  /**
   * Throttle Connections. If we are over the upper limit,
   * (i.e 90%) then sleep for 100*2^i milliseconds, increase i with each iteration.
   * If we are under the lower limit (i.e. 10%) then reset the sleep time to 1000 millis.
   */
  private void throttleConnections() throws InterruptedException {
    final long upperLimit = (3 * totalOpenConnections) / 4;
    final long upperSafeLimit = (1 * totalOpenConnections) / 10;
    long openConnections = taskQueue.getPendingDownloads();

    while (openConnections > upperLimit) {
      Thread.sleep(sleepTime);
      sleepTime *= 2;
      openConnections = taskQueue.getPendingDownloads();
    }
    if (openConnections < upperSafeLimit) {
      sleepTime = 100;
    }
  }

  public void startFeatching() {
    final AsyncHttpClient client = new AsyncHttpClient(clientConfig);

    for (; ; ) {
      try {
        final Task tsk = taskQueue.pollDownloadTask(30, TimeUnit.SECONDS);
        if (shouldTerminate(tsk)) {
          break;
        }

        final URI uri = tsk.getURI();
        final String address = uri.toString();
        final RequestCallback cb = new RequestCallback(tsk, taskQueue);

        logger.info("Trying to fetch " + address);
        // This is a workaround a BUG in AsyncHTTPClient of connection throttle.
        throttleConnections();
        taskQueue.incrementPendingDownload();
        client.prepareGet(address).execute(cb);

      } catch (InterruptedException ex) {
        logger.warn("Interrupted.");
      } catch (Throwable ex) {
        logger.error("Error during document fetching", ex);
      }
    }
  }
}
