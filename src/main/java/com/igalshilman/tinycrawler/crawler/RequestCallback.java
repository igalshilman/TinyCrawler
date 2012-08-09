/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler.crawler;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.log4j.Logger;

/**
 * @author igal
 */
class RequestCallback extends AsyncCompletionHandler<Boolean> {
  private static final Logger logger = Logger.getLogger("TinyCrawler");
  private final Task task;
  private final TaskQueue taskQueue;


  RequestCallback(Task task, TaskQueue taskQueue) {
    this.task = task;
    this.taskQueue = taskQueue;
  }

  private void accountComplition() {
    taskQueue.decrementPendingDownloads();
  }

  @Override
  public void onThrowable(Throwable t) {
    accountComplition();
    logger.error("Error during document download", t);
  }

  @Override
  public Boolean onCompleted(Response res) throws Exception {
    accountComplition();

    if (res.hasResponseBody()) {
      task.setHtmlContent(res.getResponseBody());
      logger.info("Adding document to docQueue " + res.getUri());
      taskQueue.incrementPendingProcessing();
      taskQueue.addProcessingTask(task);
      return Boolean.TRUE;
    }

    return Boolean.FALSE;
  }
}
