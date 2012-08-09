/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler.crawler;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
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
        logger.log(Level.SEVERE, t.toString());
    }

    @Override
    public Boolean onCompleted(Response res) throws Exception {
        accountComplition();

        if (res.hasResponseBody()) {
            task.setHtmlContent(res.getResponseBody());
            logger.log(Level.INFO, "Adding document to docQueue {0}", res.getUri());
            taskQueue.incrementPendingProcessing();
            taskQueue.addProcessingTask(task);
            return Boolean.TRUE;
        }

        // We should log that we weren't fetch this document.
        logger.log(Level.INFO, "Found URI without a body [{0}]", res.getUri());
        return Boolean.FALSE;
    }
}
