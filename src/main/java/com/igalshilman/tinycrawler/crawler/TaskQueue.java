package com.igalshilman.tinycrawler.crawler;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TaskQueue
 * @author igal
 */
public class TaskQueue {

    private Set<URI> taskHistory;

    private List<Task> downloadQueue;
    private ReentrantLock downloadQueueLock;
    private Condition downloadQueueNonEmpty;

    private BlockingQueue<Task> processQueue;

    private AtomicInteger pendingDownloads;
    private AtomicInteger pendingProcessing;
        

    public TaskQueue() {
        taskHistory = new HashSet<URI>();
        downloadQueue = new LinkedList<Task>();
        processQueue = new LinkedBlockingQueue<Task>();
        downloadQueueLock = new ReentrantLock();
        downloadQueueNonEmpty = downloadQueueLock.newCondition();
        pendingDownloads = new AtomicInteger(0);
        pendingProcessing = new AtomicInteger(0);
    }

    /**
     * addAll - adds <b>unique</b> URIs to crawl.
     * this operation will not add URIs that were previously crawled or are
     * pending to be crawled already.
     *
     * - this operation is thread safe.
     * @param all a collection of URI to crawler.
     */
    public void addDownloadTasks(Collection<? extends Task> all) {
        boolean newJobs = false;
        downloadQueueLock.lock();
        try {
            for (Task u : all) {
                if (taskHistory.contains(u.getURI())) {
                    continue;
                }
                taskHistory.add(u.getURI());
                downloadQueue.add(u);
                newJobs = true;
            }
            if (newJobs) {
                downloadQueueNonEmpty.signalAll();
            }
        } finally {
            downloadQueueLock.unlock();
        }
    }

    /**
     * poll - this method removes and returns a currently pending URIContainer
     * from the queue.
     * If the queue is empty this method will wait for @timeout time units.
     * If after @timeout no URIs are found in the queue it will return NULL.
     * @param timeout amount of time units to wait.
     * @param unit a type of time units.
     * @return URI to fetch, or null if none is present after @timeout time units.
     * @throws InterruptedException
     */
    public Task pollDownloadTask(int timeout, TimeUnit unit) throws InterruptedException {
        downloadQueueLock.lock();
        try {
            if (!downloadQueue.isEmpty()) {
                return downloadQueue.remove(0);
            }
            downloadQueueNonEmpty.await(timeout, unit);
            if (!downloadQueue.isEmpty()) {
                return downloadQueue.remove(0);
            }
            return null;
        } finally {
            downloadQueueLock.unlock();
        }
    }

    public void addProcessingTask(Task task) {
        processQueue.add(task);
    }

    public Task takeProcessingTask() throws InterruptedException {
        return processQueue.take();
    }

    public int incrementPendingDownload() {
        return pendingDownloads.incrementAndGet();
    }

    public int decrementPendingDownloads() {
        return pendingDownloads.decrementAndGet();
    }

    public int incrementPendingProcessing() {
        return pendingProcessing.incrementAndGet();
    }

    public int decrementPendingProcessing() {
        return pendingProcessing.decrementAndGet();
    }

    public int getPendingDownloads() {
        return pendingDownloads.get();
    }

    public int getPendingProcessing() {
        return pendingProcessing.get();
    }
}
