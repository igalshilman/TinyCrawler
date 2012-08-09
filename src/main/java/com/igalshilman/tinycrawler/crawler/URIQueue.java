package com.igalshilman.tinycrawler.crawler;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import java.net.URI;

/**
 * JobMaster
 *
 * @author igal
 */
public class URIQueue {

  private Set<URI> jobsHistory;
  private List<URI> jobs;

  public URIQueue() {
    jobsHistory = new HashSet<URI>();
    jobs = new LinkedList<URI>();
  }

  /**
   * addAll - adds <b>unique</b> URIs to crawl.
   * this operation will not add URIs that were previously crawled or are
   * pending to be crawled already.
   * <p/>
   * - this operation is thread safe.
   *
   * @param all a collection of URI to crawler.
   */
  public synchronized void addAll(Collection<? extends URI> all) {
    boolean newJobs = false;
    for (URI u : all) {
      if (jobsHistory.contains(u)) {
        continue;
      }
      jobsHistory.add(u);
      jobs.add(u);
      newJobs = true;
    }

    if (newJobs) {
      notifyAll();
    }
  }

  /**
   * takeOne - this method removes and return a currently pending URI
   * from the queue.
   * If the queue is empty this method will block.
   *
   * @return a URI to crawl.
   */
  public synchronized URI takeOne() throws InterruptedException {
    while (jobs.isEmpty()) {
      wait();
    }
    return jobs.remove(0);
  }
}
