/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler.crawler;

import java.io.File;

/**
 * @author igal
 */
public class CrawlBuilder {
  // Configurable paramters set to defaults.
  private File indexDirectory = new File("./");
  private int threadCount = 2;
  private int totalMaximumConnections = 100;
  private int totalConnectionsPerHost = 4;
  private boolean setFollowRedirect = true;
  private int maxFollowRedirect = 2;
  private String userAgent = "TinyCrawler";
  private int maxCrawlDepth = Integer.MAX_VALUE - 1;

  public Crawler build() {
    return new Crawler(indexDirectory,
            threadCount,
            totalMaximumConnections,
            totalConnectionsPerHost,
            setFollowRedirect,
            maxFollowRedirect,
            userAgent,
            maxCrawlDepth);
  }

  /**
   * @return the indexDirectory
   */
  public File getIndexDirectory() {
    return indexDirectory;
  }

  /**
   * @param indexDirectory the indexDirectory to set
   */
  public CrawlBuilder setIndexDirectory(File indexDirectory) {
    if (indexDirectory == null) {
      throw new NullPointerException("indexDirectory is null");
    }
    this.indexDirectory = indexDirectory;
    return this;
  }

  /**
   * @return the numOfThreads
   */
  public int getThreadCount() {
    return threadCount;
  }

  /**
   * @param threadCount the numOfThreads to set
   */
  public CrawlBuilder setThreadCount(int threadCount) {
    if (threadCount < 1) {
      throw new IllegalArgumentException(String.format("Number Of threads suppose to be at least one. %d given", threadCount));
    }
    this.threadCount = threadCount;
    return this;
  }

  /**
   * @return the totalMaximumConnections
   */
  public int getTotalMaximumConnections() {
    return totalMaximumConnections;
  }

  /**
   * @param totalMaximumConnections the totalMaximumConnections to set
   */
  public CrawlBuilder setTotalMaximumConnections(int totalMaximumConnections) {
    if (totalMaximumConnections < 1) {
      throw new IllegalArgumentException(String.format("total maximum connection suppose to be at least one. %d given", totalMaximumConnections));
    }
    this.totalMaximumConnections = totalMaximumConnections;
    return this;
  }

  /**
   * @return the totalConnectionsPerHost
   */
  public int getTotalConnectionsPerHost() {
    return totalConnectionsPerHost;
  }

  /**
   * @param totalConnectionsPerHost the totalConnectionsPerHost to set
   */
  public CrawlBuilder setTotalConnectionsPerHost(int totalConnectionsPerHost) {
    if (totalConnectionsPerHost < 1) {
      throw new IllegalArgumentException(String.format("total connections per host suppose to be at least one. %d given", totalConnectionsPerHost));
    }
    this.totalConnectionsPerHost = totalConnectionsPerHost;
    return this;
  }

  /**
   * @return the setFollowRedirect
   */
  public boolean isSetFollowRedirect() {
    return setFollowRedirect;
  }

  /**
   * @param setFollowRedirect the setFollowRedirect to set
   */
  public CrawlBuilder setSetFollowRedirect(boolean setFollowRedirect) {
    this.setFollowRedirect = setFollowRedirect;
    return this;
  }

  /**
   * @return the maxFollowRedirect
   */
  public int getMaxFollowRedirect() {
    return maxFollowRedirect;
  }

  /**
   * @param maxFollowRedirect the maxFollowRedirect to set
   */
  public CrawlBuilder setMaxFollowRedirect(int maxFollowRedirect) {
    if (maxFollowRedirect < 1) {
      throw new IllegalArgumentException(String.format("max follow redirect has to be at least one. %d given", maxFollowRedirect));
    }
    this.maxFollowRedirect = maxFollowRedirect;
    return this;
  }

  /**
   * @param userAgent the user agent that this crawler will present to servers.
   * @return this builder.
   */
  public CrawlBuilder setUserAgent(String userAgent) {
    if (userAgent == null) {
      throw new NullPointerException("UserAgent is NULL.");
    }
    userAgent = userAgent.trim();
    if (userAgent.equals("")) {
      throw new IllegalArgumentException("UserAgent is an empty string");
    }
    this.userAgent = userAgent;
    return this;
  }

  /**
   * @return the currently defined user agent.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * @return the maxCrawlDepth
   */
  public int getMaxCrawlDepth() {
    return maxCrawlDepth;
  }

  /**
   * @param maxCrawlDepth the maxCrawlDepth to set
   */
  public CrawlBuilder setMaxCrawlDepth(int maxCrawlDepth) {
    if (maxCrawlDepth < 0) {
      throw new IllegalArgumentException("maxCrawlDepth should be non negative");
    }
    this.maxCrawlDepth = maxCrawlDepth;
    return this;
  }

}
