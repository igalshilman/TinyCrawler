/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.igalshilman.tinycrawler.crawler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.apache.lucene.index.CorruptIndexException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author igal
 */
public class DocumentProcessor implements Runnable {
    private static final Logger logger = Logger.getLogger("TinyCrawler");
    private final Pattern NON_EMPTY_PAT = Pattern.compile("(\\w)+");
    private final TaskQueue taskQueue;
    private final IndexWriter indexWriter;
    private final int maxCrawlDepth;

    public DocumentProcessor(Crawler crawler) {
        this.taskQueue = crawler.getTaskQueue();
        this.indexWriter = crawler.getIndexWriter();
        this.maxCrawlDepth = crawler.getMaxCrawlDepth();
    }

    public void run() {
        for (;;) {
            Task task = null;
            try {
                task = taskQueue.takeProcessingTask();
            } catch (InterruptedException ex) {
               logger.log(Level.INFO,"DocumentProcessor is done.");
               return ;
            }
            logger.log(Level.INFO, "Document found [{0}]\n", task.getURI());
            process(task);
        }
    }

    public void process(Task task) {
        final String baseURI = task.getURI().toString();
        final String html = task.getHtmlContent();

        // We don't need the raw html content anymore.
        task.setHtmlContent("");

        // Parse the HTML from this document and convert it to DOM tree.
        org.jsoup.nodes.Document root = Jsoup.parse(html, baseURI);

        // Here we have the DOM element of that page for our disposal.
        // This is a good place to do whatever we want with it, such as:
        // - build keyword histogram (for indexing)
        // - serialize the page as DOM.
        // - filtering, simularity check etc'.
      

        // Extract plain text out of the html.
        String plainText = extractPlainText(root);
        Document doc = task.createDocument();
        doc.add(new Field("uri", baseURI , Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("content", plainText, Field.Store.YES, Field.Index.ANALYZED));

        try {
            indexWriter.addDocument(doc);
        } catch (CorruptIndexException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        if (task.getDepth() < maxCrawlDepth) {
            // add the new links back to the uriQueue.
            List<Task> newTasks = extractURIs(root, task.getDepth() + 1);
            taskQueue.addDownloadTasks(newTasks);
        }
        
        taskQueue.decrementPendingProcessing();
    }

    public String extractPlainText(org.jsoup.nodes.Document root) {
        Elements textElements = root.getElementsMatchingOwnText(NON_EMPTY_PAT);
        StringBuilder sb = new StringBuilder();

        for (Element e : textElements) {
            sb.append(e.ownText());
            sb.append(" ");
        }

        return sb.toString();
    }

    private ArrayList<Task> extractURIs(org.jsoup.nodes.Document root, int depth) {
        // Transform the links in this doc to a list of URIs.
        Elements links = root.select("a[href]");
        ArrayList<Task> newLinks = new ArrayList<Task>(links.size());

        for (Element link : links) {
            try {
                String url = link.absUrl("href").trim();
                if (url.equals("")) {
                    continue;
                }
                newLinks.add(new Task(new URI(url),depth));
            } catch (URISyntaxException ex) {
                // silently drop bad links.
            }
        }
        return newLinks;
    }
}
