/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.igalshilman.tinycrawler.crawler;

import java.net.URI;
import org.apache.lucene.document.Document;

/**
 * @author igal
 */
public final class Task {
    private final int depth;
    private final URI uri;

    private Document document;
    private String htmlContent;


    public Task(URI u,int depth) {
        this.uri = u;
        this.depth = depth;
        this.document = null;
        this.htmlContent = "";
    }

    public final URI getURI() {
        return uri;
    }

    public final int getDepth() {
        return depth;
    }

    public Document createDocument() {
        this.document = new Document();
        return this.document;
    }

    public Document getDocument() {
        return this.document;
    }

    public void setHtmlContent(String htmlContent) {
        this.htmlContent = htmlContent;
    }

    public String getHtmlContent() {
        return this.htmlContent;
    }
}
