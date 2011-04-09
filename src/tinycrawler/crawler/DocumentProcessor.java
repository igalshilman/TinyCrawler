/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler.crawler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
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
    private final BlockingQueue<Document> docq;
    private final URIQueue uriq;
    private final IndexWriter indexWriter;

    public DocumentProcessor(BlockingQueue<Document> documentQueue,
            URIQueue uriQueue,
            IndexWriter indexWriter) {

        this.docq = documentQueue;
        this.uriq = uriQueue;
        this.indexWriter = indexWriter;
    }

    public void run() {
        for (;;) {
            Document doc = null;
            try {
                doc = docq.poll(1, TimeUnit.MINUTES);

            } catch (InterruptedException ex) {
               logger.log(Level.INFO,"DocumentProcessor is done.");
               return ;
            }
            if (doc != null) {
                logger.log(Level.INFO, "Document found [{0}]\n", doc.get("uri"));
                process(doc);
            }
        }
    }

    public void process(Document doc) {
        final String baseURI = doc.get("uri");
        final String html = doc.get("html-content");

        // We don't need the raw html content anymore.
        doc.removeField("html-content");

        // Parse the HTML from this document and convert it to DOM tree.
        org.jsoup.nodes.Document root = Jsoup.parse(html, baseURI);

        // Here we have the DOM element of that page for our disposal.
        // This is a good place to do whatever we want with it, such as:
        // - build keyword histogram (for indexing)
        // - serialize the page as DOM.
        // - filtering, simularity check etc'.
      

        // Extract plain text out of the html.
        String plainText = extractPlainText(root);
        doc.add(new Field("content", plainText, Field.Store.YES, Field.Index.ANALYZED));

        try {
            indexWriter.addDocument(doc);
        } catch (CorruptIndexException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        // add the new links back to the uriQueue.
        List<URI> outLinks = extractURIs(root);
        uriq.addAll(outLinks);
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

    private ArrayList<URI> extractURIs(org.jsoup.nodes.Document root) {
        // Transform the links in this doc to a list of URIs.
        Elements links = root.select("a[href]");
        ArrayList<URI> newLinks = new ArrayList<URI>(links.size());

        for (Element link : links) {
            try {
                String url = link.absUrl("href").trim();
                if (url.equals("")) {
                    continue;
                }
                newLinks.add(new URI(url));
            } catch (URISyntaxException ex) {
                // silently drop bad links.
            }
        }
        return newLinks;
    }
}
