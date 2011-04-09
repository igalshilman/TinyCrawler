/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tinycrawler.crawler;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 *
 * @author igal
 */
public class DocumentFetcher {
    private final class RequestCallBack extends AsyncCompletionHandler<Boolean> {
        @Override
        public Boolean onCompleted(Response res) throws Exception {
            if (res.hasResponseBody()) {
                Document document = new Document();
                document.add(new Field("uri", res.getUri().toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
                document.add(new Field("html-content", res.getResponseBody(), Field.Store.YES, Field.Index.NO));

                logger.log(Level.INFO, "Adding document to docQueue {0}", res.getUri());
                docQueue.add(document);
                return Boolean.TRUE;
            }
            
            // We should log that we weren't fetch this document.
            logger.log(Level.INFO, "Found URI without a body [{0}]", res.getUri());
            return Boolean.FALSE;
        }
    }

    private static final Logger logger = Logger.getLogger("TinyCrawler");
    private final BlockingQueue<Document> docQueue;
    private final URIQueue uriQueue;
    private final AsyncHttpClientConfig clientConfig;

    public DocumentFetcher(BlockingQueue<Document> docQueues,
            URIQueue uriQueue,
            AsyncHttpClientConfig clientConfig) {

        this.docQueue = docQueues;
        this.uriQueue = uriQueue;
        this.clientConfig = clientConfig;
    }

    public void fetch() {
        final AsyncHttpClient client = new AsyncHttpClient(clientConfig);
        int docsFetched = 0;

        while (docsFetched++ < 100) {
            try {
                final URI uri = uriQueue.takeOne();
                final String address = uri.toString();
                final RequestCallBack cb = new RequestCallBack();

                logger.log(Level.INFO, "Fetching {0}",address);
                client.prepareGet(address).execute(cb);

            } catch (InterruptedException ex) {
                break;
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            } catch (IllegalArgumentException ex) {
               logger.log(Level.SEVERE, null, ex);
            }
        }
        logger.log(Level.INFO, "DocumentFetcher is done.");
    }
}
