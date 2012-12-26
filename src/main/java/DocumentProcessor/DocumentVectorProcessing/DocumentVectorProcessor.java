package DocumentProcessor.DocumentVectorProcessing;

import DocumentProcessor.iDocumentProcessor;
import Util.Props;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-24
 * Time: 下午3:31
 * To change this template use File | Settings | File Templates.
 */
public class DocumentVectorProcessor {

    public static void main(String args[]){
        DocumentVectorProcessor dvp = new DocumentVectorProcessor();
        if (args.length > 0){
            dvp.start(args[1]);
        } else {
            dvp.start(null);
        }
    }

    private void start(String docQueue) {
        String documentQueue;
        if (docQueue != null){
            documentQueue = docQueue;
        } else {
            documentQueue = Props.getProperty("ProcessingQueue0");
        }
        iDocumentProcessor documentProcessor = new DocumentVectorProcessing();
        documentProcessor.processDocument(documentQueue);
    }
}
