package DocumentProcessor;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-21
 * Time: 下午4:56
 * To change this template use File | Settings | File Templates.
 */
public interface iDocumentWordProcessor {

    public Map<String, Integer> getProcessingDocument(String document, String filter);

}
