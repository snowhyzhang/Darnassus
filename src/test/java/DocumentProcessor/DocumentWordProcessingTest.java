package DocumentProcessor;

import DocumentProcessor.DocumentWordProcessing.DocumentWordProcessor;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-24
 * Time: 下午4:06
 * To change this template use File | Settings | File Templates.
 */
public class DocumentWordProcessingTest extends TestCase {
    public void testDocumentWordProcessor(){
        String testStr = "leo|snow|5dx|snow|love";
        iDocumentWordProcessor dwp = new DocumentWordProcessor();
        String filter = "|";
        Map<String, Integer> wordMapper = dwp.getProcessingDocument(testStr, filter);

        assertEquals((int)wordMapper.get("snow"), 2);
        assertEquals((int)wordMapper.get("love"), 1);
        assertEquals((int)wordMapper.get("leo"), 1);
    }
}
