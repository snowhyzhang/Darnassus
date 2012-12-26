package DocumentProcessor.DocumentWordProcessing;

import DocumentProcessor.iDocumentWordProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-21
 * Time: 下午4:51
 * To change this template use File | Settings | File Templates.
 */
public class DocumentWordProcessor implements iDocumentWordProcessor {

    private Map<String,Integer> documentMapper = null;

    public Map<String, Integer> getProcessingDocument(String document, String filter){
        documentMapper = new HashMap<String, Integer>();
        if (document.contains(filter)){
            String wordCurrent = document.substring(0, document.indexOf(filter));
            String wordNext = document.substring(document.indexOf(filter) + 1);
            addToMapper(wordCurrent);
            while (wordNext.contains(filter)){
                wordCurrent = wordNext.substring(0, wordNext.indexOf(filter));
                wordNext = wordNext.substring(wordNext.indexOf(filter) + 1);
                addToMapper(wordCurrent);
            }
            addToMapper(wordNext);
        }
        return documentMapper;
    }

    private void addToMapper(String wordCurrent) {
        if (documentMapper.containsKey(wordCurrent)){
            int wordNum = documentMapper.get(wordCurrent);
            ++wordNum;
            documentMapper.put(wordCurrent, wordNum);
        } else {
            documentMapper.put(wordCurrent, 1);
        }
    }
}
