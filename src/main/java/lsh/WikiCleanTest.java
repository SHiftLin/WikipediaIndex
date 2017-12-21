package lsh;

/**
 * Created by lsh on 12/12/2017.
 */

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

import lsh.TupleWritable;

public class WikiCleanTest {
    public static void main(String[] args) throws Exception {
/*        InputStream fin=new FileInputStream("/Users/lsh/Desktop/input.txt");
        byte[] buffer=new byte[1024*1024];
        int len=fin.read(buffer);
        System.out.println(len);
        String raw=new String(buffer,0,len);
        //WikiClean cleaner=new WikiClean.Builder().withTitle(true).build();
        //String content=cleaner.clean(raw);
        //System.out.println(raw);

        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = builderFactory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(raw));
        Document doc = builder.parse(is);
        Element page = (Element) doc.getElementsByTagName("page").item(0);
        Element id = (Element) page.getElementsByTagName("id").item(0);
        System.out.println(id.getTextContent());*/
    }
}
