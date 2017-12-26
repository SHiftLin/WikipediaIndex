//https://github.com/lintool/wikiclean
package lsh;

import org.wikiclean.WikiClean;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by lsh on 12/12/2017.
 */

public class WikiCleanTest {
    public static void main(String[] args) throws Exception {
        InputStream fin=new FileInputStream("/Users/lsh/Desktop/input.txt");
        byte[] buffer=new byte[1024*1024];
        int len=fin.read(buffer);
        System.out.println(len);
        String raw=new String(buffer,0,len);
        System.out.println(raw);
        WikiClean cleaner=new WikiClean.Builder().withTitle(true).build();
        String content=cleaner.clean(raw);
        System.out.println(content);
    }
}
