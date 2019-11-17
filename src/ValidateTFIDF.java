import java.io.*;
import java.util.*;

/**
 * 单机版计算tf-idf，用来验证使用Hadoop的算法是否正确
 */
public class ValidateTFIDF {
    public static void main(String[] args) throws IOException {
        File file1 = new File(args[0]);
        File[] files = file1.listFiles();
        int docTotalCount = files.length;
        Map<String, Integer> authorCount = new HashMap<>();
        Map<String, Integer> wordCount = new HashMap<>();
        Map<String, Integer> docCount = new HashMap<>();
        for(File f : files) {
            Map<String, Integer> result = readFile(f.getPath(), f.getName());
            for(Map.Entry<String, Integer> entry : result.entrySet()) {
                if(wordCount.get(entry.getKey()) != null) {
                    wordCount.put(entry.getKey(), wordCount.get(entry.getKey())+entry.getValue());
                } else {
                    wordCount.put(entry.getKey(), entry.getValue());
                }
                String word = entry.getKey().split("#")[0];
                if(docCount.get(word) != null) {
                    docCount.put(word, docCount.get(word)+1);
                } else {
                    docCount.put(word, 1);
                }
            }


            String author = f.getName().split(".txt")[0];
            int totalWordCount = 0;
            for(Map.Entry<String, Integer> entry : result.entrySet()) {
                totalWordCount += entry.getValue();
            }
            if(authorCount.get(author) != null) {
                authorCount.put(author, totalWordCount + authorCount.get(author));
            } else {
                authorCount.put(author, totalWordCount);
            }
        }
        TreeMap<String, Integer> tm = new TreeMap<>(wordCount);

        File file = new File(args[1]+"/3.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        for(Map.Entry<String, Integer> entry : tm.entrySet()) {
            String[] keys = entry.getKey().split("#");
            double tf = entry.getValue()/(double)authorCount.get(keys[1]);
            double idf = Math.log10(docTotalCount/(double)(docCount.get(keys[0])+1));
            fileOutputStream.write((keys[1]+", "+ keys[0] +", TF: "+ tf + ", ").getBytes());
            fileOutputStream.write(("IDF: " + "lg(" + docTotalCount + "/" + (docCount.get(keys[0])+1) + ") = " + idf + ", TF-IDF: " + (tf*idf) + "\n").getBytes());
        }
        fileOutputStream.close();
    }

    public static Map<String, Integer> readFile(String path, String name) throws IOException {
        String author = name.split(".txt")[0];
        Map<String, Integer> result = new HashMap<>();
        FileInputStream fileInputStream = new FileInputStream(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            StringTokenizer itr = new StringTokenizer(line);
            while(itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if(result.get(word+"#"+author) != null) {
                    result.put(word+"#"+author, result.get(word+"#"+author)+1);
                } else {
                    result.put(word+"#"+author, 1);
                }
            }
        }
        fileInputStream.close();

        return result;
    }
}
