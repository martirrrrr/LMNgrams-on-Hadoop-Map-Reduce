import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

public class CPCalculator {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: CPCalculator <ngram_size>");
            System.exit(1);
        }

        int ngramSize = Integer.parseInt(args[0]);

        // input and output directories
        String hdfsInputPathNgrams = "hdfs://cluster-c715-m:8020/user/MapReduce/input/Ngrams/ngrams.txt";
        String hdfsInputPathNMinusOneGrams = "hdfs://cluster-c715-m:8020/user/MapReduce/input/Ngrams/n-1grams.txt";
        String hdfsOutputPath = "hdfs://cluster-c715-m:8020/user/MapReduce/input/Ngrams/probability.txt";

        System.out.println("input n-grams path: " + hdfsInputPathNgrams);
        System.out.println("input n-1grams path: " + hdfsInputPathNMinusOneGrams);
        System.out.println("output path: " + hdfsOutputPath);
        System.out.println("n-gram size: " + ngramSize);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Map<String, Integer> ngramCounts = new HashMap<>();
        Map<String, Integer> nMinusOneGramCounts = new HashMap<>();

        // 
        try (FSDataInputStream inputStream = fs.open(new Path(hdfsInputPathNgrams));
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                int count = Integer.parseInt(parts[parts.length - 1]);
                String[] words = Arrays.copyOfRange(parts, 0, parts.length - 1);

                String ngram = String.join(" ", words);
                if (words.length == ngramSize) {
                    ngramCounts.put(ngram, count);
                }
            }
        }

        // Leggi il file degli n-1grammi e calcola la frequenza degli n-1grammi
        try (FSDataInputStream inputStream = fs.open(new Path(hdfsInputPathNMinusOneGrams));
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\s+");
                int count = Integer.parseInt(parts[parts.length - 1]);
                String[] words = Arrays.copyOfRange(parts, 0, parts.length - 1);

                String nMinusOneGram = String.join(" ", words);
                nMinusOneGramCounts.put(nMinusOneGram, count);
            }
        }

        // Calcola la probabilità condizionata e scrivi i risultati nel file di output
        Path outputPath = new Path(hdfsOutputPath);
        FSDataOutputStream outputStream = fs.create(outputPath);

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            for (Map.Entry<String, Integer> entry : ngramCounts.entrySet()) {
                String ngram = entry.getKey();
                int ngramFreq = entry.getValue();

                String[] words = ngram.split("\\s+");
                if (words.length == ngramSize) {
                    String nMinusOneGram = String.join(" ", Arrays.copyOfRange(words, 0, words.length - 1));
                    int nMinusOneGramFreq = nMinusOneGramCounts.getOrDefault(nMinusOneGram, 0);

                    float conditionalProbability = (float) ((ngramFreq + 0.1) / (nMinusOneGramFreq + 0.1 * nMinusOneGramCounts.size()));

                    // Scrivi sul file di output
                    String outputLine = ngram + " " + conditionalProbability + "\n";
                    writer.write(outputLine);
                }
            }
        }
    }
}
