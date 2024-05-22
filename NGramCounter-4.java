import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NGramCounter {

    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();
        private int n; // N-gram size
        private int opt; // Option for context

        private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"
            // Stopwords provided by NLTK
        ));

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get the value of N from the configuration
            n = context.getConfiguration().getInt("n", 2); // Default to bigrams if not provided
            opt = context.getConfiguration().getInt("opt", 1); // Default to 1 if not provided
        }

        // Corpus cleaning
        private String cleanText(String text) {
            // Case insensitive
            text = text.toLowerCase();
            // Remove punctuation
            text = text.replaceAll("[^a-zA-Z\\s]", "");
            return text;
        }

        // Tokenization
        private List<String> tokenizeText(String text) {
            return Arrays.asList(text.split("\\s+"));
        }

        // Stopwords removal
        private List<String> removeStopwords(List<String> tokens) {
            return tokens.stream()
                         .filter(token -> !STOPWORDS.contains(token))
                         .collect(Collectors.toList());
        }

        // Open and close tags
        private String addSentenceTags(String text, int n, int opt) {
            String open;
            String close;

            if (opt == 1) {
                open = "<s> ".repeat(n);
                close = " </s>";
            } else {
                open = "<s> ".repeat(n - 1);
                close = " </s>";
            }
            return open + text + close;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Preprocessing
            String text = value.toString();
            text = cleanText(text);
            text = addSentenceTags(text, n, opt);

            List<String> tokens = tokenizeText(text);
            // tokens = removeStopwords(tokens); // Generally not useful since also stopwords can be "predicted"

            if (tokens.size() < n) {
                return; // Skip if the line has fewer words than the specified N
            }

            // Build n-grams
            for (int i = 0; i <= tokens.size() - n; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = i; j < i + n; j++) {
                    sb.append(tokens.get(j)).append(" ");
                }
                ngram.set(sb.toString().trim());
                context.write(ngram, one);
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: NGramCounter <input path> <output path> <n> <opt>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("n", Integer.parseInt(args[2]));
        conf.setInt("opt", Integer.parseInt(args[3]));
        // Set the value of N and opt from command line

        Job job = Job.getInstance(conf, "N-Gram Counter");
        job.setJarByClass(NGramCounter.class);
        job.setMapperClass(NGramMapper.class);
        job.setReducerClass(NGramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
