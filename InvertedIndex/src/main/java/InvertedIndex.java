import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input: <offset, line>
            // output: <word, fileName>

            // split this record into words
            String[] words = value.toString().trim().split("\\s+");

            // fetch the file name of this record
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            for (String word : words) {
                context.write(new Text(word.toLowerCase()), new Text(fileName));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input: <word, (file1, file2, file1, ...)>
            // output: <word, {file1=count1, file2=count2, ...)>

            Map<String, Integer> hist = new HashMap<String, Integer>();

            for (Text value : values) {
                String fileName = value.toString();

                if (hist.containsKey(fileName)) {
                    hist.put(fileName, hist.get(fileName) + 1);
                } else {
                    hist.put(fileName, 1);
                }
            }

            context.write(key, new Text(hist.toString()));
        }
    }

}
