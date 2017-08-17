import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CellMultiplication extends Configured implements Tool {

    public static class MatrixReaderMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: <offset, line>, each line is: row col element
            // output: <col, row=element>

            String[] cell = value.toString().trim().split("\t");
            String col = cell[1];
            String rowVal = cell[0] + "=" + cell[2];
            context.write(new Text(col), new Text(rowVal));
        }
    }

    public static class VectorReaderMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: <offset, line>, each line is: row element
            // output: <row, element>

            String[] cell = value.toString().trim().split("\t");
            context.write(new Text(cell[0]), new Text(cell[1]));
        }
    }

    public static class CellReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input: <col, row1=element1, row2=element2, ..., v>
            // output: <row, element * v>

            double vecCell = 0.0;
            List<String> matrixRow = new ArrayList<String>();
            for (Text value : values) {
                String val = value.toString();
                if (val.contains("=")) {
                    matrixRow.add(val);
                } else {
                    vecCell = Double.parseDouble(val);
                }
            }

            for (String rowVal : matrixRow) {
                String row = rowVal.split("=")[0];
                double value = Double.parseDouble(rowVal.split("=")[1]) * vecCell;
                context.write(new Text(row), new Text(String.valueOf(value)));
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Cell Multiplication");
        job.setJarByClass(CellMultiplication.class);
        job.setReducerClass(CellReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Multiple input paths: one for each Mapper
        // No need to use job.setMapperClass()
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MatrixReaderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, VectorReaderMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CellMultiplication(), args);
        if (exitCode == 1) {
            System.exit(exitCode);
        }
    }
}
