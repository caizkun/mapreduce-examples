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

    public static class FirstMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class SecondMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: <offset, line>, each line is: row col element
            // output: <row, col:element>

            String[] cell = value.toString().trim().split("\t");
            String row = cell[0];
            String colVal = cell[1] + ":" + cell[2];
            context.write(new Text(row), new Text(colVal));
        }
    }

    public static class CellReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // The input aggregate the data with the same key from the two mappers
            // input: <j, (i1=M1[i1][j], i2=M1[i2][j], ..., j1:M2[j][j1], j2:M2[j][j2], ...)>
            // output: <i1:j2, M1[i1][j] * M2[j][j2]>

            // separate the input from matrix1 and matrix2
            List<String> matrix1Col = new ArrayList<String>();
            List<String> matrix2Row = new ArrayList<String>();
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains("=")) {
                    matrix1Col.add(value);
                } else {
                    matrix2Row.add(value);
                }
            }

            // generate all pairs of (row from matrix1, col from matrix2)
            for (String m1Col : matrix1Col) {
                String row = m1Col.split("=")[0];
                double element1 = Double.parseDouble(m1Col.split("=")[1]);

                for (String m2Row : matrix2Row) {
                    String col = m2Row.split(":")[0];
                    double element2 = Double.parseDouble(m2Row.split(":")[1]);

                    String outputKey = row + ":" + col;
                    double outputValue = element1 * element2;
                    context.write(new Text(outputKey), new Text(String.valueOf(outputValue)));
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // create a configuration
        Configuration conf = new Configuration();

        // create a job
        Job job = Job.getInstance(conf, "Cell Multiplication");
        job.setJarByClass(CellMultiplication.class);
        job.setReducerClass(CellReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Multiple input paths: one for each Mapper
        // No need to use job.setMapperClass()
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FirstMatrixMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SecondMatrixMapper.class);

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
