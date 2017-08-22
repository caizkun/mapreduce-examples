import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;

public class CellSum extends Configured implements Tool {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: <offset, line>, each line is: row:col subSum
            // output: <row:col, subSum>

            String[] cell = value.toString().trim().split("\t");
            double subSum = Double.parseDouble(cell[1]);
            context.write(new Text(cell[0]), new DoubleWritable(subSum));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // input: <row:col, (subSum1, subSum2, ...)>
            // output: <row\tcol, subSum1+subSum2+...>

            double sum = 0.0;
            for (DoubleWritable subSum : values) {
                sum += subSum.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));

            String[] rowCol = key.toString().trim().split(":");
            String outputKey = rowCol[0] + "\t" + rowCol[1];
            context.write(new Text(outputKey), new DoubleWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // create a configuration
        Configuration conf = new Configuration();

        // create a job
        Job job = Job.getInstance(conf, "Cell Sum");
        job.setJarByClass(CellSum.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CellSum(), args);
        if (exitCode == 1) {
            System.exit(exitCode);
        }
    }
}
