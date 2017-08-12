import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*
 * This driver implements the Tool interface (which calls GenericOptionsParser)
 * to help interpret common Hadoop command-line options.
 *
 * All implementations of Tool need to implement Configurable (since Tool extends it),
 * and the easiest way to do this is subclassing Configured.
 *
 * ToolRunner is a utility to help run Tool. It can be used to run classes implementing Tool interface.
 * It works in conjunction with GenericOptionsParser to parse the generic hadoop command line
 * arguments and modifies the Configuration of the Tool. The application-specific options are passed along
 * without being modified.
 *
 *
 * Although it is unnecessary in this example, this driver format is very useful for customizing configuration
 * at run time, not compile time!
 *
 */

public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // check the run parameters
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // create a configuration
        Configuration conf = getConf();

        // instantiate a job
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // specify io
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // use ToolRunner to run Tool
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
