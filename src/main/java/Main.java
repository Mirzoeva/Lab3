import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: JoinJob <input path> <input path> <output path>");
            System.exit(-1);
        }
        


        //"Hello World! QQQQQQQQ"
        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        job.setJobName("Main sort");
//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperAirports.class);
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperFlights.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//        job.setPartitionerClass(PartitionerAirports.class);
//        job.setGroupingComparatorClass(ComparatorAirports.class);
//        job.setReducerClass(JoinReducer.class);
//        job.setMapOutputKeyClass(TextPair.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
