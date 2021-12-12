import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;


          import java.util.*;

          import org.apache.hadoop.conf.Configuration;
          import org.apache.hadoop.fs.Path;
          import org.apache.hadoop.io.DoubleWritable;
          import org.apache.hadoop.io.LongWritable;
          import org.apache.hadoop.io.Text;
          import org.apache.hadoop.mapreduce.Job;
          import org.apache.hadoop.mapreduce.Mapper;
          import org.apache.hadoop.mapreduce.Reducer;
          import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
          import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
          import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
          import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;

public class MinMax {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            double val;
            // Separating the line into 4 metrics
            String line = value.toString();
            String[] values = line.split("\t");

            // keeping track of the record read and updating it
            int record = Integer.parseInt(conf.get("record_counter"));
            int batch_unit = Integer.parseInt(conf.get("batch_unit"));


            //pick the reading depending on the requested metric
            int controller;
            switch (conf.get("workload")){
                case "CPU":
                    controller = 0;
                    break;
                case "NetworkIn":
                    controller = 1;
                    break;
                case "NetworkOut":
                    controller = 2;
                    break;
                case "Memory":
                    controller = 3;
                    break;
                default:
                    controller = 4;
            }

                // if in bound
                if ((record >= (Integer.parseInt(conf.get("batch_id")))*Integer.parseInt(conf.get("batch_unit")))&(record <= (Integer.parseInt(conf.get("batch_id"))+Integer.parseInt(conf.get("batch_size")))*Integer.parseInt(conf.get("batch_unit")))){
                    if (controller == 0)
                        val =  Double.parseDouble(values[0]);
                    else if (controller == 1)
                        val =  Double.parseDouble(values[1]);
                    else if (controller == 2)
                        val =  Double.parseDouble(values[2]);
                    else if (controller == 3)
                        val =  Double.parseDouble(values[3]);
                    else
                       val =  0;
                    //debugging purposes
                    //System.out.println("###############################################");


                    // mapping

                    context.write(new Text(String.valueOf((record-1)/batch_unit)), new DoubleWritable(val));
                }


            // updating the value of the record
            record++;
            conf.set("record_counter",""+ record);
        }
    }
    public static class MyReducer extends
            Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int batch_unit = Integer.parseInt(conf.get("batch_unit"));

            double temp,avg=0;
           // Collections.sort(values);
            List<Double> list= new ArrayList<>();
            for (DoubleWritable val : values) {
                temp = val.get();
                avg += temp;
                list.add(temp);
            }
            avg /= batch_unit;
            int length = list.size();
            Collections.sort(list);
            double std = 0;
            for (int i =0;i<length;i++){
                std += Math.pow((list.get(i)-avg),2);
            }
            std /= length;
            std = Math.sqrt(std);

            if (batch_unit%2 == 0)
                context.write(key,new Text("Avg is : " + avg +
                                                " Max is : "+ list.get(length-1) +
                                                " and Min is : "+ list.get(0) +
                                                " Median is : "+ (double)((list.get((length-1)/2)+list.get(((length-1)/2) +1))/2)+
                                                " Standard deviation is : "+ std));
            else
                context.write(key,new Text("Avg is : " + avg +
                                                 " Max is : "+ list.get(length-1) +
                                                 " and Min is : "+ list.get(0) +
                                                 " Median is : "+ (list.get((length-1)/2))+
                                                 " Standard deviation is : "+ std));

        }
    }
    public static void main(String[] args) throws Exception {

        BufferedReader inFromConsole = new BufferedReader(new InputStreamReader(System.in));
        Configuration conf = new Configuration();
        conf.set("batch_size", "3");
        conf.set("workload", "NetworkIn");
        conf.set("batch_unit", "100");
        conf.set("batch_id", "0");
        conf.set("record_counter", "-1");

        while (true) {
            System.out.print("Enter 1 for request or 2 for output : ");

            int controller = Integer.parseInt(inFromConsole.readLine());
            if (controller == 1) {


                int temp ;
                System.out.print("For the workload metric, enter 1 for CPU, 2 for NetworkIn, 3 for NetworkOut, 4 for Memory : ");
                temp = Integer.parseInt(inFromConsole.readLine());
                String Workload_Metric;
                if (temp == 1)
                    Workload_Metric = "CPU";
                else if (temp == 2)
                    Workload_Metric = "NetworkIn";
                else if (temp == 3)
                    Workload_Metric = "NetworkOut";
                else if (temp == 4)
                    Workload_Metric = "Memory";
                else {
                    System.out.println("Invalid input");
                    continue;
                }
                conf.set("workload", Workload_Metric);

                System.out.print("Enter the batch unit please : ");
                String Batch_Unit = inFromConsole.readLine();
                if (Batch_Unit.equals("0")) {
                    System.out.println("Invalid input");
                    continue;
                }
                conf.set("batch_unit", Batch_Unit);

                System.out.print("Enter the batch ID please : ");
                String Batch_ID = inFromConsole.readLine();
                conf.set("batch_id", Batch_ID);

                System.out.print("Enter the batch size please : ");
                String Batch_Size = inFromConsole.readLine();
                conf.set("batch_size", Batch_Size);

                System.out.print("For data types, enter 1 for training or 2 for testing : ");
                int temp2 = Integer.parseInt(inFromConsole.readLine());
                String Data_Type;
                Job job = Job.getInstance(conf);

                if (temp2 == 1)
                    FileInputFormat.setInputPaths(job, new Path("c:\\input\\NDBench-training.txt"));
                else if (temp2 == 2)
                    FileInputFormat.setInputPaths(job, new Path("c:\\coen\\NDBench-testing.txt"));
                else {
                    System.out.println("Invalid input");
                    continue;
                }


                job.setJarByClass(MinMax.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
               // FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[0]));
                boolean status = job.waitForCompletion(true);
                if (status) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
            }


        }






    }
}

