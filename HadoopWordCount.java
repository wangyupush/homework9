import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class HadoopWordCount {

    private static SortableMap<Integer> totalWords = new SortableMap<Integer>();
    
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@SuppressWarnings("unused")
		private String pattern = "[^//w]"; // 正则表达式，代表不是0-9, a-z,
											

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			
			try {
				InputStream is = new ByteArrayInputStream(
						line.getBytes("UTF-8"));
				IKSegmenter seg = new IKSegmenter(new InputStreamReader(is),
						false);

				Lexeme lex = seg.next();

				while (lex != null) {
					String text = lex.getLexemeText();
					word.set(text);
					context.write(word, one);
					// output.collect(word, one);
					
					lex = seg.next();
				}

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			//保存结果
			totalWords.put(key.toString(), Integer.valueOf(sum));
		}
	}

	private static class IntWritableDecreasingComparator extends
			IntWritable.Comparator {
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static Map<String, Integer> wordCount(String doc, boolean isSelectFile, int wordLength) throws IOException{
		Configuration conf = new Configuration();
		Path inputFile; //word count job input
		if (isSelectFile){
			inputFile = new Path(doc);
		}
		else{
			inputFile = new Path("src/spark/text/" + doc + ".txt"); 
		}
        FileUtil.deleteFolder("D:\\hadoop");
        FileUtil.deleteFolder("D:\\hadoopsort");
		Path outputFolder = new Path("D:\\hadoop");//word count job output,sort job input
		Path sortOutput = new Path("D:\\hadoopsort");//sort job output
		
		try {
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(HadoopWordCount.class);
			
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, outputFolder);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = Job.getInstance(conf, "sort");
				sortJob.setJarByClass(HadoopWordCount.class);

				FileInputFormat.addInputPath(sortJob, outputFolder);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				sortJob.setMapperClass(InverseMapper.class);
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, sortOutput);

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

			    if (sortJob.waitForCompletion(true)){
			    	if (wordLength == 0){
			    		return totalWords.sortMapByValue(false);
			    	}
			    	else{
			    		SortableMap<Integer> words = new SortableMap<Integer>();
			    		for(String key:totalWords.keySet()){
			    			if (key.length() == wordLength){
			    				words.put(key, totalWords.get(key));
			    			}
			    		}
			    		
			    		return words.sortMapByValue(false);
			    	}
			    }
			}
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		} finally {
			FileSystem.get(conf).deleteOnExit(outputFolder);
		}
		
		return null;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputFile = new Path("src/spark/text/full.txt");
		Path outputFolder = new Path("F:\\text\\hadoop");//word count job output,sort job input
		Path sortOutput = new Path("F:\\text\\hadoopsort");//sort job output
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(HadoopWordCount.class);
		try {
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, outputFolder);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if (job.waitForCompletion(true)) {
				Job sortJob = Job.getInstance(conf, "sort");
				sortJob.setJarByClass(HadoopWordCount.class);

				FileInputFormat.addInputPath(sortJob, outputFolder);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);

				sortJob.setMapperClass(InverseMapper.class);
				sortJob.setNumReduceTasks(1);
				FileOutputFormat.setOutputPath(sortJob, sortOutput);

				sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

			    System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		} finally {
			FileSystem.get(conf).deleteOnExit(outputFolder);
		}
	}
}
