import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import scala.Tuple2;

import java.io.Serializable;

public class SparkWordCount implements Serializable {
	private static final long serialVersionUID = -6629178988243085024L;
	//定义输入文件
	private String doc = "full";
    private boolean isSelectFile = false;
    private int wordLength = 0; //0 为所有单词长度
    private static SparkConf conf = null;
    private static JavaSparkContext sc = null;
    
    private void initSpark(){
        conf = new SparkConf()
                //设置Spark应用程序的名称
                .setAppName(SparkWordCount.class.getSimpleName());
        
        conf.setMaster("local");
        sc = new JavaSparkContext(conf);
    }
    
	SparkWordCount(String doc, boolean isSelectFile, int wordLength){
		this.doc = doc;
		this.isSelectFile = isSelectFile;
		this.wordLength = wordLength;
		initSpark();
	}
	
	SparkWordCount(){
		initSpark();
	}
	
	private List<String> getSplitWords(String line){
		List<String> words = new ArrayList<String>();
		if (line == null || line.trim().length() == 0){
			return words;
		}
		
		try {
			InputStream is = new ByteArrayInputStream(line.getBytes("UTF-8"));
			IKSegmenter seg = new IKSegmenter(new InputStreamReader(is),false);
			
			Lexeme lex = seg.next();
			
			while (lex != null){
				String word = lex.getLexemeText();
				if (wordLength == 0 || word.length() == wordLength){
					words.add(word);
				}
				
				lex = seg.next();
			}
		
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return words;
	}
	
	public JavaPairRDD<String, Integer> wordCount(){
        JavaRDD<String> lines = null;
        if (isSelectFile){
        	lines = sc.textFile(doc);
        }
        else{
        	lines = sc.textFile("src/spark/text/" + doc + ".txt");
        }
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
            private static final long serialVersionUID = -3243665984299496473L;
        
			@Override
            public Iterator<String> call(String line) throws Exception {
            	return getSplitWords(line).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -7879847028195817507L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -4171349401750495688L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        
        return wordCount;
	}
	
	public JavaPairRDD<String, Integer> sortByValue(JavaPairRDD<String, Integer> wordCount, boolean isAsc){
        JavaPairRDD<Integer, String> pairs2 = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = -7879847028195817508L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> word) throws Exception {
                return new Tuple2<Integer, String>(word._2, word._1);
            }
        });
        //降序
        pairs2 = pairs2.sortByKey(isAsc);
        wordCount = pairs2.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = -7879847028195817509L;
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> word) throws Exception {
                return new Tuple2<String, Integer>(word._2, word._1);
            }
        });
        
        return wordCount;
	}
	
	public void closeSpark(JavaPairRDD<String, Integer> wordCount){
        wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            private static final long serialVersionUID = -5926812153234798612L;
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+":"+wordCount._2);
            }
        });
        wordCount.saveAsTextFile("hdfs://localhost:9000/spark");
        sc.close();
	}
	
	public static void main(String[] args) {
        SparkWordCount app = new SparkWordCount();
        
        JavaPairRDD<String, Integer> wordCount = app.wordCount();
        
        wordCount = app.sortByValue(wordCount, false);
        
        app.closeSpark(wordCount);
	}

}
