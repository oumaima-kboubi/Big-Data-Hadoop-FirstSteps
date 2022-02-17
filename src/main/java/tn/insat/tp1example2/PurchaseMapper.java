package tn.insat.tp1example2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class PurchaseMapper extends Mapper<Object, Text,Text, DoubleWritable> {
    private DoubleWritable priceWritable = new DoubleWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] words = row.split("\t");
        String magasin = words[2];
        double price = Double.parseDouble(words[4]);
        word.set(magasin);
        priceWritable.set(price);
        context.write(word, priceWritable);
    }
}
