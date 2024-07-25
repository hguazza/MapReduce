package com.pucpr.implementacaomapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Info6 {
    
    public static class MapperImplementacaoATP5 extends Mapper<Object, Text, Text, DoubleWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            
            // Ensure the line has the correct number of fields and filter by year and location (Brasil)
            if(campos.length == 10 && campos[1].equals("2016") && campos[4].equalsIgnoreCase("Brasil")) {                   
                String mercadoria = campos[3]; 
                String preco = campos[5];
                DoubleWritable valorMap = new DoubleWritable(0);
                Text chaveMap = new Text(mercadoria);
                
                try {
                    valorMap = new DoubleWritable(Double.parseDouble(preco));
                } catch(NumberFormatException e) {
                    // Handle the exception if needed
                }
                
                context.write(chaveMap, valorMap);
            }
        }
    }
    
    public static class ReducerImplementacaoATP5 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        
        @Override
        public void reduce(Text chave, Iterable<DoubleWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
            for(DoubleWritable val : valores) {
                soma += val.get();
            }
            DoubleWritable valorSaida = new DoubleWritable(soma);
            context.write(chave, valorSaida);
            System.out.printf("%s  %s\n", chave, valorSaida);
        }
    }
                       
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/henrique.guazzelli/Desktop/ATP/Informacao5";
        
        if(args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade5ATP");
        job.setJarByClass(Informacao5.class);
        job.setMapperClass(MapperImplementacaoATP5.class);
        job.setReducerClass(ReducerImplementacaoATP5.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true); 
    }
}
