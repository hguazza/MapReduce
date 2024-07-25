/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

/**
 *
 * @author henrique.guazzelli
 * 
 */
public class Info5 {
    
    public static class MapperImplementacaoATP5 extends Mapper<Object, Text, Text, DoubleWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            /* campos.length deve ser igual ao nº de colunas, isso ocorre pq o HDFS pode quebrar linhas do database*/
            if(campos.length == 10 && campos[1].equals("2016")){                   
                String mercadoria = campos[3]; 
                String preco = campos[5];
                DoubleWritable valorMap = new DoubleWritable(0);
                Text chaveMap = new Text(mercadoria);
                try{
                    valorMap = new DoubleWritable(Double.parseDouble(preco));
                }catch(NumberFormatException e){
                    
                }finally{
                    
                }
                
                context.write(chaveMap, valorMap);
                
            }
        }
    }
    
        
        public static class ReducerImplementacaoATP5 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
            
            @Override
            public void reduce(Text chave, Iterable<DoubleWritable> valores, Context context) throws IOException, InterruptedException{
                int soma = 0;
                for(DoubleWritable val : valores){
                    soma += val.get();
                }
                DoubleWritable valorSaida = new DoubleWritable(soma);
                context.write(chave, valorSaida);
                System.out.printf("%s  %s\n", chave, valorSaida);
            }
        }
                       
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
     
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/henrique.guazzelli/Desktop/ATP/Informacao5";
        
        /* se estiver passando 2 parametros, entao estamos escrevendo no HDFS */
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade5ATP");
        job.setJarByClass(Informacao5.class);
        job.setMapperClass(MapperImplementacaoATP5.class);
        job.setReducerClass(ReducerImplementacaoATP5.class);
        
        /* ajustando formato de saida da chave e valor */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        /* definindo arquivos de entrada e saída */
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true); 
   }
    
}