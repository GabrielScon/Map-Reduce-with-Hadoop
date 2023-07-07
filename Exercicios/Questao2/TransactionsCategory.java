package Exercicios.Questao2;

//Qual é o valor médio das transações por categoria?

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsCategory {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // Cria o job e define seu nome
        Job j = new Job(c, "TransactionsCat");

        // Registrar as classes
        j.setJarByClass(TransactionsCategory.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definir os tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TransactionsCategoryWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // inicia o job
        j.waitForCompletion(false);
    }

    // Map para filtrar o dataset e gerar a chave composta
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, TransactionsCategoryWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtem a linha
            String linha = value.toString();

            // ignora o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                // quebra linha em colunas
                String colunas[] = linha.split(";");

                // chave
                String category = colunas[9];

                // valor
                double valor = Double.parseDouble(colunas[5]);
                int quantidade = 1;

                // enviando dados no formato (chave,valor) para o reduce
                con.write(new Text(category),
                        new TransactionsCategoryWritable(valor, quantidade));
            }
        }
    }
    //Combiner para pré-processar os dados do map
    public static class CombineForAverage extends Reducer<Text, TransactionsCategoryWritable, Text, TransactionsCategoryWritable>{

        public void reduce(Text key, Iterable<TransactionsCategoryWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar os valores e as quantidades
            double SomaValores = 0;
            int SomaQuantidades = 0;
            for(TransactionsCategoryWritable o : values){
                SomaValores += o.getSomaValores();
                    SomaQuantidades += o.getQuantidades();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new TransactionsCategoryWritable(SomaValores, SomaQuantidades));
        }
    }
    //Reduce para processar os dados do combiner
    public static class ReduceForAverage extends Reducer<Text, TransactionsCategoryWritable, Text, DoubleWritable> {

        private int count = 0;

        public void reduce(Text key, Iterable<TransactionsCategoryWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar os valores e as quantidades
            double somaValores = 0;
            int somaQuantidades = 0;
            for (TransactionsCategoryWritable o : values){
                somaValores += o.getSomaValores();
                somaQuantidades += o.getQuantidades();
            }

            // calcular a media
            double media = somaValores / somaQuantidades;

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new DoubleWritable(media));
                count++;
            }
        }
    }
}
