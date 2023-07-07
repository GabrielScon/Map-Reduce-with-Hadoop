package Exercicios.Questao4;

//Qual é o país com o maior valor médio de transações no fluxo de exportação?

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

public class LargestCommodityAveragePriceExport {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./Output2/ex4.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Criando o primeiro job
        Job j1 = new Job(c, "LargestAverageExport1");
        j1.setJarByClass(LargestCommodityAveragePriceExport.class);
        j1.setMapperClass(MapParte1.class);
        j1.setReducerClass(ReduceParte1.class);
        j1.setCombinerClass(CombineParte1.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LargestCommodityAveragePriceWritable1.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "LargestAverageExport2");
        j2.setJarByClass(LargestCommodityAveragePriceExport.class);
        j2.setMapperClass(MapParte2.class);
        j2.setReducerClass(ReduceParte2.class);
        j2.setCombinerClass(CombineParte2.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(LargestCommodityAveragePriceWritable2.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(LargestCommodityAveragePriceWritable2.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);


        j2.waitForCompletion(false);


    }
    // Map para o job1
    //Tmp feito pela parte 1
    public static class MapParte1 extends Mapper<LongWritable, Text, Text, LargestCommodityAveragePriceWritable1> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pega a linha
            String linha = value.toString();

            // ignora o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String flow = colunas[4];

                // checando se o flow é Export
                if (flow.equals("Export")) {

                    // chave
                    String pais = colunas[0];

                    // valor
                    double valor = Double.parseDouble(colunas[5]);
                    int quantidade = 1;

                    LargestCommodityAveragePriceWritable1 valores = new LargestCommodityAveragePriceWritable1(valor, quantidade);

                    // chave e valor
                    con.write(new Text(pais), valores);
                }
            }
        }
    }
    //Combine do job1
    public static class CombineParte1 extends Reducer<Text, LargestCommodityAveragePriceWritable1, Text, LargestCommodityAveragePriceWritable1>{

        public void reduce(Text key, Iterable<LargestCommodityAveragePriceWritable1> values, Context con)
                throws IOException, InterruptedException {

            double SomaValores = 0.0;
            int SomaQuantidade = 0;

            // somando os valores e as qtds
            for (LargestCommodityAveragePriceWritable1 o : values) {
                SomaValores += o.getSomaValores();
                SomaQuantidade += o.getQuantidade();
            }

            // mandando para o reduce os valores pré-somados
            con.write(key, new LargestCommodityAveragePriceWritable1(SomaValores, SomaQuantidade));

        }
    }
    //Reduce do job1
    public static class ReduceParte1 extends Reducer<Text, LargestCommodityAveragePriceWritable1, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<LargestCommodityAveragePriceWritable1> values, Context con)
                throws IOException, InterruptedException {

            double SomaValores = 0.0;
            int SomaQuantidade = 0;

            // somando os valores e as qtds
            for (LargestCommodityAveragePriceWritable1 o : values) {
                SomaValores += o.getSomaValores();
                SomaQuantidade += o.getQuantidade();
            }

            // calculando a media
            double ResultadoMedia = SomaValores / SomaQuantidade;

            // chave e valor
            con.write(key, new DoubleWritable(ResultadoMedia));
        }
    }

    //Map para o job2
    //
    public static class MapParte2 extends Mapper<LongWritable, Text, Text, LargestCommodityAveragePriceWritable2> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Pegando uma linha
            String linha = value.toString();

            // quebrando a linha por tabs porque o arquivo é um csv
            String linhas[] = linha.split("\t");

            // valor
            String pais = linhas[0];
            double quantidade = Double.parseDouble(linhas[1]);

            // chave
            Text chave = new Text("");

            LargestCommodityAveragePriceWritable2 valor = new LargestCommodityAveragePriceWritable2(pais, quantidade);

            con.write(chave, valor);

        }
    }

    public static class CombineParte2 extends Reducer<Text, LargestCommodityAveragePriceWritable2, Text, LargestCommodityAveragePriceWritable2> {
        public void reduce(Text key, Iterable<LargestCommodityAveragePriceWritable2> values, Context con)
                throws IOException, InterruptedException {

            double maior = 0.0;
            String pais = "";

            // verificando qual país possui o maior valor
            // salvando o nome e o valor que cada país apresentou
            for (LargestCommodityAveragePriceWritable2 o : values) {
                if (o.getQuantidade() > maior) {
                    maior = o.getQuantidade();
                    pais = o.getPais();
                }
            }

            // chave e valor
            Text chave = new Text(key);
            LargestCommodityAveragePriceWritable2 valores = new LargestCommodityAveragePriceWritable2(pais, maior);

            con.write(chave, valores);
        }
    }


    public static class ReduceParte2 extends Reducer<Text, LargestCommodityAveragePriceWritable2, Text, LargestCommodityAveragePriceWritable2> {
        public void reduce(Text key, Iterable<LargestCommodityAveragePriceWritable2> values, Context con)
                throws IOException, InterruptedException {

            double maior = 0.0;
            String pais = "";

            // verifica qual país possui o maior valor
            // salva o nome e o valor que cada país apresentou
            for (LargestCommodityAveragePriceWritable2 o : values) {
                if (o.getQuantidade() > maior) {
                    maior = o.getQuantidade();
                    pais = o.getPais();
                }
            }

            // chave e valor
            Text chave = new Text(key);
            LargestCommodityAveragePriceWritable2 valores = new LargestCommodityAveragePriceWritable2(pais, maior);

            con.write(chave, valores);
        }
    }
}
