package Exercicios.Questao3;

//Qual é a commodity mais comercializada, ao somar as quantidades (amount) em
//2018 e no fluxo de importação?

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


public class MostCommercialized {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        //Caminho para o tmp
        Path intermediate = new Path("./Output2/ex3.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Criando o primeiro job
        Job j1 = new Job(c, "MostCommercialized1");
        j1.setJarByClass(MostCommercialized.class);
        j1.setMapperClass(MapParte1.class);
        j1.setReducerClass(ReduceParte1.class);
        j1.setCombinerClass(CombineParte1.class);
        j1.setMapOutputKeyClass(MostCommercializedWritable.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(MostCommercializedWritable.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Para rodar o job 1
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "MostCommercialized2");
        j2.setJarByClass(MostCommercialized.class);
        j2.setMapperClass(MapParte2.class);
        j2.setReducerClass(ReduceParte2.class);
        j2.setCombinerClass(CombineParte2.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(MostCommercializedWritableVal.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(MostCommercializedWritableVal.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // Para rodar o job 2
        j2.waitForCompletion(false);


    }

    // Map para o primeiro job
    public static class MapParte1 extends Mapper<LongWritable, Text, MostCommercializedWritable, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Pega a linha
            String linha = value.toString();

            // ignora o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String ano = colunas[1];

                // chave
                String flow = colunas[4];
                // verifica se o ano é 2018 e o flow é do tipo import
                if (ano.equals("2018") && flow.equals("Import")){

                    // chave
                    String commodity = colunas[3];
                    // valor
                    double amount = Double.parseDouble(colunas[8]);

                    MostCommercializedWritable chaves = new MostCommercializedWritable(flow, commodity);
                    DoubleWritable valor = new DoubleWritable(amount);

                    con.write(chaves, valor);
                }
            }
        }
    }

    public static class CombineParte1 extends Reducer<MostCommercializedWritable, DoubleWritable, MostCommercializedWritable, DoubleWritable> {
        public void reduce(MostCommercializedWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double sum = 0.0;

            // somando a quantidade
            for (DoubleWritable d : values) {
                sum += d.get();
            }

            // escrevendo o arquivo de resultados
            con.write(key, new DoubleWritable(sum));
        }
    }

    public static class ReduceParte1 extends Reducer<MostCommercializedWritable, DoubleWritable, MostCommercializedWritable, DoubleWritable> {
        public void reduce(MostCommercializedWritable key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double sum = 0.0;

            // somando a quantidade
            for (DoubleWritable d : values) {
                sum += d.get();
            }

            // escrevendo o arquivo de resultados
            con.write(key, new DoubleWritable(sum));
        }
    }


    public static class MapParte2 extends Mapper<LongWritable, Text, Text, MostCommercializedWritableVal> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Pega uma linha
            String linha = value.toString();

            // quebrando a linha por tabs por que o arquivo é um csv
            String linhas[] = linha.split("\t");

            // chave
            String flow = linhas[0];

            // valor
            String commodity = linhas[1];
            double quantidade = Double.parseDouble(linhas[2]);

            Text chave = new Text(flow);
            MostCommercializedWritableVal valores = new MostCommercializedWritableVal(commodity, quantidade);

            con.write(chave, valores);

        }
    }

    public static class CombineParte2 extends Reducer<Text, MostCommercializedWritableVal, Text, MostCommercializedWritableVal> {
        public void reduce(Text key, Iterable<MostCommercializedWritableVal> values, Context con)
                throws IOException, InterruptedException {

            double maior = 0.0;
            String commodity = "";

            // verificando qual commodity tem o maior valor
            // salvando o nome e o valor de cada commodity
            for (MostCommercializedWritableVal c : values) {
                if (c.getQuantidade() > maior) {
                    maior = c.getQuantidade();
                    commodity = c.getCommodity();
                }
            }

            MostCommercializedWritableVal valores = new MostCommercializedWritableVal(commodity, maior);

            con.write(key, valores);
        }
    }

    public static class ReduceParte2 extends Reducer<Text, MostCommercializedWritableVal, Text, MostCommercializedWritableVal> {
        public void reduce(Text key, Iterable<MostCommercializedWritableVal> values, Context con)
                throws IOException, InterruptedException {

            double maior = 0.0;
            String commodity = "";

            // verificando qual commodity tem o maior valor
            // salvando o nome e o valor de cada commodity
            for (MostCommercializedWritableVal c : values) {
                if (c.getQuantidade() > maior) {
                    maior = c.getQuantidade();
                    commodity = c.getCommodity();
                }

            }

            MostCommercializedWritableVal valores = new MostCommercializedWritableVal(commodity, maior);

            con.write(key, valores);
        }
    }
}