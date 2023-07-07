package Exercicios.Questao1;

//Quantas transações temos por país e por tipo de fluxo?

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class TransactionsCountryFlow {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "CountryFlow");

        // Registro de Classes
        j.setJarByClass(TransactionsCountryFlow.class);
        j.setMapperClass(MapForCountryFlow.class);
        j.setReducerClass(ReduceForCountryFlow.class);
        j.setCombinerClass(CombineForCountryFlow.class);

        // Definir os tipos de saída
        j.setMapOutputKeyClass(TransactionsCountryFlowWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(TransactionsCountryFlowWritable.class);
        j.setOutputValueClass(IntWritable.class);

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }
    // Map para filtrar o dataset e gerar a chave composta
    public static class MapForCountryFlow extends Mapper<LongWritable, Text, TransactionsCountryFlowWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Covertendo a linha inicial em uma string
            String linha = value.toString();

            // ignora o header
            if (!linha.startsWith("country_or_area;")) {

                // quebra linhas em colunas
                String colunas[] = linha.split(";");

                // pega as chaves
                String pais = colunas[0];
                String flow = colunas[4];

                // chave, valor
                TransactionsCountryFlowWritable keys = new TransactionsCountryFlowWritable(pais, flow);
                IntWritable valor = new IntWritable(1);

                con.write(keys, valor);
            }

        }
    }

    //Combiner para pré-processar os dados do map
    public static class CombineForCountryFlow extends Reducer<TransactionsCountryFlowWritable, IntWritable, TransactionsCountryFlowWritable, IntWritable> {

        private int count = 0;

        public void reduce(TransactionsCountryFlowWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int contador = 0;

            // contagem
            for (IntWritable v : values) {
                contador += v.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new IntWritable(contador));
                count++;
            }
        }
    }

    //Reduce para processar os dados e gerar o output
    public static class ReduceForCountryFlow extends Reducer<TransactionsCountryFlowWritable, IntWritable, TransactionsCountryFlowWritable, IntWritable> {

        private int count = 0;

        public void reduce(TransactionsCountryFlowWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int contador = 0;

            // contagem
            for (IntWritable i : values) {
                contador += i.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new IntWritable(contador));
                count++;
            }
        }
    }


}
