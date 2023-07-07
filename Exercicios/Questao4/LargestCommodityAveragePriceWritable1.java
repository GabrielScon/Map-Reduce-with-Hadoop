package Exercicios.Questao4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LargestCommodityAveragePriceWritable1 implements WritableComparable<LargestCommodityAveragePriceWritable1> {

    private double somaValores;
    private int quantidade;

    public LargestCommodityAveragePriceWritable1() {

    }

    public LargestCommodityAveragePriceWritable1(double somaValores, int quantidade) {
        this.somaValores = somaValores;
        this.quantidade = quantidade;
    }

    public double getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(double somaValores) {
        this.somaValores = somaValores;
    }

    public int getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(int quantidade) {
        this.quantidade = quantidade;
    }

    @Override
    public int compareTo(LargestCommodityAveragePriceWritable1 o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(somaValores);
        dataOutput.writeInt(quantidade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaValores = dataInput.readDouble();
        quantidade = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LargestCommodityAveragePriceWritable1 that = (LargestCommodityAveragePriceWritable1) o;
        return Double.compare(that.somaValores, somaValores) == 0 && quantidade == that.quantidade;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValores, quantidade);
    }
}
