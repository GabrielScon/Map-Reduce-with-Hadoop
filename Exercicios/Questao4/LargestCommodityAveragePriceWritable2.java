package Exercicios.Questao4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LargestCommodityAveragePriceWritable2 implements WritableComparable<LargestCommodityAveragePriceWritable2> {

    private String pais;
    private double quantidade;

    public LargestCommodityAveragePriceWritable2() {

    }

    public LargestCommodityAveragePriceWritable2(String pais, double quantidade) {
        this.pais = pais;
        this.quantidade = quantidade;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public double getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(double quantidade) {
        this.quantidade = quantidade;
    }

    @Override
    public int compareTo(LargestCommodityAveragePriceWritable2 o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pais);
        dataOutput.writeDouble(quantidade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pais = dataInput.readUTF();
        quantidade = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LargestCommodityAveragePriceWritable2 that = (LargestCommodityAveragePriceWritable2) o;
        return Double.compare(that.quantidade, quantidade) == 0 && Objects.equals(pais, that.pais);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, quantidade);
    }

    @Override
    public String toString() {
        return pais + " - " + quantidade + " - ";
    }
}
