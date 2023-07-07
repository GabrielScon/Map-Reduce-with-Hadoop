package Exercicios.Questao3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MostCommercializedWritableVal implements WritableComparable<MostCommercializedWritableVal> {

    private String commodity;
    private double quantidade;

    public MostCommercializedWritableVal() {

    }

    public MostCommercializedWritableVal(String commodity, double quantidade) {
        this.commodity = commodity;
        this.quantidade = quantidade;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String comm) {
        this.commodity = comm;
    }

    public double getQuantidade() {
        return quantidade;
    }

    public void setQuantidade(double qtd) {
        this.quantidade = quantidade;
    }

    @Override
    public int compareTo(MostCommercializedWritableVal o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeDouble(quantidade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        quantidade = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MostCommercializedWritableVal that = (MostCommercializedWritableVal) o;
        return Double.compare(that.quantidade, quantidade) == 0 && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, quantidade);
    }

    @Override
    public String toString() {
        return commodity + '\t' + quantidade + '\t';
    }
}
