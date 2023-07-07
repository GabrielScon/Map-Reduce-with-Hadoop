package Exercicios.Questao2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionsCategoryWritable
        implements WritableComparable<TransactionsCategoryWritable> {

    // Atributos privados
    private double somaValores;
    private int quantidades;

    // Construtor vazio
    public TransactionsCategoryWritable() {
    }

    public TransactionsCategoryWritable(Double somaValores, int quantidades) {
        this.somaValores = somaValores;
        this.quantidades = quantidades;
    }

    // gets e sets de todos os atributos
    public Double getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(Double somaValores) {
        this.somaValores = somaValores;
    }

    public int getQuantidades() {
        return quantidades;
    }

    public void setQuantidades(int quantidades) {
        this.quantidades = quantidades;
    }

    @Override
    public int compareTo(TransactionsCategoryWritable o) {
        // manter essa implemementacao independentemente da classe e dos atributos
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeDouble(somaValores);
        dataOutput.writeInt(quantidades);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        somaValores = dataInput.readDouble();
        quantidades = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionsCategoryWritable that = (TransactionsCategoryWritable) o;
        return Double.compare(that.somaValores, somaValores) == 0 && quantidades == that.quantidades;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValores, quantidades);
    }
}
