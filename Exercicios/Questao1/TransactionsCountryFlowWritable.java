package Exercicios.Questao1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransactionsCountryFlowWritable implements WritableComparable<TransactionsCountryFlowWritable> {

    private String pais;
    private String flow;

    public TransactionsCountryFlowWritable() {

    }

    public TransactionsCountryFlowWritable(String pais, String flow) {
        this.pais = pais;
        this.flow = flow;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionsCountryFlowWritable that = (TransactionsCountryFlowWritable) o;
        return Objects.equals(flow, that.flow) && Objects.equals(pais, that.pais);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, flow);
    }

    @Override
    public int compareTo(TransactionsCountryFlowWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flow);
        dataOutput.writeUTF(pais);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flow = dataInput.readUTF();
        pais = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return flow + " - " + pais + " -";
    }
}
