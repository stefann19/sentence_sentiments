import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JustTheTuple implements Writable {
    private int negativeAppearances;
    private int positiveAppearances;

    public JustTheTuple() {
    }

    public JustTheTuple(int negativeAppearances, int positiveAppearances) {
        this.negativeAppearances = negativeAppearances;
        this.positiveAppearances = positiveAppearances;
    }

    public int getNegativeAppearances() {
        return negativeAppearances;
    }

    public void setNegativeAppearances(int negativeAppearances) {
        this.negativeAppearances = negativeAppearances;
    }

    public int getPositiveAppearances() {
        return positiveAppearances;
    }

    public void setPositiveAppearances(int positiveAppearances) {
        this.positiveAppearances = positiveAppearances;
    }

    @Override
    public String toString() {
        return negativeAppearances>positiveAppearances ? "negative":"positive";
    }

    @Override
    public void write(DataOutput dataOutput) throws
                                             IOException
    {
        dataOutput.writeInt(positiveAppearances);
        dataOutput.writeInt(negativeAppearances);
    }

    @Override
    public void readFields(DataInput dataInput) throws
                                                IOException
    {
        positiveAppearances = dataInput.readInt();
        negativeAppearances = dataInput.readInt();
    }
}
