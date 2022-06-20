package data;

import org.apache.spark.sql.Row;

import scala.Serializable;

public class IncomeInfo implements Serializable {
    private static final long serialVersionUID = -26854218382696366L;

    private Double population;
    private Double rfd;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public Double getPopulation() {
        return population;
    }

    public void setPopulation(Double population) {
        this.population = population;
    }

    public Double getRfd() {
        return rfd;
    }

    public void setRfd(Double rfd) {
        this.rfd = rfd;
    }

    public IncomeInfo add(IncomeInfo other) {
        IncomeInfo result = new IncomeInfo();
        result.setPopulation(this.getPopulation() + other.getPopulation());
        result.setRfd(this.getRfd() + other.getRfd());
        return result;
    }

    public IncomeInfo divide(Integer divisor) {
        IncomeInfo result = new IncomeInfo();
        result.setPopulation(this.getPopulation() / divisor);
        result.setRfd(this.getRfd() / divisor);
        return result;
    }

    public IncomeInfo () { }

    public IncomeInfo(Row row) {
        // We don't need the year, so we'll skip it.
        // this.year = row.getInt(0);

        this.population = (double) row.getInt(1);
        this.rfd = row.getDouble(2);
    }
}
