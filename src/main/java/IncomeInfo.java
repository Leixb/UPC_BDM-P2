import org.apache.spark.sql.Row;

import scala.Serializable;

public class IncomeInfo implements Serializable {
    private static final long serialVersionUID = -26854218382696366L;

    private Integer year;
	private Integer population;
    private Double rfd;

    public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getPopulation() {
		return population;
	}

	public void setPopulation(Integer population) {
		this.population = population;
	}

	public Double getRfd() {
		return rfd;
	}

	public void setRfd(Double rfd) {
		this.rfd = rfd;
	}

	public IncomeInfo () { }

    public IncomeInfo(Row row) {
        this.year = row.getInt(0);
        this.population = row.getInt(1);
        this.rfd = row.getDouble(2);
    }
}
