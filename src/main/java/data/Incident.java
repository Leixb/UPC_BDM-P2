package data;

import java.io.Serializable;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.spark.sql.Row;

public class Incident implements Serializable {
    private static final long serialVersionUID = 1L;

    private String neighborhood;
    private Integer year;
    private Integer count;

	public String getNeighborhood() {
		return neighborhood;
	}
	public void setNeighborhood(String neighborhood) {
		this.neighborhood = neighborhood;
	}
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}

    public Incident(String neighborhood, Integer year, Integer count) {
        this.neighborhood = neighborhood;
        this.year = year;
        this.count = count;
    }

    private <T> T parseFirstMatch(Row row, String[] fields) {
        for (final String field : fields) {
            final Integer fieldIndex = row.fieldIndex(field);
            if (fieldIndex != -1 && !row.isNullAt(fieldIndex))
                return row.getAs(field);
        }
        return null;
    }

    public Incident(Row row) {
        this.neighborhood = parseFirstMatch(row, (new String[] { "Nom_barri", "Nom barri" }));
        this.year = parseFirstMatch(row, (new String[] { "NK_Any", "NK Any" }));
        this.count = parseFirstMatch(row, (new String[] {  "Numero_incidents_GUB", "Numero d'incidents GUB" }));
    }

    // Helper function to filter codes
    public static boolean codeEquals(Row row, String code) {
        return Stream.of(new String[] { "Codi_Incident", "Codi Incident" }).anyMatch(field -> {
            final Integer fieldIndex = row.fieldIndex(field);
            return fieldIndex != -1 && !row.isNullAt(fieldIndex) && row.getString(fieldIndex).equals(code);
        });
    }
}
