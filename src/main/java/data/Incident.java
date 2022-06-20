package data;

import java.io.Serializable;
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

    private int getIndex(Row row, String[] fields) {
        for (int i = 0; i < fields.length; i++) {
            final Integer fieldIndex = row.fieldIndex(fields[i]);
            if (fieldIndex != -1 && !row.isNullAt(fieldIndex))
                return i;
        }
        return -1;
    }

    private <T> T getValue(Row row, String field) {
        return row.getAs(row.fieldIndex(field));
    }

    public Incident(Row row) {
        // There are two different kinds of datasets in the input, we need
        // to determine which schema are we using.

        final String[][] field_candidates = {
            {"Nom_barri",            "Nom barri"},
            {"NK_Any",               "NK Any"},
            {"Numero_incidents_GUB", "Numero d'incidents GUB"},
        };

        // Since we know that once we get a match the rest will use the same
        // schema, we can use the first match to determine the schema.
        final int index = getIndex(row, field_candidates[0]);

        this.neighborhood = getValue(row, field_candidates[0][index]);
        this.year = getValue(row, field_candidates[1][index]);
        this.count = getValue(row, field_candidates[2][index]);
    }

    // Helper function to filter codes
    public static boolean codeEquals(Row row, String code) {
        return Stream.of(new String[] { "Codi_Incident", "Codi Incident" }).anyMatch(field -> {
            final Integer fieldIndex = row.fieldIndex(field);
            return fieldIndex != -1 && !row.isNullAt(fieldIndex) && row.getString(fieldIndex).equals(code);
        });
    }
}
