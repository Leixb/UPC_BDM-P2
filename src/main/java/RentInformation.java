import java.util.List;

import org.apache.spark.sql.Row;

import scala.Serializable;

public class RentInformation implements Serializable {
    private static final long serialVersionUID = -2685444218382696366L;

    private String  address;
    private Long    bathrooms;
    private String  country;
    private String  detailedType_subTypology;
    private String  detailedType_typology;
    private String  distance;
    private String  district;
    private Boolean exterior;
    private String  externalReference;
    private String  floor;
    private Boolean has360;
    private Boolean has3DTour;
    private Boolean hasLift;
    private Boolean hasPlan;
    private Boolean hasStaging;
    private Boolean hasVideo;
    private Double  latitude;
    private Double  longitude;
    private String  municipality;
    private String  neighborhood;
    private Boolean newDevelopment;
    private Long    numPhotos;
    private String  operation;
    private Double  price;
    private Double  priceByArea;
    private String  propertyCode;
    private String  propertyType;
    private String  province;
    private Long    rooms;
    private Boolean showAddress;
    private Double  size;
    private String  status;

	private String  suggestedTexts_title;
    private String  suggestedTexts_subtitle;
    private String  thumbnail;
    private Boolean topNewDevelopment;
    private String  url;

    // From opendataBCN rent information
    private List<IncomeInfo> incomeInfo;
    // From opendataBCN incident information
    private long incidents;

    public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Long getBathrooms() {
		return bathrooms;
	}

	public void setBathrooms(Long bathrooms) {
		this.bathrooms = bathrooms;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getDetailedType_subTypology() {
		return detailedType_subTypology;
	}

	public void setDetailedType_subTypology(String detailedType_subTypology) {
		this.detailedType_subTypology = detailedType_subTypology;
	}

	public String getDetailedType_typology() {
		return detailedType_typology;
	}

	public void setDetailedType_typology(String detailedType_typology) {
		this.detailedType_typology = detailedType_typology;
	}

	public String getDistance() {
		return distance;
	}

	public void setDistance(String distance) {
		this.distance = distance;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public Boolean getExterior() {
		return exterior;
	}

	public void setExterior(Boolean exterior) {
		this.exterior = exterior;
	}

	public String getExternalReference() {
		return externalReference;
	}

	public void setExternalReference(String externalReference) {
		this.externalReference = externalReference;
	}

	public String getFloor() {
		return floor;
	}

	public void setFloor(String floor) {
		this.floor = floor;
	}

	public Boolean getHas360() {
		return has360;
	}

	public void setHas360(Boolean has360) {
		this.has360 = has360;
	}

	public Boolean getHas3DTour() {
		return has3DTour;
	}

	public void setHas3DTour(Boolean has3dTour) {
		has3DTour = has3dTour;
	}

	public Boolean getHasLift() {
		return hasLift;
	}

	public void setHasLift(Boolean hasLift) {
		this.hasLift = hasLift;
	}

	public Boolean getHasPlan() {
		return hasPlan;
	}

	public void setHasPlan(Boolean hasPlan) {
		this.hasPlan = hasPlan;
	}

	public Boolean getHasStaging() {
		return hasStaging;
	}

	public void setHasStaging(Boolean hasStaging) {
		this.hasStaging = hasStaging;
	}

	public Boolean getHasVideo() {
		return hasVideo;
	}

	public void setHasVideo(Boolean hasVideo) {
		this.hasVideo = hasVideo;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public String getMunicipality() {
		return municipality;
	}

	public void setMunicipality(String municipality) {
		this.municipality = municipality;
	}

	public String getNeighborhood() {
		return neighborhood;
	}

	public void setNeighborhood(String neighborhood) {
		this.neighborhood = neighborhood;
	}

	public Boolean getNewDevelopment() {
		return newDevelopment;
	}

	public void setNewDevelopment(Boolean newDevelopment) {
		this.newDevelopment = newDevelopment;
	}

	public Long getNumPhotos() {
		return numPhotos;
	}

	public void setNumPhotos(Long numPhotos) {
		this.numPhotos = numPhotos;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Double getPriceByArea() {
		return priceByArea;
	}

	public void setPriceByArea(Double priceByArea) {
		this.priceByArea = priceByArea;
	}

	public String getPropertyCode() {
		return propertyCode;
	}

	public void setPropertyCode(String propertyCode) {
		this.propertyCode = propertyCode;
	}

	public String getPropertyType() {
		return propertyType;
	}

	public void setPropertyType(String propertyType) {
		this.propertyType = propertyType;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public Long getRooms() {
		return rooms;
	}

	public void setRooms(Long rooms) {
		this.rooms = rooms;
	}

	public Boolean getShowAddress() {
		return showAddress;
	}

	public void setShowAddress(Boolean showAddress) {
		this.showAddress = showAddress;
	}

	public Double getSize() {
		return size;
	}

	public void setSize(Double size) {
		this.size = size;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getSuggestedTexts_title() {
		return suggestedTexts_title;
	}

	public void setSuggestedTexts_title(String suggestedTexts_title) {
		this.suggestedTexts_title = suggestedTexts_title;
	}

	public String getSuggestedTexts_subtitle() {
		return suggestedTexts_subtitle;
	}

	public void setSuggestedTexts_subtitle(String suggestedTexts_subtitle) {
		this.suggestedTexts_subtitle = suggestedTexts_subtitle;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public Boolean getTopNewDevelopment() {
		return topNewDevelopment;
	}

	public void setTopNewDevelopment(Boolean topNewDevelopment) {
		this.topNewDevelopment = topNewDevelopment;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<IncomeInfo> getIncomeInfo() {
		return incomeInfo;
	}

	public void setIncomeInfo(List<IncomeInfo> incomeInfo) {
		this.incomeInfo = incomeInfo;
	}

	public long getIncidents() {
		return incidents;
	}

	public void setIncidents(long incidents) {
		this.incidents = incidents;
	}

    // Helper functions to get the values form the Rows

    private String getString(Row row, String key) {
        Integer idx = row.fieldIndex(key);
        if (idx < 0 || row.isNullAt(idx)) return null;
        return row.getString(idx);
    }

    private Long getLong(Row row, String key) {
        Integer idx = row.fieldIndex(key);
        if (idx < 0 || row.isNullAt(idx)) return null;
        return row.getLong(idx);
    }

    private Double getDouble(Row row, String key) {
        Integer idx = row.fieldIndex(key);
        if (idx < 0 || row.isNullAt(idx)) return null;
        return row.getDouble(idx);
    }

    private Boolean getBoolean(Row row, String key) {
        Integer idx = row.fieldIndex(key);
        if (idx < 0 || row.isNullAt(idx)) return null;
        return row.getBoolean(idx);
    }

    private Row getStruct(Row row, String key) {
        Integer idx = row.fieldIndex(key);
        if (idx < 0 || row.isNullAt(idx)) return null;
        return row.getStruct(idx);
    }

    public RentInformation() { }

    public RentInformation(Row row) {
        this.address                  = getString(row, "address");
        this.bathrooms                = getLong(row, "bathrooms");
        this.country                  = getString(row, "country");
        this.detailedType_subTypology = getString(getStruct(row, "detailedType"), "subTypology");
        this.detailedType_typology    = getString(getStruct(row, "detailedType"), "typology");
        this.distance                 = getString(row, "distance");
        this.district                 = getString(row, "district");
        this.exterior                 = getBoolean(row, "exterior");
        this.externalReference        = getString(row, "externalReference");
        this.floor                    = getString(row, "floor");
        this.has360                   = getBoolean(row, "has360");
        this.has3DTour                = getBoolean(row, "has3DTour");
        this.hasLift                  = getBoolean(row, "hasLift");
        this.hasPlan                  = getBoolean(row, "hasPlan");
        this.hasStaging               = getBoolean(row, "hasStaging");
        this.hasVideo                 = getBoolean(row, "hasVideo");
        this.latitude                 = getDouble(row, "latitude");
        this.longitude                = getDouble(row, "longitude");
        this.municipality             = getString(row, "municipality");
        this.neighborhood             = getString(row, "neighborhood");
        this.newDevelopment           = getBoolean(row, "newDevelopment");
        this.numPhotos                = getLong(row, "numPhotos");
        this.operation                = getString(row, "operation");
        this.price                    = getDouble(row, "price");
        this.priceByArea              = getDouble(row, "priceByArea");
        this.propertyCode             = getString(row, "propertyCode");
        this.propertyType             = getString(row, "propertyType");
        this.province                 = getString(row, "province");
        this.rooms                    = getLong(row, "rooms");
        this.showAddress              = getBoolean(row, "showAddress");
        this.size                     = getDouble(row, "size");
        this.status                   = getString(row, "status");
        this.suggestedTexts_title     = getString(getStruct(row, "suggestedTexts"), "title");
        this.suggestedTexts_subtitle  = getString(getStruct(row, "suggestedTexts"), "subtitle");
        this.thumbnail                = getString(row, "thumbnail");
        this.topNewDevelopment        = getBoolean(row, "topNewDevelopment");
        this.url                      = getString(row, "url");
    }
}
