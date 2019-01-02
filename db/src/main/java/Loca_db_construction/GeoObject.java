package Loca_db_construction;

import java.util.List;

public class GeoObject {

    String name;
    long popindex;
    List<double[]> shape;
    String supercat;
    String subcat;

    public GeoObject(String name, long popindex, List<double[]> shape, String supercat, String subcat) {

	this.name = name;
	this.popindex = popindex;
	this.shape = shape;
	this.supercat = supercat;
	this.subcat = subcat;
    }

    public String toString() {
	return String.format("%s:%s:%s:%s:%s", name, popindex, shape, supercat, subcat);
    }
}
