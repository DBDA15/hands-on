package de.hpi.fgis.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This function filters empty IND sets.
 *
 * @author sebastian.kruse
 * @since 05.06.2015
 */
@FunctionAnnotation.ReadFields("1")
@FunctionAnnotation.ForwardedFields({"0", "1"})
public class FilterEmptyIndSets implements FilterFunction<Tuple2<Integer, int[]>> {

	@Override
	public boolean filter(Tuple2<Integer, int[]> indSet) throws Exception {
		return indSet.f1.length > 0;
	}
}
