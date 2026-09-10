/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Maps the frontend element-wise ARRAY expression operators onto backend-local {@link SqlOperator}
 * constants whose {@code FunctionMappings.Sig} (registered in {@link DataFusionFragmentConvertor})
 * binds by reference to the native DataFusion UDFs {@code array_map_string}, {@code
 * array_map_integer}, {@code array_nullif}, and {@code array_coalesce}.
 *
 * <p>The frontend already names these operators identically to the native UDFs, but the frontend's
 * {@code SqlOperator} instances are not on the backend classpath. Rewriting to these local
 * constants gives the substrait converter a stable, reference-identity binding target — the same
 * pattern {@link ArrayAnyPredicateAdapter} uses for the predicate operators. Each local operator's
 * declared return type is a placeholder; {@link AbstractNameMappingAdapter} carries the original
 * call's array return type forward.
 */
final class ArrayElementWiseAdapter extends AbstractNameMappingAdapter {

    /** {@code array_map_string(array, fn_name, extra…)} → {@code ARRAY<STRING>} (carried forward). */
    static final SqlOperator LOCAL_ARRAY_MAP_STRING = SqlBasicFunction.create("array_map_string", ReturnTypes.ARG0, OperandTypes.VARIADIC);

    /** {@code array_map_integer(array, fn_name, extra…)} → {@code ARRAY<INTEGER>} (carried forward). */
    static final SqlOperator LOCAL_ARRAY_MAP_INTEGER = SqlBasicFunction.create(
        "array_map_integer",
        ReturnTypes.ARG0,
        OperandTypes.VARIADIC
    );

    /** {@code array_nullif(array, scalar)} → array type of arg0. */
    static final SqlOperator LOCAL_ARRAY_NULLIF = SqlBasicFunction.create(
        "array_nullif",
        ReturnTypes.ARG0,
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY)
    );

    /** {@code array_coalesce(array, scalar)} → array type of arg0. */
    static final SqlOperator LOCAL_ARRAY_COALESCE = SqlBasicFunction.create(
        "array_coalesce",
        ReturnTypes.ARG0,
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY)
    );

    private ArrayElementWiseAdapter(SqlOperator target) {
        super(target, List.of(), List.of());
    }

    static ArrayElementWiseAdapter mapString() {
        return new ArrayElementWiseAdapter(LOCAL_ARRAY_MAP_STRING);
    }

    static ArrayElementWiseAdapter mapInteger() {
        return new ArrayElementWiseAdapter(LOCAL_ARRAY_MAP_INTEGER);
    }

    static ArrayElementWiseAdapter nullif() {
        return new ArrayElementWiseAdapter(LOCAL_ARRAY_NULLIF);
    }

    static ArrayElementWiseAdapter coalesce() {
        return new ArrayElementWiseAdapter(LOCAL_ARRAY_COALESCE);
    }
}
