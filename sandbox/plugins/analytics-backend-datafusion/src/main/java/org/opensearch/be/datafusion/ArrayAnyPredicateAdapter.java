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

/** Maps frontend ARRAY predicates to native DataFusion UDF names. */
final class ArrayAnyPredicateAdapter extends AbstractNameMappingAdapter {

    static final SqlOperator LOCAL_ARRAY_ANY_COMPARE = SqlBasicFunction.create(
        "array_any_compare",
        ReturnTypes.BOOLEAN_NULLABLE,
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
    );

    static final SqlOperator LOCAL_ARRAY_ANY_BETWEEN = SqlBasicFunction.create(
        "array_any_between",
        ReturnTypes.BOOLEAN_NULLABLE,
        OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
    );

    private ArrayAnyPredicateAdapter(SqlOperator target) {
        super(target, List.of(), List.of());
    }

    static ArrayAnyPredicateAdapter compare() {
        return new ArrayAnyPredicateAdapter(LOCAL_ARRAY_ANY_COMPARE);
    }

    static ArrayAnyPredicateAdapter between() {
        return new ArrayAnyPredicateAdapter(LOCAL_ARRAY_ANY_BETWEEN);
    }
}
