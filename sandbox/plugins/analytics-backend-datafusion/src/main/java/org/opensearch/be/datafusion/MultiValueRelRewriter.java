/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;

/** Adds implicit element expansion for LIST-valued GROUP BY keys. */
final class MultiValueRelRewriter {

    private MultiValueRelRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(LogicalProject project) {
                RelNode input = project.getInput().accept(this);
                if (input == project.getInput()) {
                    return project;
                }

                RexShuttle inputRefRetype = new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return new RexInputRef(inputRef.getIndex(), input.getRowType().getFieldList().get(inputRef.getIndex()).getType());
                    }
                };
                List<RexNode> projects = project.getProjects().stream().map(expression -> expression.accept(inputRefRetype)).toList();
                return LogicalProject.create(
                    input,
                    project.getHints(),
                    projects,
                    project.getRowType().getFieldNames(),
                    project.getVariablesSet()
                );
            }

            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited instanceof Aggregate aggregate ? rewriteAggregate(aggregate) : visited;
            }
        });
    }

    private static RelNode rewriteAggregate(Aggregate aggregate) {
        RelNode input = scalarizeFinalStageInput(aggregate);
        boolean inputRetyped = input != aggregate.getInput();
        boolean groupKeysExpanded = false;
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                input = new MultiValueExpandRel(input, fieldIndex);
                groupKeysExpanded = true;
            }
        }

        java.util.Map<ReductionKey, Integer> hiddenKeys = new java.util.LinkedHashMap<>();
        int inputFieldCount = input.getRowType().getFieldCount();
        for (org.apache.calcite.rel.core.AggregateCall call : aggregate.getAggCallList()) {
            ReductionKey key = reductionKey(call, aggregate.getInput());
            if (key != null && input.getRowType().getFieldList().get(key.inputIndex()).getType().getComponentType() != null) {
                hiddenKeys.computeIfAbsent(key, ignored -> inputFieldCount + hiddenKeys.size());
            }
        }

        if (!hiddenKeys.isEmpty()) {
            var rexBuilder = aggregate.getCluster().getRexBuilder();
            List<RexNode> projects = new java.util.ArrayList<>(input.getRowType().getFieldCount() + hiddenKeys.size());
            List<String> names = new java.util.ArrayList<>(input.getRowType().getFieldNames());
            for (int index = 0; index < input.getRowType().getFieldCount(); index++) {
                projects.add(rexBuilder.makeInputRef(input, index));
            }
            for (ReductionKey key : hiddenKeys.keySet()) {
                RexNode list = rexBuilder.makeInputRef(input, key.inputIndex());
                projects.add(
                    rexBuilder.makeCall(key.minimum() ? MultiValueSortRewriter.LIST_MIN_OP : MultiValueSortRewriter.LIST_MAX_OP, list)
                );
                names.add("___mv_agg_" + (key.minimum() ? "min_" : "max_") + key.inputIndex());
            }
            input = LogicalProject.create(input, List.of(), projects, names);
        }

        RelNode rewrittenInput = input;
        List<org.apache.calcite.rel.core.AggregateCall> calls = aggregate.getAggCallList()
            .stream()
            .map(call -> rewriteReductionCall(call, aggregate, rewrittenInput, hiddenKeys))
            .toList();
        boolean aggregateCallsChanged = false;
        for (int index = 0; index < calls.size(); index++) {
            aggregateCallsChanged |= calls.get(index) != aggregate.getAggCallList().get(index);
        }
        return inputRetyped || groupKeysExpanded || !hiddenKeys.isEmpty() || aggregateCallsChanged
            ? aggregate.copy(aggregate.getTraitSet(), rewrittenInput, aggregate.getGroupSet(), aggregate.getGroupSets(), calls)
            : aggregate;
    }

    /**
     * PARTIAL aggregation has already expanded LIST group keys and reduced LIST MIN/MAX inputs to
     * scalar values. The planner's StageInputScan still carries the original ARRAY types, so
     * retype those columns before FINAL conversion and avoid inserting a second expansion/reduction.
     */
    private static RelNode scalarizeFinalStageInput(Aggregate aggregate) {
        if (!(aggregate.getInput() instanceof DataFusionFragmentConvertor.StageInputTableScan stageInput)) {
            return aggregate.getInput();
        }
        List<String> qualifiedName = stageInput.getTable().getQualifiedName();
        if (qualifiedName.isEmpty() || !qualifiedName.getLast().startsWith("input-")) {
            return aggregate.getInput();
        }

        java.util.Set<Integer> scalarIndexes = new java.util.LinkedHashSet<>();
        aggregate.getGroupSet().forEach(scalarIndexes::add);
        for (org.apache.calcite.rel.core.AggregateCall call : aggregate.getAggCallList()) {
            ReductionKey key = reductionKey(call, aggregate.getInput());
            if (key != null) {
                scalarIndexes.add(key.inputIndex());
            }
        }

        RelDataTypeFactory.Builder rowType = aggregate.getCluster().getTypeFactory().builder();
        boolean changed = false;
        for (int index = 0; index < stageInput.getRowType().getFieldCount(); index++) {
            var field = stageInput.getRowType().getFieldList().get(index);
            var fieldType = field.getType();
            if (scalarIndexes.contains(index) && fieldType.getComponentType() != null) {
                fieldType = aggregate.getCluster().getTypeFactory().createTypeWithNullability(fieldType.getComponentType(), true);
                changed = true;
            }
            rowType.add(field.getName(), fieldType);
        }
        if (!changed) {
            return aggregate.getInput();
        }
        return new DataFusionFragmentConvertor.StageInputTableScan(
            stageInput.getCluster(),
            stageInput.getTraitSet(),
            qualifiedName.getLast(),
            rowType.build()
        );
    }

    private static ReductionKey reductionKey(org.apache.calcite.rel.core.AggregateCall call, RelNode input) {
        if (call.getArgList().size() != 1) {
            return null;
        }
        org.apache.calcite.sql.SqlKind kind = call.getAggregation().getKind();
        if (kind != org.apache.calcite.sql.SqlKind.MIN && kind != org.apache.calcite.sql.SqlKind.MAX) {
            return null;
        }
        int inputIndex = call.getArgList().get(0);
        return input.getRowType().getFieldList().get(inputIndex).getType().getComponentType() == null
            ? null
            : new ReductionKey(inputIndex, kind == org.apache.calcite.sql.SqlKind.MIN);
    }

    private static boolean isArrayApproxDistinct(org.apache.calcite.rel.core.AggregateCall call, RelNode input) {
        if (call.getArgList().size() != 1 || !"APPROX_COUNT_DISTINCT".equalsIgnoreCase(call.getAggregation().getName())) {
            return false;
        }
        int inputIndex = call.getArgList().get(0);
        return input.getRowType().getFieldList().get(inputIndex).getType().getComponentType() != null;
    }

    private static org.apache.calcite.rel.core.AggregateCall rewriteReductionCall(
        org.apache.calcite.rel.core.AggregateCall call,
        Aggregate aggregate,
        RelNode input,
        java.util.Map<ReductionKey, Integer> hiddenKeys
    ) {
        ReductionKey key = reductionKey(call, aggregate.getInput());
        if (key == null) {
            if (isArrayApproxDistinct(call, aggregate.getInput())) {
                return org.apache.calcite.rel.core.AggregateCall.create(
                    DataFusionFragmentConvertor.LOCAL_OS_COUNT_DISTINCT_OP,
                    false,
                    call.isApproximate(),
                    call.ignoreNulls(),
                    call.rexList,
                    call.getArgList(),
                    call.filterArg,
                    call.distinctKeys,
                    call.collation,
                    aggregate.getGroupCount(),
                    input,
                    call.getType(),
                    call.getName()
                );
            }
            return call;
        }
        int inputIndex = hiddenKeys.getOrDefault(key, key.inputIndex());
        var componentType = aggregate.getInput().getRowType().getFieldList().get(key.inputIndex()).getType().getComponentType();
        var scalarType = aggregate.getCluster().getTypeFactory().createTypeWithNullability(componentType, true);
        return org.apache.calcite.rel.core.AggregateCall.create(
            call.getAggregation(),
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            call.rexList,
            List.of(inputIndex),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            aggregate.getGroupCount(),
            input,
            scalarType,
            call.getName()
        );
    }

    private record ReductionKey(int inputIndex, boolean minimum) {
    }
}
