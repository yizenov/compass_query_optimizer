/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef QUERYENGINE_RELALGEXECUTOR_H
#define QUERYENGINE_RELALGEXECUTOR_H

#include "../Shared/scope.h"
#include "Distributed/AggregatedResult.h"
#include "Execute.h"
#include "InputMetadata.h"
#include "QueryRewrite.h"
#include "RelAlgExecutionDescriptor.h"
#include "SpeculativeTopN.h"
#include "StreamingTopN.h"

#include <ctime>

enum class MergeType { Union, Reduce };

struct FirstStepExecutionResult {
  ExecutionResult result;
  const MergeType merge_type;
  const unsigned node_id;
  bool is_outermost_query;
};

class RelAlgExecutor {
 public:
  RelAlgExecutor(Executor* executor, const Catalog_Namespace::SessionInfo& session)
      : executor_(executor),
        session_(session),
        cat_(session.get_catalog()),
        now_(0),
        queue_time_ms_(0),
        fpd_enabled_(session.fpd_enabled()) {}

  RelAlgExecutor(Executor* executor, const Catalog_Namespace::SessionInfo& session, const bool fpd_enabled)
      : executor_(executor),
        session_(session),
        cat_(session.get_catalog()),
        now_(0),
        queue_time_ms_(0),
        fpd_enabled_(fpd_enabled) {}

  RelAlgExecutor(Executor* executor,
                 const std::unordered_map<std::pair<unsigned, unsigned>,
                                          std::pair<std::vector<unsigned>, FAGMS_Sketch*>,
                                          pair_hash>& sketch_data,
                 const Catalog_Namespace::SessionInfo& session,
                 const bool fpd_enabled)
      : executor_(executor),
        session_(session),
        cat_(session.get_catalog()),
        fagms_sketches_(sketch_data),
        now_(0),
        queue_time_ms_(0),
        fpd_enabled_(fpd_enabled) {}

  ExecutionResult executeRelAlgQuery(const std::string& query_ra,
                                     const CompilationOptions& co,
                                     const ExecutionOptions& eo,
                                     RenderInfo* render_info);

  FirstStepExecutionResult executeRelAlgQueryFirstStep(const RelAlgNode* ra,
                                                       const CompilationOptions& co,
                                                       const ExecutionOptions& eo,
                                                       RenderInfo* render_info);

  void prepareLeafExecution(const AggregatedColRange& agg_col_range,
                            const StringDictionaryGenerations& string_dictionary_generations,
                            const TableGenerations& table_generations);

  ExecutionResult executeRelAlgSubQuery(const RexSubQuery* subquery,
                                        const CompilationOptions& co,
                                        const ExecutionOptions& eo);

  ExecutionResult executeRelAlgSeq(std::vector<RaExecutionDesc>& ed_list,
                                   const CompilationOptions& co,
                                   const ExecutionOptions& eo,
                                   RenderInfo* render_info,
                                   const int64_t queue_time_ms);

  void addLeafResult(const unsigned id, const AggregatedResult& result) {
    const auto it_ok = leaf_results_.emplace(id, result);
    CHECK(it_ok.second);
  }

  void registerSubquery(RexSubQuery* subquery) noexcept { subqueries_.push_back(subquery); }

  const std::vector<RexSubQuery*>& getSubqueries() const noexcept { return subqueries_; };

  AggregatedColRange computeColRangesCache(const RelAlgNode* ra);

  StringDictionaryGenerations computeStringDictionaryGenerations(const RelAlgNode* ra);

  TableGenerations computeTableGenerations(const RelAlgNode* ra);

  Executor* getExecutor() const;

  const Catalog_Namespace::SessionInfo& getSessionInfo() { return session_; }

  const bool fpd_enabled() { return fpd_enabled_; }

  ExecutionResult executeRelAlgQueryFPD(const RelAlgNode* ra,
                                        ssize_t fpd_max_count,
                                        const std::pair<int32_t, int> table_id_phy);

  ssize_t addPushDownFilter(const int table_id, ExecutionResult& result) {
    ssize_t num_filtered_rows = -1;
    auto row_set = boost::get<RowSetPtr>(&result.getDataPtr());
    if (row_set) {
      CHECK_LT(size_t(0), (*row_set)->colCount());
      num_filtered_rows = (*row_set)->rowCount();
    }
    CHECK_LT(table_id, 0);
    ExecutionResult result_copy(std::move(result));
    const auto it_ok = push_down_filters_.insert(std::make_pair(table_id, std::move(result_copy)));
    CHECK(it_ok.second);
    return num_filtered_rows;
  }

  void addHashJoinCol(const std::pair<uint32_t, unsigned> t_id_lhs, const std::pair<uint32_t, unsigned> t_id_rhs) {
    hash_join_cols_.emplace(t_id_lhs, t_id_rhs);
  }

  void addJoinColConnection(const std::pair<size_t, size_t> nodes_connection, const std::pair<unsigned, unsigned> col_ids) {
    join_col_connections.emplace(nodes_connection, col_ids);
  }

  void addTableIndexInfo(const std::pair<int32_t, unsigned> t_info, const int t_order_id) {
    table_index_info.emplace(t_info, t_order_id);
  }

  std::unordered_multimap<std::pair<uint32_t, unsigned>, std::pair<uint32_t, unsigned>, pair_hash>& getHashJoinCols() {
    return hash_join_cols_;
  }

  std::unordered_multimap<std::pair<size_t, size_t>, std::pair<unsigned, unsigned>, pair_hash>& getJoinColConnections() {
    return join_col_connections;
  }

  std::unordered_multimap<std::pair<int32_t, unsigned>, int, pair_hash>& getTableIndexInfo() {
    return table_index_info;
  }

  std::vector<std::pair<std::pair<unsigned int, unsigned int>, unsigned int>>& getSketchComplexity() {
    return table_sketch_complexity;
  }

  std::unordered_map<std::pair<unsigned, unsigned>, bool, pair_hash> getSketchStatuses() { return sketch_status; }

  FAGMS_Sketch* createAndAddSketch(const std::pair<unsigned, int> table_tuple,
                                   const unsigned col_id,
                                   const unsigned num_buckets,
                                   const unsigned num_rows,
                                   Xi_CW2B* xi_b,
                                   Xi_EH3* xi_pm1) {
    FAGMS_Sketch* sketch = new FAGMS_Sketch(num_buckets, num_rows, xi_b, xi_pm1);
    std::vector<unsigned> col_ids = {col_id};
    fagms_sketches_.emplace(table_tuple, std::make_pair(col_ids, sketch));
    return sketch;
  }

  FAGMS_Sketch* createAndAddSketch(const std::pair<unsigned, int> table_tuple,
                                   std::vector<unsigned> col_ids,
                                   const unsigned num_keys,
                                   const unsigned num_buckets,
                                   const unsigned num_rows,
                                   Xi_CW2B** xis_b,
                                   Xi_EH3** xis_pm1) {
    FAGMS_Sketch* sketch = new FAGMS_Sketch(num_buckets, num_rows, num_keys, xis_b, xis_pm1);
    fagms_sketches_.emplace(table_tuple, std::make_pair(col_ids, sketch));
    return sketch;
  }

  void initializeSketchInitValues(uint64_t no_rows, uint64_t no_buckets, uint64_t block_size, uint64_t gird_size);

  std::unordered_map<std::pair<unsigned, unsigned>, std::pair<std::vector<unsigned>, FAGMS_Sketch*>, pair_hash>&
    getSketches() { return fagms_sketches_; }

 private:
  ExecutionResult executeRelAlgQueryNoRetry(const std::string& query_ra,
                                            const CompilationOptions& co,
                                            const ExecutionOptions& eo,
                                            RenderInfo* render_info);

  void executeRelAlgStep(const size_t step_idx,
                         std::vector<RaExecutionDesc>&,
                         const CompilationOptions&,
                         const ExecutionOptions&,
                         RenderInfo*,
                         const int64_t queue_time_ms);

  ExecutionResult executeCompound(const RelCompound*,
                                  const CompilationOptions&,
                                  const ExecutionOptions&,
                                  RenderInfo*,
                                  const int64_t queue_time_ms);

  ExecutionResult executeAggregate(const RelAggregate* aggregate,
                                   const CompilationOptions& co,
                                   const ExecutionOptions& eo,
                                   RenderInfo* render_info,
                                   const int64_t queue_time_ms);

  ExecutionResult executeProject(const RelProject*,
                                 const CompilationOptions&,
                                 const ExecutionOptions&,
                                 RenderInfo*,
                                 const int64_t queue_time_ms);

  ExecutionResult executeFilter(const RelFilter*,
                                const CompilationOptions&,
                                const ExecutionOptions&,
                                RenderInfo*,
                                const int64_t queue_time_ms);

  ExecutionResult executeSort(const RelSort*,
                              const CompilationOptions&,
                              const ExecutionOptions&,
                              RenderInfo*,
                              const int64_t queue_time_ms);

  ExecutionResult executeJoin(const RelJoin*,
                              const CompilationOptions&,
                              const ExecutionOptions&,
                              RenderInfo*,
                              const int64_t queue_time_ms);

  ExecutionResult executeLogicalValues(const RelLogicalValues*, const ExecutionOptions&);

  // TODO(alex): just move max_groups_buffer_entry_guess to RelAlgExecutionUnit once
  //             we deprecate the plan-based executor paths and remove WorkUnit
  struct WorkUnit {
    RelAlgExecutionUnit exe_unit;
    const RelAlgNode* body;
    const size_t max_groups_buffer_entry_guess;
    std::unique_ptr<QueryRewriter> query_rewriter;
  };

  WorkUnit createSortInputWorkUnit(const RelSort*, const bool just_explain);

  ExecutionResult executeWorkUnit(const WorkUnit& work_unit,
                                  const std::vector<TargetMetaInfo>& targets_meta,
                                  const bool is_agg,
                                  const CompilationOptions& co,
                                  const ExecutionOptions& eo,
                                  RenderInfo*,
                                  const int64_t queue_time_ms);

  size_t getNDVEstimation(const WorkUnit& work_unit,
                          const bool is_agg,
                          const CompilationOptions& co,
                          const ExecutionOptions& eo);

  ssize_t getFilteredCountAll(const WorkUnit& work_unit,
                              const bool is_agg,
                              const CompilationOptions& co,
                              const ExecutionOptions& eo);

  ssize_t getFilteredCountAllAndUpdateSketch(const WorkUnit& work_unit,
                                             const bool is_agg,
                                             const CompilationOptions& co,
                                             const ExecutionOptions& eo);

  bool isRowidLookup(const WorkUnit& work_unit);

  ExecutionResult renderWorkUnit(const RelAlgExecutor::WorkUnit& work_unit,
                                 const std::vector<TargetMetaInfo>& targets_meta,
                                 RenderInfo* render_info,
                                 const int32_t error_code,
                                 const int64_t queue_time_ms);

  void executeUnfoldedMultiJoin(const RelAlgNode* user,
                                RaExecutionDesc& exec_desc,
                                const CompilationOptions& co,
                                const ExecutionOptions& eo,
                                const int64_t queue_time_ms);

  ExecutionResult handleRetry(const int32_t error_code_in,
                              const RelAlgExecutor::WorkUnit& work_unit,
                              const std::vector<TargetMetaInfo>& targets_meta,
                              const bool is_agg,
                              const CompilationOptions& co,
                              const ExecutionOptions& eo,
                              const int64_t queue_time_ms);

  static void handlePersistentError(const int32_t error_code);

  static std::string getErrorMessageFromCode(const int32_t error_code);

  WorkUnit createWorkUnit(const RelAlgNode*, const SortInfo&, const bool just_explain);

  WorkUnit createCompoundWorkUnit(const RelCompound*, const SortInfo&, const bool just_explain);

  WorkUnit createAggregateWorkUnit(const RelAggregate*, const SortInfo&, const bool just_explain);

  WorkUnit createProjectWorkUnit(const RelProject*, const SortInfo&, const bool just_explain);

  WorkUnit createFilterWorkUnit(const RelFilter*, const SortInfo&, const bool just_explain);

  WorkUnit createJoinWorkUnit(const RelJoin*, const SortInfo&, const bool just_explain);

  void addTemporaryTable(const int table_id, const ResultPtr& result) {
    auto row_set = boost::get<RowSetPtr>(&result);
    if (row_set) {
      CHECK_LT(size_t(0), (*row_set)->colCount());
    }
    CHECK_LT(table_id, 0);
    const auto it_ok = temporary_tables_.emplace(table_id, result);
    CHECK(it_ok.second);
  }

  void handleNop(const RelAlgNode*);

  JoinQualsPerNestingLevel translateLeftDeepJoinFilter(
      const RelLeftDeepInnerJoin* join,
      const std::vector<InputDescriptor>& input_descs,
      const std::unordered_map<const RelAlgNode*, int>& input_to_nest_level,
      const bool just_explain);

  // Transform the provided `join_condition` to conjunctive form, find composite
  // key opportunities and finally translate it to an Analyzer expression.
  std::list<std::shared_ptr<Analyzer::Expr>> makeJoinQuals(
      const RexScalar* join_condition,
      const std::vector<JoinType>& join_types,
      const std::unordered_map<const RelAlgNode*, int>& input_to_nest_level,
      const bool just_explain) const;

  Executor* executor_;
  const Catalog_Namespace::SessionInfo& session_;
  const Catalog_Namespace::Catalog& cat_;
  TemporaryTables temporary_tables_;
  std::unordered_map<int, const ExecutionResult> push_down_filters_;
  // key and value are pair of table_id and node_id, it is used for constructing a graph
  std::unordered_multimap<std::pair<uint32_t, unsigned>, std::pair<uint32_t, unsigned>, pair_hash> hash_join_cols_;
  // key is a pair of lhs(t_id, node_id) and rhs(t_id, node_id) and value is a pair of lhs_col_id and rhs_col_id
  // this information is needed to estimate join sizes with a separate sketch representation
  std::unordered_multimap<std::pair<size_t, size_t>, std::pair<unsigned, unsigned>, pair_hash> join_col_connections;
  // key is a pair of table_id and node_id and value is original node index in relational algebra tree
  std::unordered_multimap<std::pair<int32_t, unsigned>, int, pair_hash> table_index_info;
  // key is a pair of table_id and node_id and value is a pair of vector of column(s) and sketch (or sketch group)
  std::unordered_map<std::pair<unsigned, unsigned>, std::pair<std::vector<unsigned>, FAGMS_Sketch*>, pair_hash> fagms_sketches_;
  // sketch complexity of each table: number of survived tuples * number of columns
  std::vector<std::pair<std::pair<unsigned int, unsigned int>, unsigned int>> table_sketch_complexity;
  // this is to indicate whether sketch is copied from the sketch templates
  std::unordered_map<std::pair<unsigned, unsigned>, bool, pair_hash> sketch_status;

  time_t now_;
  std::vector<std::shared_ptr<Analyzer::Expr>> target_exprs_owned_;  // TODO(alex): remove
  std::vector<RexSubQuery*> subqueries_;
  std::unordered_map<unsigned, AggregatedResult> leaf_results_;
  int64_t queue_time_ms_;
  static SpeculativeTopNBlacklist speculative_topn_blacklist_;
  static const size_t max_groups_buffer_entry_default_guess{16384};
  const bool fpd_enabled_;
};

#endif  // QUERYENGINE_RELALGEXECUTOR_H
