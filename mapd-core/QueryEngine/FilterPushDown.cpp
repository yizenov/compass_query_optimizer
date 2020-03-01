#include "FilterPushDown.h"

class FilterPushDown {
 public:
  size_t getNumTuples(int32_t table_id, Catalog_Namespace::Catalog& cat) {
    size_t num_tuples = 0;
    const auto td = cat.getMetadataForTable(table_id);
    const auto shard_tables = cat.getPhysicalTablesDescriptors(td);
    for (const auto shard_table : shard_tables) {
      const auto& shard_metainfo = shard_table->fragmenter->getFragmentsForQuery();
      num_tuples += shard_metainfo.getPhysicalNumTuples();
    }
    return num_tuples;
  }

  bool extractHashJoinCol(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                          const RexScalar* rex,
                          RelAlgExecutor* ra_executor) {
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      auto oper_size = rex_operator->size();
      if (oper_size == 2) {
        const auto rex_input_lhs = dynamic_cast<const RexInput*>(rex_operator->getOperand(0));
        const auto rex_input_rhs = dynamic_cast<const RexInput*>(rex_operator->getOperand(1));
        if (rex_input_lhs && rex_input_rhs) {
          unsigned t_id_lhs, t_id_rhs;
          std::string col_name_lhs, col_name_rhs;
          std::tie(t_id_lhs, col_name_lhs) =
              findColumnNameByIndex(rex_input_lhs->getSourceNode(), rex_input_lhs->getIndex());
          std::tie(t_id_rhs, col_name_rhs) =
              findColumnNameByIndex(rex_input_rhs->getSourceNode(), rex_input_rhs->getIndex());

          // look for the RelScan's order in nodes using t_id
          int order_lhs = -1, order_rhs = -1, order = 0;
          for (auto node : nodes) {
            auto node_scan = std::dynamic_pointer_cast<RelScan>(node);
            if (node_scan) {
              auto t_id = node_scan->getId();
              if (t_id == t_id_lhs) {
                // join only if the type is integer|time|string
                // this is required because of condition (ps_supplycost = min_ps_supplycost) in 02.sql
                auto col_desc = ra_executor->getSessionInfo().get_catalog().getMetadataForColumn(
                    node_scan->getTableDescriptor()->tableId, col_name_lhs);
                if (!col_desc->columnType.is_integer() && !col_desc->columnType.is_time() &&
                    !col_desc->columnType.is_string()) {
                  return true;
                }
                order_lhs = order;
              }

              if (t_id == t_id_rhs) {
                order_rhs = order;
              }

              if (order_lhs != -1 && order_rhs != -1) {
                break;
              }

              order++;
            }
          }

          CHECK_GT(order_lhs, -1);
          CHECK_GT(order_rhs, -1);
          if (order_lhs != order_rhs) {
            // ra_executor->addHashJoinCol(order_lhs, order_rhs);
            return true;
          }
        }
      }

      for (size_t i = 0; i < oper_size; i++) {
        const auto operand = rex_operator->getOperand(i);
        if (!extractHashJoinCol(nodes, operand, ra_executor)) {
          return false;
        }
      }

      return true;
    }

    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query) {
      return false;
    }

    return true;
  }

  bool evaluateAndPushDown(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                           RelAlgExecutor* ra_executor,
                           std::unordered_map<const RelAlgNode*, std::pair<size_t, bool>>& table_sizes) {
    // look for each RelScan
    for (size_t i = 0; i < nodes.size(); i++) {
      auto node_scan = std::dynamic_pointer_cast<RelScan>(nodes[i]);
      if (node_scan) {
        // if next node is neither scan nor join, this is already a derived table, skip
        if (!std::dynamic_pointer_cast<RelScan>(nodes[i + 1]) && !std::dynamic_pointer_cast<RelJoin>(nodes[i + 1])) {
          continue;
        }

        auto table_size = table_sizes[node_scan.get()];
        // check table is the largest: if so, skip
        if (table_size.second) {
          continue;
        }

        auto num_tuples = table_size.first;
        // check table's size and if it is not large enough, skip
        if (num_tuples < PUSH_DOWN_MIN_TABLE_SIZE) {
          continue;
        }

        // find filter predicates to push-down and columns to project for this table
        std::vector<std::pair<unsigned, std::string>> cols_to_project;
        auto filter_expr_new = findFilterAndProject(nodes, i, cols_to_project);
        if (!filter_expr_new) {
          continue;
        }

        // add new push-down filter from filter_expr_new
        auto node_filter_new = std::shared_ptr<RelAlgNode>(new RelFilter(filter_expr_new, nodes[i]));
        // look for the parent node to replace its input into new RelFilter
        size_t input_idx = 0;
        size_t dist = 1;
        if (std::dynamic_pointer_cast<RelScan>(nodes[i + dist])) {
          // current node is left input of next join
          while (true) {
            dist++;
            if (std::dynamic_pointer_cast<RelJoin>(nodes[i + dist])) {
              break;
            }
          }
        } else if (std::dynamic_pointer_cast<RelJoin>(nodes[i + dist])) {
          // current node is right input of right next join
          input_idx = 1;
        }

        auto node_output = std::dynamic_pointer_cast<RelAlgNode>(nodes[i + dist]);
        auto old_input = node_output->getAndOwnInput(input_idx);
        node_output->replaceInput(old_input, node_filter_new);

        // add new RelFilter to nodes
        nodes.insert(nodes.begin() + i + 1, node_filter_new);
        dist++;
        std::cout << "------ AFTER findFilterAndProject ------" << std::endl;
        std::cout << tree_string(nodes.back().get(), 0) << std::endl;

        // make RelCompound and insert into nodes
        makeAndInsertCompound(nodes, i + 1, i + dist, cols_to_project);
        std::cout << "------ AFTER makeAndInsertCompound ------" << std::endl;
        std::cout << tree_string(nodes.back().get(), 0) << std::endl;

        // execute RelCompound and add it to temporary table
        auto table_id_phy = std::make_pair(node_scan->getTableDescriptor()->tableId, node_scan->getId());
        if (executeFilterAndEvaluate(nodes[i + 1].get(), num_tuples, table_id_phy, ra_executor)) {
          // update rex_input in post-join nodes
          updatePostJoinExprs(nodes, i, i + dist, input_idx, cols_to_project);
          std::cout << "------ AFTER updatePostJoinExprs ------" << std::endl;
          std::cout << tree_string(nodes.back().get(), 0) << std::endl;

          schema_map_.clear();
          i++;
        } else {
          // remove filter/compound
          auto node_to_erase = node_output->getAndOwnInput(input_idx);
          node_output->replaceInput(node_to_erase, old_input);
          nodes.erase(nodes.begin() + i + 1);
        }

        continue;
      }
    }

    std::cout << "------ AFTER pushDownFilterPredicates ------" << std::endl;
    std::cout << tree_string(nodes.back().get(), 0) << std::endl;

    return true;
  }

 private:
  std::pair<unsigned, std::string> findColumnNameByIndex(const RelAlgNode* source, unsigned c_id) {
    std::vector<std::pair<unsigned, unsigned>> t_num_cols;
    std::vector<std::string> schema;
    std::tie(t_num_cols, schema) = getSchemaFromSource(source);
    CHECK(!t_num_cols.empty());
    std::pair<int, std::string> c_info;
    unsigned num_cols_so_far = 0;
    for (auto it = t_num_cols.begin(); it != t_num_cols.end(); ++it) {
      if (c_id < num_cols_so_far + it->second) {
        c_info = std::make_pair(it->first, schema[c_id]);
        break;
      } else {
        num_cols_so_far += it->second;
      }
    }
    CHECK_GT(c_info.first, -1);
    return c_info;
  }

  std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>> getSchemaFromSource(
      const RelAlgNode* node) {
    auto it = schema_map_.find(node);
    if (it != schema_map_.end()) {
      return it->second;
    } else {
      auto schema_new = buildSchemaFromSource(node);
      schema_map_[node] = schema_new;
      return schema_new;
    }
  }

  std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>> buildSchemaFromSource(
      const RelAlgNode* node) {
    std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>> schema;
    std::vector<std::pair<unsigned, unsigned>> t_num_cols;
    // RelScan
    const auto node_scan = dynamic_cast<const RelScan*>(node);
    if (node_scan) {
      t_num_cols.emplace_back(node_scan->getId(), node_scan->size());
      schema = std::make_pair(t_num_cols, node_scan->getFieldNames());
    }
    // RelFilter
    const auto node_filter = dynamic_cast<const RelFilter*>(node);
    if (node_filter) {
      schema = buildSchemaFromSource(node_filter->getInput(0));
    }
    // RelProject
    const auto node_project = dynamic_cast<const RelProject*>(node);
    if (node_project) {
      t_num_cols.emplace_back(findBaseTableId(node_project->getInput(0)), node_project->size());
      schema = std::make_pair(t_num_cols, node_project->getFields());
    }
    // RelAggregate
    const auto node_aggregate = dynamic_cast<const RelAggregate*>(node);
    if (node_aggregate) {
      t_num_cols.emplace_back(findBaseTableId(node_aggregate->getInput(0)), node_aggregate->size());
      schema = std::make_pair(t_num_cols, node_aggregate->getFields());
    }
    // RelSort
    const auto node_sort = dynamic_cast<const RelSort*>(node);
    if (node_sort) {
      schema = buildSchemaFromSource(node_sort->getInput(0));
    }
    // RelJoin
    const auto node_join = dynamic_cast<const RelJoin*>(node);
    if (node_join) {
      std::vector<std::pair<unsigned, unsigned>> t_num_cols_lhs, t_num_cols_rhs;
      std::vector<std::string> schema_lhs, schema_rhs;
      std::tie(t_num_cols_lhs, schema_lhs) = buildSchemaFromSource(node_join->getInput(0));
      std::tie(t_num_cols_rhs, schema_rhs) = buildSchemaFromSource(node_join->getInput(1));
      t_num_cols_lhs.insert(std::end(t_num_cols_lhs), std::begin(t_num_cols_rhs), std::end(t_num_cols_rhs));
      schema_lhs.insert(std::end(schema_lhs), std::begin(schema_rhs), std::end(schema_rhs));
      schema = std::make_pair(t_num_cols_lhs, schema_lhs);
    }
    // RelCompound
    const auto node_compound = dynamic_cast<const RelCompound*>(node);
    if (node_compound) {
      t_num_cols.emplace_back(findBaseTableId(node_compound->getInput(0)), node_compound->size());
      schema = std::make_pair(t_num_cols, node_compound->getFields());
    }
    CHECK(node_scan || node_filter || node_project || node_aggregate || node_sort || node_join || node_compound);
    return schema;
  }

  unsigned findBaseTableId(const RelAlgNode* node) {
    const auto node_scan = dynamic_cast<const RelScan*>(node);
    if (node_scan) {
      return node_scan->getId();
    } else {
      return findBaseTableId(node->getInput(0));
    }
  }

  std::unique_ptr<const RexScalar> findFilterAndProject(
      std::vector<std::shared_ptr<RelAlgNode>>& nodes,
      size_t scan_idx,
      std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    std::unique_ptr<const RexScalar> filter_expr_new;
    for (auto it_nodes = nodes.rbegin(); it_nodes != nodes.rend(); ++it_nodes) {
      auto node_project = std::dynamic_pointer_cast<RelProject>(*it_nodes);
      if (node_project) {
        if (dynamic_cast<const RelAggregate*>(node_project->getInput(0)) ||
            dynamic_cast<const RelProject*>(node_project->getInput(0))) {
          continue;
        }
        for (size_t i = 0; i < node_project->size(); i++) {
          getProjectFromRex(node_project->getProjectAt(i), nodes[scan_idx], cols_to_project);
        }
        continue;
      }
      auto node_filter = std::dynamic_pointer_cast<RelFilter>(*it_nodes);
      if (node_filter) {
        filter_expr_new = findFilterAndProjectFromRex(node_filter->getCondition(), nodes[scan_idx], cols_to_project);
        continue;
      }
      if (std::dynamic_pointer_cast<RelJoin>(*it_nodes)) {
        break;
      }
    }
    return filter_expr_new;
  }

  void getProjectFromRex(const RexScalar* rex,
                         std::shared_ptr<RelAlgNode>& node_scan,
                         std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      auto c_info = getColumnInfoFromScan(rex_input, node_scan->getId());
      if (!c_info.second.empty()) {
        auto it = std::lower_bound(cols_to_project.begin(), cols_to_project.end(), c_info, [](auto lhs, auto rhs) {
          return lhs.first < rhs.first;
        });
        if (it == cols_to_project.end() || !(*it == c_info)) {
          cols_to_project.emplace(it, std::move(c_info));
        }
      }
    }
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      for (size_t i = 0; i < rex_operator->size(); i++) {
        const auto operand = rex_operator->getOperand(i);
        getProjectFromRex(operand, node_scan, cols_to_project);
      }
    }

    const auto rex_case = dynamic_cast<const RexCase*>(rex);
    if (rex_case) {
      for (size_t i = 0; i < rex_case->branchCount(); i++) {
        getProjectFromRex(rex_case->getWhen(i), node_scan, cols_to_project);
        getProjectFromRex(rex_case->getThen(i), node_scan, cols_to_project);
      }
      getProjectFromRex(rex_case->getElse(), node_scan, cols_to_project);
    }
  }

  std::unique_ptr<const RexScalar> findFilterAndProjectFromRex(
      const RexScalar* rex,
      std::shared_ptr<RelAlgNode>& node_scan,
      std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      auto c_info = getColumnInfoFromScan(rex_input, node_scan->getId());
      if (!c_info.second.empty()) {
        return std::unique_ptr<const RexScalar>(new RexInput(node_scan.get(), c_info.first));
      } else {
        return nullptr;
      }
    }
    const auto rex_literal = dynamic_cast<const RexLiteral*>(rex);
    if (rex_literal) {
      return rex_literal->deepCopy();
    }
    const auto rex_func_operator = dynamic_cast<const RexFunctionOperator*>(rex);
    if (rex_func_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      std::vector<std::pair<unsigned, std::string>> c_infos_tmp;
      auto oper_size = rex_func_operator->size();
      for (size_t i = 0; i < oper_size; i++) {
        auto operand_new = findFilterAndProjectFromFuncOper(rex_func_operator->getOperand(i), node_scan, c_infos_tmp);
        if (operand_new) {
          operands_new.push_back(std::move(operand_new));
        }
      }
      if (operands_new.size() != oper_size) {
        for (auto& c_info : c_infos_tmp) {
          auto it = std::lower_bound(cols_to_project.begin(), cols_to_project.end(), c_info, [](auto lhs, auto rhs) {
            return lhs.first < rhs.first;
          });
          if (it == cols_to_project.end() || !(*it == c_info)) {
            cols_to_project.emplace(it, std::move(c_info));
          }
        }
        return nullptr;
      } else {
        return std::unique_ptr<const RexScalar>(
            new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
      }
    }
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      auto oper_size = rex_operator->size();
      if (oper_size >= 2) {
        std::vector<std::pair<unsigned, std::string>> c_infos_tmp;
        unsigned t_id_prev = 0;
        bool has_joint_condition = false;
        for (size_t i = 0; i < oper_size; i++) {
          const auto rex_input = dynamic_cast<const RexInput*>(rex_operator->getOperand(i));
          if (rex_input) {
            auto t_id = findColumnNameByIndex(rex_input->getSourceNode(), rex_input->getIndex()).first;
            if (t_id_prev == 0) {
              t_id_prev = t_id;
            } else if (!has_joint_condition && t_id_prev != t_id) {
              has_joint_condition = true;
            }
            auto c_info = getColumnInfoFromScan(rex_input, node_scan->getId());
            if (!c_info.second.empty()) {
              c_infos_tmp.push_back(std::move(c_info));
            }
          }
        }
        if (has_joint_condition) {
          for (auto& c_info : c_infos_tmp) {
            auto it = std::lower_bound(cols_to_project.begin(), cols_to_project.end(), c_info, [](auto lhs, auto rhs) {
              return lhs.first < rhs.first;
            });
            if (it == cols_to_project.end() || !(*it == c_info)) {
              cols_to_project.emplace(it, std::move(c_info));
            }
          }
          return nullptr;
        }
      }
      for (size_t i = 0; i < oper_size; i++) {
        auto operand_new = findFilterAndProjectFromRex(rex_operator->getOperand(i), node_scan, cols_to_project);
        if (operand_new) {
          operands_new.push_back(std::move(operand_new));
        }
      }
      if (operands_new.size() > 1) {
        return std::unique_ptr<const RexScalar>(
            new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
      } else if (operands_new.size() == 1) {
        if (dynamic_cast<const RexOperator*>(operands_new[0].get()) ||
            dynamic_cast<const RexCase*>(operands_new[0].get())) {
          return std::move(operands_new[0]);
        } else {
          return nullptr;
        }
      } else {
        return nullptr;
      }
    }
    const auto rex_case = dynamic_cast<const RexCase*>(rex);
    if (rex_case) {
      std::vector<std::pair<std::unique_ptr<const RexScalar>, std::unique_ptr<const RexScalar>>> expr_pair_list_new;
      for (size_t i = 0; i < rex_case->branchCount(); i++) {
        auto oper_when_new = findFilterAndProjectFromRex(rex_case->getWhen(i), node_scan, cols_to_project);
        auto oper_then_new = findFilterAndProjectFromRex(rex_case->getThen(i), node_scan, cols_to_project);
        if (oper_when_new && oper_then_new) {
          auto expr_pair_new = std::make_pair(std::move(oper_when_new), std::move(oper_then_new));
          expr_pair_list_new.push_back(std::move(expr_pair_new));
        }
      }
      if (!expr_pair_list_new.empty()) {
        auto else_expr_new = findFilterAndProjectFromRex(rex_case->getElse(), node_scan, cols_to_project);
        if (else_expr_new) {
          return std::unique_ptr<const RexScalar>(new RexCase(expr_pair_list_new, else_expr_new));
        }
      }
      return nullptr;
    }
    const auto rex_ref = dynamic_cast<const RexRef*>(rex);
    if (rex_ref) {
      return rex_ref->deepCopy();
    }
    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query) {
      return nullptr;
    }
    CHECK(rex_input || rex_literal || rex_operator || rex_case || rex_ref || rex_sub_query);
    return nullptr;
  }

  std::unique_ptr<const RexScalar> findFilterAndProjectFromFuncOper(
      const RexScalar* rex,
      std::shared_ptr<RelAlgNode>& node_scan,
      std::vector<std::pair<unsigned, std::string>>& c_infos_tmp) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      auto c_info = getColumnInfoFromScan(rex_input, node_scan->getId());
      if (!c_info.second.empty()) {
        c_infos_tmp.push_back(std::move(c_info));
        return std::unique_ptr<const RexScalar>(new RexInput(node_scan.get(), c_info.first));
      } else {
        return nullptr;
      }
    }
    const auto rex_literal = dynamic_cast<const RexLiteral*>(rex);
    if (rex_literal) {
      return rex_literal->deepCopy();
    }
    const auto rex_func_operator = dynamic_cast<const RexFunctionOperator*>(rex);
    if (rex_func_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      auto oper_size = rex_func_operator->size();
      for (size_t i = 0; i < oper_size; i++) {
        auto operand_new = findFilterAndProjectFromFuncOper(rex_func_operator->getOperand(i), node_scan, c_infos_tmp);
        if (operand_new) {
          operands_new.push_back(std::move(operand_new));
        }
      }
      if (operands_new.size() != oper_size) {
        return nullptr;
      } else {
        return std::unique_ptr<const RexScalar>(
            new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
      }
    }
    CHECK(rex_input || rex_literal || rex_func_operator);
    return nullptr;
  }

  std::pair<unsigned, std::string> getColumnInfoFromScan(const RexInput* rex_input, unsigned scan_id) {
    const auto source = rex_input->getSourceNode();
    const auto c_id = rex_input->getIndex();
    std::vector<std::pair<unsigned, unsigned>> t_num_cols;
    std::vector<std::string> schema;
    std::tie(t_num_cols, schema) = getSchemaFromSource(source);
    unsigned num_cols_so_far = 0;
    for (auto t_num_col : t_num_cols) {
      if (t_num_col.first == scan_id) {
        if (c_id < (num_cols_so_far + t_num_col.second) && c_id >= num_cols_so_far) {
          return std::make_pair(c_id - num_cols_so_far, schema[c_id]);
        }
        break;
      } else {
        num_cols_so_far += t_num_col.second;
      }
    }
    // this input doesn't belong to the given table
    return std::make_pair(-1, "");
  }

  void makeAndInsertCompound(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                             size_t filter_idx,
                             size_t output_idx,
                             std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    std::unique_ptr<const RexScalar> filter_rex;
    std::vector<std::unique_ptr<const RexScalar>> scalar_sources;
    size_t groupby_count{0};
    std::vector<std::string> fields;
    std::vector<const RexAgg*> agg_exprs;
    std::vector<const Rex*> target_exprs;
    // extract filter
    const auto node_filter = std::dynamic_pointer_cast<RelFilter>(nodes[filter_idx]);
    if (node_filter) {
      CHECK(!filter_rex);
      filter_rex.reset(node_filter->getAndReleaseCondition());
      CHECK(filter_rex);
    } else {
      CHECK(false);
    }
    // extract project
    for (auto col : cols_to_project) {
      auto rex_col = std::unique_ptr<const RexScalar>(new RexInput(nodes[filter_idx]->getInput(0), col.first));
      scalar_sources.push_back(std::move(rex_col));
      target_exprs.push_back(scalar_sources.back().get());
      fields.push_back(col.second);
    }
    // create compound
    auto node_compound = std::make_shared<RelCompound>(
        filter_rex, target_exprs, groupby_count, agg_exprs, fields, scalar_sources, false);
    CHECK_EQ(size_t(1), nodes[filter_idx]->inputCount());
    node_compound->addManagedInput(nodes[filter_idx]->getAndOwnInput(0));
    nodes[output_idx]->replaceInput(nodes[filter_idx], node_compound);
    // remove filter and project and insert compound
    nodes.erase(nodes.begin() + filter_idx);
    nodes.insert(nodes.begin() + filter_idx, node_compound);
  }

  bool executeFilterAndEvaluate(const RelAlgNode* node,
                                size_t num_tuples,
                                std::pair<int32_t, int> table_id_phy,
                                RelAlgExecutor* ra_executor) {
    auto executor = Executor::getExecutor(ra_executor->getSessionInfo().get_catalog().get_currentDB().dbId);
    RelAlgExecutor ra_executor_sub(executor.get(), ra_executor->getSessionInfo(), false);
    unsigned fpd_max_count = (unsigned)(num_tuples * PUSH_DOWN_MAX_SELECTIVITY);
    try {
      auto execution_result = ra_executor_sub.executeRelAlgQueryFPD(node, fpd_max_count, table_id_phy);
      ra_executor->addPushDownFilter(-node->getId(), execution_result);
      return true;
    } catch (ssize_t e) {
      return false;
    }
  }

  void updatePostJoinExprs(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                           size_t scan_idx,
                           size_t output_idx,
                           size_t input_idx,
                           std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    for (auto it_nodes = nodes.rbegin(); it_nodes != nodes.rend(); ++it_nodes) {
      auto node_project = std::dynamic_pointer_cast<RelProject>(*it_nodes);
      if (node_project) {
        if (dynamic_cast<const RelAggregate*>(node_project->getInput(0)) ||
            dynamic_cast<const RelProject*>(node_project->getInput(0))) {
          continue;
        }
        std::vector<std::unique_ptr<const RexScalar>> exprs_new;
        for (size_t i = 0; i < node_project->size(); i++) {
          auto rex = node_project->getProjectAtAndRelease(i);
          auto rex_new = buildNewProjectExpr(rex, nodes[scan_idx], nodes[output_idx], input_idx, cols_to_project);
          exprs_new.push_back(std::move(rex_new));
        }
        node_project->setExpressions(exprs_new);
        continue;
      }
      auto node_filter = std::dynamic_pointer_cast<RelFilter>(*it_nodes);
      if (node_filter) {
        auto rex = node_filter->getAndReleaseCondition();
        auto rex_new = buildNewFilterExpr(rex, nodes[scan_idx], nodes[output_idx], input_idx, cols_to_project);
        // empty filter expr handling
        // 1. insert always-true condition: easy
        // if(!rex_new) {
        //   rex_new = std::unique_ptr<const RexScalar>(
        //     new RexLiteral(true, kBOOLEAN, kBOOLEAN, -1, 1, -1, 1));
        // }
        // node_filter->setCondition(rex_new);
        // 2. remove filter from nodes and RexInputs and connect filter's child and parent
        // pro: save time to execute always-true filter
        // con: take extra time to modify tree
        if (rex_new) {
          node_filter->setCondition(rex_new);
        } else {
          removeEmptyFilter(nodes, node_filter.get());
        }
        continue;
      }
      if (std::dynamic_pointer_cast<RelJoin>(*it_nodes)) {
        break;
      }
    }
  }

  std::unique_ptr<const RexScalar> buildNewProjectExpr(const RexScalar* rex,
                                                       std::shared_ptr<RelAlgNode>& node_scan,
                                                       std::shared_ptr<RelAlgNode>& node_output,
                                                       size_t input_idx,
                                                       std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      auto source = rex_input->getSourceNode();
      auto c_id = rex_input->getIndex();
      if (!hasConnection(node_output->getInput(input_idx), source) &&
          !hasConnection(source, node_output->getInput(input_idx))) {
        return std::unique_ptr<const RexScalar>(new RexInput(source, c_id));
      }
      std::vector<std::pair<unsigned, unsigned>> t_num_cols;
      std::vector<std::string> schema;
      std::tie(t_num_cols, schema) = getSchemaFromSource(source);
      int num_cols_so_far = 0;
      int num_modifier = 0;
      for (auto it = t_num_cols.begin(); it != t_num_cols.end(); ++it) {
        if (c_id < num_cols_so_far + it->second) {
          if (it->first == node_scan->getId()) {
            auto it_find =
                std::find_if(cols_to_project.begin(), cols_to_project.end(), [&c_id, &num_cols_so_far](auto& col) {
                  return col.first == (c_id - num_cols_so_far);
                });
            CHECK(it_find != cols_to_project.end());
            auto c_id_new = std::distance(cols_to_project.begin(), it_find) + num_cols_so_far;
            if (node_output->getInput(input_idx)->getInput(0) == source) {
              return std::unique_ptr<const RexScalar>(new RexInput(node_output->getInput(input_idx), c_id_new));
            } else {
              return std::unique_ptr<const RexScalar>(new RexInput(source, c_id_new));
            }
          } else {
            auto c_id_new = c_id - num_modifier;
            return std::unique_ptr<const RexScalar>(new RexInput(source, c_id_new));
          }
        } else {
          num_cols_so_far += it->second;
          if (it->first == node_scan->getId()) {
            num_modifier = it->second - cols_to_project.size();
          }
        }
      }
      CHECK(false);  // something went wrong...
    }
    const auto rex_literal = dynamic_cast<const RexLiteral*>(rex);
    if (rex_literal) {
      return rex_literal->deepCopy();
    }
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      for (size_t i = 0; i < rex_operator->size(); i++) {
        auto oper_new = buildNewProjectExpr(
            rex_operator->getOperandAndRelease(i), node_scan, node_output, input_idx, cols_to_project);
        operands_new.push_back(std::move(oper_new));
      }
      const auto rex_func_operator = dynamic_cast<const RexFunctionOperator*>(rex);
      if (rex_func_operator) {
        return std::unique_ptr<const RexScalar>(
            new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
      } else {
        return std::unique_ptr<const RexScalar>(
            new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
      }
    }
    const auto rex_case = dynamic_cast<const RexCase*>(rex);
    if (rex_case) {
      std::vector<std::pair<std::unique_ptr<const RexScalar>, std::unique_ptr<const RexScalar>>> expr_pair_list_new;
      for (size_t i = 0; i < rex_case->branchCount(); i++) {
        auto oper_when_new =
            buildNewProjectExpr(rex_case->getWhen(i), node_scan, node_output, input_idx, cols_to_project);
        auto oper_then_new =
            buildNewProjectExpr(rex_case->getThen(i), node_scan, node_output, input_idx, cols_to_project);
        auto expr_pair_new = std::make_pair(std::move(oper_when_new), std::move(oper_then_new));
        expr_pair_list_new.push_back(std::move(expr_pair_new));
      }
      auto else_expr_new = buildNewProjectExpr(rex_case->getElse(), node_scan, node_output, input_idx, cols_to_project);
      return std::unique_ptr<const RexScalar>(new RexCase(expr_pair_list_new, else_expr_new));
    }
    const auto rex_ref = dynamic_cast<const RexRef*>(rex);
    if (rex_ref) {
      return rex_ref->deepCopy();
    }
    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query) {
      return nullptr;
    }
    CHECK(rex_input || rex_literal || rex_operator || rex_case || rex_ref || rex_sub_query);
    return nullptr;
  }

  std::unique_ptr<const RexScalar> buildNewFilterExpr(const RexScalar* rex,
                                                      std::shared_ptr<RelAlgNode>& node_scan,
                                                      std::shared_ptr<RelAlgNode>& node_output,
                                                      size_t input_idx,
                                                      std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      auto source = rex_input->getSourceNode();
      auto c_id = rex_input->getIndex();
      if (!hasConnection(node_output->getInput(input_idx), source) &&
          !hasConnection(source, node_output->getInput(input_idx))) {
        return std::unique_ptr<const RexScalar>(new RexInput(source, c_id));
      }
      std::vector<std::pair<unsigned, unsigned>> t_num_cols;
      std::vector<std::string> schema;
      std::tie(t_num_cols, schema) = getSchemaFromSource(source);
      int num_cols_so_far = 0;
      int num_modifier = 0;
      for (auto it = t_num_cols.begin(); it != t_num_cols.end(); ++it) {
        if (c_id < num_cols_so_far + it->second) {
          if (it->first == node_scan->getId()) {
            auto it_find =
                std::find_if(cols_to_project.begin(), cols_to_project.end(), [&c_id, &num_cols_so_far](auto& col) {
                  return col.first == (c_id - num_cols_so_far);
                });
            if (it_find == cols_to_project.end()) {
              return nullptr;  // this column is just for filter, which should be pushed down already
            }
            auto c_id_new = std::distance(cols_to_project.begin(), it_find) + num_cols_so_far;
            if (node_output->getInput(input_idx)->getInput(0)->getId() == source->getId()) {
              return std::unique_ptr<const RexScalar>(new RexInput(node_output->getInput(input_idx), c_id_new));
            } else {
              return std::unique_ptr<const RexScalar>(new RexInput(source, c_id_new));
            }
          } else {
            auto c_id_new = c_id - num_modifier;
            return std::unique_ptr<const RexScalar>(new RexInput(source, c_id_new));
          }
        } else {
          num_cols_so_far += it->second;
          if (it->first == node_scan->getId()) {
            num_modifier = it->second - cols_to_project.size();
          }
        }
      }
    }
    const auto rex_literal = dynamic_cast<const RexLiteral*>(rex);
    if (rex_literal) {
      return rex_literal->deepCopy();
    }
    const auto rex_func_operator = dynamic_cast<const RexFunctionOperator*>(rex);
    if (rex_func_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      auto oper_size = rex_func_operator->size();
      for (size_t i = 0; i < oper_size; i++) {
        auto operand_new =
            buildNewFilterExpr(rex_func_operator->getOperand(i), node_scan, node_output, input_idx, cols_to_project);
        if (operand_new) {
          operands_new.push_back(std::move(operand_new));
        }
      }
      return std::unique_ptr<const RexScalar>(
          new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
    }
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      auto oper_size = rex_operator->size();
      if (oper_size >= 2) {
        std::vector<std::pair<unsigned, std::string>> c_infos_tmp;
        unsigned t_id_prev = 0;
        bool has_joint_condition = false;
        for (size_t i = 0; i < oper_size; i++) {
          const auto rex_input = dynamic_cast<const RexInput*>(rex_operator->getOperand(i));
          if (rex_input) {
            auto t_id = findColumnNameByIndex(rex_input->getSourceNode(), rex_input->getIndex()).first;
            if (t_id_prev == 0) {
              t_id_prev = t_id;
            } else if (!has_joint_condition && t_id_prev != t_id) {
              has_joint_condition = true;
            }
            auto c_info = getColumnInfoFromScan(rex_input, node_scan->getId());
            if (!c_info.second.empty()) {
              c_infos_tmp.push_back(std::move(c_info));
            }
          }
        }
        if (!has_joint_condition) {
          if (!c_infos_tmp.empty()) {  // at least a single column is on this table
            return nullptr;
          }
        }
      }
      for (size_t i = 0; i < oper_size; i++) {
        auto operand_new =
            buildNewFilterExpr(rex_operator->getOperand(i), node_scan, node_output, input_idx, cols_to_project);
        if (operand_new) {
          operands_new.push_back(std::move(operand_new));
        }
      }
      if (operands_new.size() > 1) {
        return std::unique_ptr<const RexScalar>(
            new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
      } else if (operands_new.size() == 1) {
        if (dynamic_cast<const RexOperator*>(operands_new[0].get()) ||
            dynamic_cast<const RexCase*>(operands_new[0].get())) {
          return std::move(operands_new[0]);
        } else {
          return nullptr;
        }
      } else {
        return nullptr;
      }
    }
    const auto rex_case = dynamic_cast<const RexCase*>(rex);
    if (rex_case) {
      std::vector<std::pair<std::unique_ptr<const RexScalar>, std::unique_ptr<const RexScalar>>> expr_pair_list_new;
      for (size_t i = 0; i < rex_case->branchCount(); i++) {
        auto oper_when_new =
            buildNewFilterExpr(rex_case->getWhen(i), node_scan, node_output, input_idx, cols_to_project);
        auto oper_then_new =
            buildNewFilterExpr(rex_case->getThen(i), node_scan, node_output, input_idx, cols_to_project);
        auto expr_pair_new = std::make_pair(std::move(oper_when_new), std::move(oper_then_new));
        expr_pair_list_new.push_back(std::move(expr_pair_new));
      }
      auto else_expr_new = buildNewFilterExpr(rex_case->getElse(), node_scan, node_output, input_idx, cols_to_project);
      return std::unique_ptr<const RexScalar>(new RexCase(expr_pair_list_new, else_expr_new));
    }
    const auto rex_ref = dynamic_cast<const RexRef*>(rex);
    if (rex_ref) {
      return rex_ref->deepCopy();
    }
    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query) {
      return nullptr;
    }
    CHECK(rex_input || rex_literal || rex_operator || rex_case || rex_ref || rex_sub_query);
    return nullptr;
  }

  bool hasConnection(const RelAlgNode* lhs, const RelAlgNode* rhs) {
    if (lhs == rhs) {
      return true;
    }
    for (size_t i = 0; i < rhs->inputCount(); i++) {
      if (lhs == rhs->getInput(i)) {
        return true;
      } else if (hasConnection(lhs, rhs->getInput(i))) {
        return true;
      }
    }
    return false;
  }

  void removeEmptyFilter(std::vector<std::shared_ptr<RelAlgNode>>& nodes, const RelAlgNode* node_filter) {
    for (auto it_nodes = nodes.rbegin(); it_nodes != nodes.rend(); ++it_nodes) {
      auto node_project = std::dynamic_pointer_cast<RelProject>(*it_nodes);
      if (node_project) {
        if (node_project->getInput(0) == node_filter) {
          std::vector<std::unique_ptr<const RexScalar>> exprs_new;
          for (size_t i = 0; i < node_project->size(); i++) {
            auto rex = node_project->getProjectAtAndRelease(i);
            auto rex_new = replaceEmptyFilterSource(rex, node_filter, node_filter->getInput(0));
            exprs_new.push_back(std::move(rex_new));
          }
          node_project->setExpressions(exprs_new);
          node_project->replaceInput(node_project->getAndOwnInput(0), node_filter->getAndOwnInput(0));
          nodes.erase(it_nodes.base() - 2);
          return;
        }
      }
    }
  }

  std::unique_ptr<const RexScalar> replaceEmptyFilterSource(const RexScalar* rex,
                                                            const RelAlgNode* source_old,
                                                            const RelAlgNode* source_new) {
    const auto rex_input = dynamic_cast<const RexInput*>(rex);
    if (rex_input) {
      if (rex_input->getSourceNode() == source_old) {
        return std::unique_ptr<const RexScalar>(new RexInput(source_new, rex_input->getIndex()));
      }
    }
    const auto rex_literal = dynamic_cast<const RexLiteral*>(rex);
    if (rex_literal) {
      return rex_literal->deepCopy();
    }
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      std::vector<std::unique_ptr<const RexScalar>> operands_new;
      for (size_t i = 0; i < rex_operator->size(); i++) {
        auto oper_new = replaceEmptyFilterSource(rex_operator->getOperandAndRelease(i), source_old, source_new);
        operands_new.push_back(std::move(oper_new));
      }
      const auto rex_func_operator = dynamic_cast<const RexFunctionOperator*>(rex);
      if (rex_func_operator) {
        return std::unique_ptr<const RexScalar>(
            new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
      } else {
        return std::unique_ptr<const RexScalar>(
            new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
      }
    }
    const auto rex_case = dynamic_cast<const RexCase*>(rex);
    if (rex_case) {
      std::vector<std::pair<std::unique_ptr<const RexScalar>, std::unique_ptr<const RexScalar>>> expr_pair_list_new;
      for (size_t i = 0; i < rex_case->branchCount(); i++) {
        auto oper_when_new = replaceEmptyFilterSource(rex_case->getWhen(i), source_old, source_new);
        auto oper_then_new = replaceEmptyFilterSource(rex_case->getThen(i), source_old, source_new);
        auto expr_pair_new = std::make_pair(std::move(oper_when_new), std::move(oper_then_new));
        expr_pair_list_new.push_back(std::move(expr_pair_new));
      }
      auto else_expr_new = replaceEmptyFilterSource(rex_case->getElse(), source_old, source_new);
      return std::unique_ptr<const RexScalar>(new RexCase(expr_pair_list_new, else_expr_new));
    }
    const auto rex_ref = dynamic_cast<const RexRef*>(rex);
    if (rex_ref) {
      return rex_ref->deepCopy();
    }
    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query) {
      return nullptr;
    }
    CHECK(rex_input || rex_literal || rex_operator || rex_case || rex_ref || rex_sub_query);
    return nullptr;
  }

  std::unordered_map<const RelAlgNode*, std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>>>
      schema_map_;
};

bool push_down_filter_predicates(std::vector<std::shared_ptr<RelAlgNode>>& nodes, RelAlgExecutor* ra_executor) {
  FilterPushDown fpd;
  std::unordered_map<const RelAlgNode*, std::pair<size_t, bool>> table_sizes;
  size_t max_size_so_far = 0;
  RelAlgNode* max_table = nullptr;

  for (auto it_nodes = nodes.begin(); it_nodes != nodes.end(); ++it_nodes) {
    auto node_scan = std::dynamic_pointer_cast<RelScan>(*it_nodes);
    if (node_scan) {
      if (!std::dynamic_pointer_cast<RelScan>(*(std::next(it_nodes, 1))) &&
          !std::dynamic_pointer_cast<RelJoin>(*(std::next(it_nodes, 1)))) {
        continue;
      }
      auto num_tuples =
          fpd.getNumTuples(node_scan->getTableDescriptor()->tableId, ra_executor->getSessionInfo().get_catalog());

      if (max_size_so_far == 0 || num_tuples > max_size_so_far) {
        max_size_so_far = num_tuples;
        max_table = node_scan.get();
      }
      std::pair<size_t, bool> table_size = std::make_pair(num_tuples, false);
      table_sizes.emplace(node_scan.get(), table_size);
    }

    auto node_filter = std::dynamic_pointer_cast<RelFilter>(*it_nodes);
    if (node_filter) {
      const auto input = node_filter->getInput(0);
      if (dynamic_cast<const RelJoin*>(input)) {
        if (!fpd.extractHashJoinCol(nodes, node_filter->getCondition(), ra_executor)) {
          return false;
        }
        CHECK(max_table);
        table_sizes[max_table].second = true;
        return fpd.evaluateAndPushDown(nodes, ra_executor, table_sizes);
      }
    }
  }
  return false;
}
