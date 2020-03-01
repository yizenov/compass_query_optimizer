#include "FilterPushDownSketch.h"

class FilterPushDownSketch {
 public:
  // return the number of tuples in a table given by its identifier
  // get it from the catalog
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

  // retrieve join conditions from a set of rel alg nodes and a given expression
  // join conditions are added to the executor
  // return false only in the case of a subquery
  // we have to preserve the columns appearing in a join for each table because
  // we have to build sketches on them
  // we do this with a map [table --> (colIdx, colName)]
  bool extractHashJoinCol(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                          const RexScalar* rex,
                          RelAlgExecutor* ra_executor) {
    const auto rex_operator = dynamic_cast<const RexOperator*>(rex);
    if (rex_operator) {
      auto oper_size = rex_operator->size();

      // this is the base case of a simple operation with two operands
      if (oper_size == 2) {
        const auto rex_input_lhs = dynamic_cast<const RexInput*>(rex_operator->getOperand(0));
        const auto rex_input_rhs = dynamic_cast<const RexInput*>(rex_operator->getOperand(1));

        if (rex_input_lhs && rex_input_rhs) {
          unsigned t_id_lhs, t_id_rhs;
          std::string col_name_lhs, col_name_rhs;
          unsigned col_id_lhs, col_id_rhs;
          std::tie(t_id_lhs, col_name_lhs, col_id_lhs) =
              findColumnNameByIndex(rex_input_lhs->getSourceNode(), rex_input_lhs->getIndex());
          std::tie(t_id_rhs, col_name_rhs, col_id_rhs) =
              findColumnNameByIndex(rex_input_rhs->getSourceNode(), rex_input_rhs->getIndex());

          // look for the RelScan's order in nodes using t_id
          int t_order_lhs = -1, t_order_rhs = -1, t_order = 0;
          int32_t t_id_lhs_phy = -1, t_id_rhs_phy = -1;
          auto lhs_node_id = -1, rhs_node_id = -1;
          for (auto node : nodes) {
            auto node_scan = std::dynamic_pointer_cast<RelScan>(node);
            if (node_scan) {
              if (t_order_lhs < 0 && t_id_lhs == node_scan->getId()) {
                t_order_lhs = t_order;
                t_id_lhs_phy = node_scan->getTableDescriptor()->tableId;
                lhs_node_id = node_scan->getId();
              }
              if (t_order_rhs < 0 && t_id_rhs == node_scan->getId()) {
                t_order_rhs = t_order;
                t_id_rhs_phy = node_scan->getTableDescriptor()->tableId;
                rhs_node_id = node_scan->getId();
              }
              if ((t_order_lhs != -1) && (t_order_rhs != -1)) {
                break;
              }
              t_order++;
            }
          }
          CHECK_GT(t_order_lhs, -1);
          CHECK_GT(t_order_rhs, -1);
          CHECK_GT(t_id_lhs_phy, -1);
          CHECK_GT(t_id_rhs_phy, -1);
          if (t_order_lhs != t_order_rhs) {
            // add the join connection between two relations
            // using physical ids instead of table index
            ra_executor->addHashJoinCol(std::make_pair(t_id_lhs_phy, lhs_node_id),
                                        std::make_pair(t_id_rhs_phy, rhs_node_id));
            // adding predicates involved in the query
            // same seeds/sketch for same attribute type
            addPredicate(ra_executor->getSessionInfo().getSeedTemplates(),
                         std::make_pair(t_id_lhs_phy, lhs_node_id),
                         std::make_pair(t_id_rhs_phy, rhs_node_id),
                         col_id_lhs,
                         col_id_rhs,
                         ra_executor);

            // join column connection information between relations for sketch-based join estimation
            auto key_lhs = std::stoi(std::to_string(t_id_lhs_phy) + std::to_string(lhs_node_id));
            auto key_rhs = std::stoi(std::to_string(t_id_rhs_phy) + std::to_string(rhs_node_id));
            ra_executor->addJoinColConnection(std::make_pair(key_lhs, key_rhs), std::make_pair(col_id_lhs, col_id_rhs));
            ra_executor->addJoinColConnection(std::make_pair(key_rhs, key_lhs), std::make_pair(col_id_rhs, col_id_lhs));

            // table physical and order indices needed in join ordering logic. See in RelAlgExecutor for more details.
            auto tables_info = ra_executor->getTableIndexInfo();
            if (tables_info.find(std::make_pair(t_id_lhs_phy, lhs_node_id)) == tables_info.end()) {
              ra_executor->addTableIndexInfo(std::make_pair(t_id_lhs_phy, lhs_node_id), t_order_lhs);
            }
            if (tables_info.find(std::make_pair(t_id_rhs_phy, rhs_node_id)) == tables_info.end()) {
              ra_executor->addTableIndexInfo(std::make_pair(t_id_rhs_phy, rhs_node_id), t_order_rhs);
            }

            return true;
          }
        }
      }

      // perform recursive extraction from all the parts of a complex expression
      for (size_t i = 0; i < oper_size; i++) {
        const auto operand = rex_operator->getOperand(i);
        bool ret = extractHashJoinCol(nodes, operand, ra_executor);
        if (false == ret)
          return false;
      }

      return true;
    }

    const auto rex_sub_query = dynamic_cast<const RexSubQuery*>(rex);
    if (rex_sub_query)
      return false;

    return true;
  }

  bool evaluateAndPushDown(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
                           RelAlgExecutor* ra_executor,
                           std::vector<std::pair<const RelAlgNode*, size_t>>& table_sizes) {
    std::cout << "------ BEFORE pushDownFilterPredicates ------" << std::endl;
    // std::cout << tree_string(nodes.back().get(), 0) << std::endl;

    // look for each RelScan in ascending order in terms of size so that we can deal with the largest relation at the
    // end to ignore selection push-down on the largest relation if the number of filtered rows is still greater than
    // the second largest table's, i.e., does not affect join ordering
    auto it_table_sizes = table_sizes.begin();
    size_t max_size_so_far = 0;
    size_t i = 0;
    bool skip_filter;  // flag for skip selection push-down
    while (it_table_sizes != table_sizes.end()) {
      auto node_scan = std::dynamic_pointer_cast<RelScan>(nodes[i]);
      // auto table_size = table_sizes[ts_count];
      if (node_scan.get() == it_table_sizes->first) {
        // if next node is neither scan nor join,
        // this is already a derived table, skip
        if (!std::dynamic_pointer_cast<RelScan>(nodes[i + 1]) && !std::dynamic_pointer_cast<RelJoin>(nodes[i + 1])) {
          continue;
        }
        // reset the flag for each RelScan
        skip_filter = false;
        // find filter predicates to push-down and columns to project for this table
        // if there is no filter predicate, skip selection push-down
        std::vector<std::pair<unsigned, std::string>> cols_to_project;
        auto filter_expr_new = findFilterAndProject(nodes, i, cols_to_project);
        // if the size of relation is too small, it is not worth of materialization
        if (!filter_expr_new) {
          skip_filter = true;
        } else if (!ra_executor->getSessionInfo().getPreProcessingSign() && it_table_sizes->second < ra_executor->getSessionInfo().getPD_MinTableSize_SK()) {
          skip_filter = true;
        }

        std::string table_name = node_scan->getTableDescriptor()->tableName;
        unsigned table_id = node_scan->getTableDescriptor()->tableId;
        std::pair<unsigned, int> table_id_phy = std::make_pair(table_id, node_scan->getId());

        if (skip_filter) {  // generate a sketch without filter
          auto clock_begin = timer_start();
          // make cols_to_project contain only join attributes
          auto cols_to_join =
              ra_executor->getSketches()[std::make_pair(node_scan->getTableDescriptor()->tableId, node_scan->getId())]
                  .first;
          CHECK(!cols_to_join.empty());
          if (cols_to_join.size() != cols_to_project.size()) {
            auto it_proj = cols_to_project.begin();
            auto it_join = cols_to_join.begin();
            while (it_proj != cols_to_project.end()) {
              if (it_proj->first == *it_join) {
                ++it_proj;
                ++it_join;
              } else {  // not a join attribute: erase
                cols_to_project.erase(it_proj);
              }
            }
          }
          CHECK_EQ(cols_to_join.size(), cols_to_project.size());
          // create RelProject
          std::vector<std::unique_ptr<const RexScalar>> scalar_exprs;
          std::vector<std::string> fields;
          for (auto col : cols_to_project) {
            scalar_exprs.push_back(std::unique_ptr<const RexScalar>(new RexInput(node_scan.get(), col.first)));
            fields.push_back(col.second);
          }

          // update sketch
          findSketchTemplateAndCopy(table_name, it_table_sizes->second, table_id_phy, ra_executor);

          max_size_so_far = std::max(max_size_so_far, it_table_sizes->second);

          int64_t queue_time_ms = timer_stop(clock_begin);
          std::cout << "time taken for updating sketch without filter: " << queue_time_ms << std::endl;
        } else {
          auto clock_begin = timer_start();

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
          // std::cout << tree_string(nodes.back().get(), 0) << std::endl;

          // make RelCompound and insert into nodes
          makeAndInsertCompound(nodes, i + 1, i + dist, cols_to_project);
          std::cout << "------ AFTER makeAndInsertCompound ------" << std::endl;
          // std::cout << tree_string(nodes.back().get(), 0) << std::endl;

          // update sketch and cloning original template sketch before update
          std::pair<int32_t, int> table_id_phy = std::make_pair(node_scan->getTableDescriptor()->tableId, node_scan->getId());

          // cloning is needed in case of having more than one instance of the same relation
          FAGMS_Sketch* sketch_clone = new FAGMS_Sketch();
          sketch_clone->Reset_Sketch(ra_executor->getSketches()[table_id_phy].second);
          ra_executor->getSketches()[table_id_phy].second = sketch_clone;

          std::cout << std::endl << "table-name: " << table_name
                    << " table-id: " << table_id_phy.first
                    << ", count: " << it_table_sizes->second << ", cols:";
          auto template_columns = ra_executor->getSketches()[table_id_phy].first;
          for (auto col : template_columns) {
            std::cout << " " << col;
          }
          std::cout << std::endl;

          ssize_t num_tuples_filtered =
              executeFilterAndEvaluate(nodes[i + 1].get(), it_table_sizes->second,
                                       std::next(it_table_sizes) == table_sizes.end() ? max_size_so_far : -1,
                                       table_id_phy, ra_executor, cols_to_project);

          // this is true only when we create pre-calculated sketch templates
          if (ra_executor->getSessionInfo().getPreProcessingSign()) {
            if (updateTemplateSketch(table_id_phy, ra_executor))
              std::cout << "table-sketch-template: " << table_name << " has been updated" << std::endl;
            else
              std::cout << "table-sketch-template: " << table_name << " hasn't been updated" << std::endl;
          }

          // if num_tuples_filtered is -1, then push-down was reverted
          if (num_tuples_filtered > 0) {
            // update rex_input in post-join nodes
            updatePostJoinExprs(nodes, i, i + dist, input_idx, cols_to_project);
            max_size_so_far = std::max(max_size_so_far, (size_t)num_tuples_filtered);
            std::cout << "------ AFTER updatePostJoinExprs ------" << std::endl;
            // std::cout << tree_string(nodes.back().get(), 0) << std::endl;
            schema_map_.clear();
          } else {
            // remove filter/compound
            auto node_to_erase = node_output->getAndOwnInput(input_idx);
            node_output->replaceInput(node_to_erase, old_input);
            nodes.erase(nodes.begin() + i + 1);
            max_size_so_far = std::max(max_size_so_far, it_table_sizes->second);

            std::cout << "SELECTION WAS REVERTED: " << table_name << std::endl;
            // since selection is reverted, we use pre-calculated sketches
//            if (!ra_executor->getSessionInfo().getPreProcessingSign())
//              findSketchTemplateAndCopy(table_name, it_table_sizes->second, table_id_phy, ra_executor);
          }

          int64_t queue_time_ms = timer_stop(clock_begin);
          std::cout << "time taken for updating sketch using filter: " << queue_time_ms << std::endl;
        }
        // advance iterator and reset counter to look up next larger relation
        ++it_table_sizes;
        i = 0;
        continue;
      }
      // keep iterate over nodes until we find the target node
      i++;
    }
    // sample code for sketch-based join size estimation
    std::cout << "------ SKETCH-BASED JOIN SIZE ESTIMATION ------" << std::endl;

    std::cout << "------ AFTER pushDownFilterPredicates ------" << std::endl;
    // std::cout << tree_string(nodes.back().get(), 0) << std::endl;

    return true;
  }

  // access to all predicates that involved in current query
  // key is a pair of table_id and node_id, the value is a vector of pairs(column_id, seed)
  std::unordered_map<std::pair<unsigned, unsigned>,
                     std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>>,
                     pair_hash>&
  getPredicates() {
    return table_predicates_;
  }

  void findSketchTemplateAndCopy(std::string& tableName, size_t& table_size,
                                 std::pair<unsigned, int>& table_id_phy, RelAlgExecutor* ra_executor) {

    auto sketchTemplates = ra_executor->getSessionInfo().getSketchTemplates();
    auto fagms_instance = ra_executor->getSketches()[table_id_phy];
    auto fagms_instance_cols = fagms_instance.first;

    bool isTableFound = false;
    std::vector<std::tuple<bool, std::vector<unsigned>, FAGMS_Sketch*>>* sketch_info;
    for (auto it = sketchTemplates->begin(); it != sketchTemplates->end(); ++it) {
      if (it->first.first == table_id_phy.first) {
        isTableFound = true;
        sketch_info = &it->second;
        break;
      }
    }

    if (!isTableFound) {
      // do nothing
      std::cout << "table-name: " << tableName;
      std::cout << " table-id: " << table_id_phy.first;
      std::cout << " -- template sketch is not found" << std::endl;
    } else {
      for (auto it = sketch_info->begin(); it != sketch_info->end(); ++it) {
        auto template_columns = std::get<1>(*it);
        std::vector<unsigned> col_indices;
        unsigned column_counter = 0;
        for (unsigned i = 0; i < fagms_instance_cols.size(); i++) {
          for (unsigned j = 0; j < template_columns.size(); j++) {
            if (template_columns[j] == fagms_instance_cols[i]) {
              column_counter += 1;
              col_indices.push_back(j);
              break;
            }
          }
        }
        if (column_counter == fagms_instance_cols.size()) {
          std::cout << std::endl << "table-name: " << tableName
                    << " table-id: " << table_id_phy.first
                    << ", count: " << table_size << ", cols:";
          auto template_columns = std::get<1>(*it);
          for (auto col_idx : col_indices) {
            std::cout << " " << template_columns[col_idx];
          }
          std::cout << std::endl;

          if (!std::get<0>(*it)) { // to avoid double sketch template update
            std::cout << " -- PRE-PROCESSED SKETCH WASN'T FOUND." << std::endl;
          } else {
            std::cout << " -- sketch template is already update" << std::endl;

            // creating a copy of template sketch since we clear fagms_sketches
            FAGMS_Sketch* sketch_clone = new FAGMS_Sketch();
            sketch_clone->Clone_Sketch(ra_executor->getSketches()[table_id_phy].second, std::get<2>(*it)->get_separate_sketch_elements(), col_indices);
            ra_executor->getSketches()[table_id_phy].second = sketch_clone;

            std::pair<unsigned int, unsigned int> table_data = {table_id_phy.first, table_id_phy.second};
            unsigned int sketch_complexity = column_counter * table_size;
            ra_executor->getSketchComplexity().emplace_back(std::make_pair(table_data, sketch_complexity));
          }
          break;
        } else {
          std::cout << "vector of template sketch is not found" << std::endl;
        }
        col_indices.clear();
      }
    }
  }

  // this function is active when it creates sketch templates
  bool updateTemplateSketch(std::pair<unsigned, int> table_id_phy, RelAlgExecutor* ra_executor) {
    auto sketchTemplates = ra_executor->getSessionInfo().getSketchTemplates();
    auto fagms_instance = ra_executor->getSketches()[table_id_phy];
    auto fagms_instance_cols = fagms_instance.first;

    bool isTableFound = false;
    std::vector<std::tuple<bool, std::vector<unsigned>, FAGMS_Sketch*>>* sketch_info;
    for (auto it = sketchTemplates->begin(); it != sketchTemplates->end(); ++it) {
      if (it->first.first == table_id_phy.first) {
        isTableFound = true;
        sketch_info = &it->second;
        break;
      }
    }

    if (!isTableFound) {
      // do nothing
      std::cout << "sketch templates wasn't found during template creation" << std::endl;
      return false;
    } else {
      std::cout << "new writing sketch templates" << std::endl;
      for (auto it = sketch_info->begin(); it != sketch_info->end(); ++it) {
        auto template_columns = std::get<1>(*it);
        std::vector<unsigned> col_indices;
        unsigned column_counter = 0;
        for (unsigned i = 0; i < fagms_instance_cols.size(); i++) {
          for (unsigned j = 0; j < template_columns.size(); j++) {
            if (template_columns[j] == fagms_instance_cols[i]) {
              column_counter += 1;
              col_indices.push_back(j);
              break;
            }
          }
        }
        if (column_counter == fagms_instance_cols.size()) {
          // if status value is zero, then it is first time of creation current template
          if (std::get<0>(*it) == 0) {
            // updating the template sketch
            std::get<0>(*it) = true;
            std::get<2>(*it) = ra_executor->getSketches()[table_id_phy].second;
            return true;
          } else {
            // PRE-PROCESSED SKETCH is already up-to-date.
            std::cout << "duplicate re-writing sketch templates" << std::endl;
            return false;
          }
        } else {
          std::cout << "columns didn't match while template creation" << std::endl;
        }
      }
      return false;
    }
  }

 private:
  // find the attribute name of a column given by its index in the rel alg node
  // tuple: node_id, column_name and column_id
  std::tuple<unsigned, std::string, unsigned> findColumnNameByIndex(const RelAlgNode* source, unsigned c_id) {
    std::vector<std::pair<unsigned, unsigned>> t_num_cols;
    std::vector<std::string> schema;
    std::tie(t_num_cols, schema) = getSchemaFromSource(source);
    CHECK(!t_num_cols.empty());

    std::tuple<int, std::string, int> c_info;
    unsigned num_cols_so_far = 0;
    for (auto it = t_num_cols.begin(); it != t_num_cols.end(); ++it) {
      if (c_id < num_cols_so_far + it->second) {
        auto col_id = c_id - num_cols_so_far;
        c_info = std::make_tuple(it->first, schema[c_id], col_id);
        break;
      } else {
        num_cols_so_far += it->second;
      }
    }

    CHECK_GT(std::get<0>(c_info), -1);

    return c_info;
  }

  // get the schema of a node in the relational algebra tree
  std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>> getSchemaFromSource(
      const RelAlgNode* node) {
    auto it = schema_map_.find(node);
    if (it != schema_map_.end())
      return it->second;
    else {
      auto schema_new = buildSchemaFromSource(node);
      schema_map_[node] = schema_new;
      return schema_new;
    }
  }

  // build schema for a node
  // schema consists of a pair of (node_id, num_atts) pairs and a vector of att names
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
      unsigned int t_source_id = findBaseTableId(node_project->getInput(0));
      t_num_cols.emplace_back(t_source_id, node_project->size());
      schema = std::make_pair(t_num_cols, node_project->getFields());
    }

    // RelAggregate
    const auto node_aggregate = dynamic_cast<const RelAggregate*>(node);
    if (node_aggregate) {
      unsigned int t_source_id = findBaseTableId(node_aggregate->getInput(0));
      t_num_cols.emplace_back(t_source_id, node_aggregate->size());
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
      unsigned int t_source_id = findBaseTableId(node_compound->getInput(0));
      t_num_cols.emplace_back(t_source_id, node_compound->size());
      schema = std::make_pair(t_num_cols, node_compound->getFields());
    }

    CHECK(node_scan || node_filter || node_project || node_aggregate || node_sort || node_join || node_compound);

    return schema;
  }

  // recursive method to find the base table on a left subtree
  unsigned int findBaseTableId(const RelAlgNode* node) {
    const auto node_scan = dynamic_cast<const RelScan*>(node);
    if (node_scan)
      return node_scan->getId();
    else
      return findBaseTableId(node->getInput(0));
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
            auto t_id = std::get<0>(findColumnNameByIndex(rex_input->getSourceNode(), rex_input->getIndex()));
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
        // preserve the internal operator in case of NOT and ISNOTNULL operators
        if (rex_operator->getOperator() == kNOT || rex_operator->getOperator() == kISNOTNULL
                                                   || rex_operator->getOperator() == kISNULL) {
          return std::unique_ptr<const RexScalar>(
              new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
        } else if (dynamic_cast<const RexOperator*>(operands_new[0].get()) ||
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

  RelCompound makeCompound(RelAlgNode* node, std::vector<std::pair<unsigned, std::string>>& cols_to_project) {
    std::unique_ptr<const RexScalar> filter_rex;
    std::vector<std::unique_ptr<const RexScalar>> scalar_sources;
    size_t groupby_count{0};
    std::vector<std::string> fields;
    std::vector<const RexAgg*> agg_exprs;
    std::vector<const Rex*> target_exprs;

    // extract project
    for (auto col : cols_to_project) {
      auto rex_col = std::unique_ptr<const RexScalar>(new RexInput(node->getInput(0), col.first));
      scalar_sources.push_back(std::move(rex_col));
      target_exprs.push_back(scalar_sources.back().get());
      fields.push_back(col.second);
    }
    // create compound
    RelCompound node_compound =
        RelCompound(filter_rex, target_exprs, groupby_count, agg_exprs, fields, scalar_sources, false);
    CHECK_EQ(size_t(1), node->inputCount());
    node_compound.addManagedInput(node->getAndOwnInput(0));
    return node_compound;
  }

  ssize_t executeFilterAndEvaluate(const RelAlgNode* node,
                                   size_t num_tuples,
                                   ssize_t max_size_so_far,
                                   const std::pair<int32_t, int> table_id_phy,
                                   RelAlgExecutor* ra_executor,
                                   std::vector<std::pair<unsigned, std::string>> cols_to_project) {
    auto executor = Executor::getExecutor(ra_executor->getSessionInfo().get_catalog().get_currentDB().dbId);
    size_t fpd_max_count = num_tuples * ra_executor->getSessionInfo().getPD_MaxSelectivity_SK();
    if (fpd_max_count > ra_executor->getSessionInfo().getPD_MaxSize()) fpd_max_count = ra_executor->getSessionInfo().getPD_MaxSize();
//    if (max_size_so_far >= 0) {
//      fpd_max_count = std::min(fpd_max_count, (size_t)max_size_so_far);
//    }
    auto execution_result = ra_executor->executeRelAlgQueryFPD(node, fpd_max_count, table_id_phy);
    // sample code for reading raw data from ExecutionResult
    // need cols_to_project to match with join attributes for sketch update
    if (!execution_result.getTargetsMeta().empty()) {
      return ra_executor->addPushDownFilter(-node->getId(), execution_result);
    } else {
      return -1;
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
      if (oper_size == operands_new.size()) {
        return std::unique_ptr<const RexScalar>(
            new RexFunctionOperator(rex_func_operator->getName(), operands_new, rex_func_operator->getType()));
      } else {
        return nullptr;
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
            auto t_id = std::get<0>(findColumnNameByIndex(rex_input->getSourceNode(), rex_input->getIndex()));
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
        // preserve the internal operator in case of NOT and ISNOTNULL operators
        if (rex_operator->getOperator() == kNOT || rex_operator->getOperator() == kISNOTNULL
                                                   || rex_operator->getOperator() == kISNULL) {
          return std::unique_ptr<const RexScalar>(
              new RexOperator(rex_operator->getOperator(), operands_new, rex_operator->getType()));
        } else if (dynamic_cast<const RexOperator*>(operands_new[0].get()) ||
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

  // schema at each node in the query execution tree
  std::unordered_map<const RelAlgNode*, std::pair<std::vector<std::pair<unsigned, unsigned>>, std::vector<std::string>>>
      schema_map_;

  // key is a pair of table_id and node_id, value is a vector of pairs(column_id, seed)
  std::unordered_map<std::pair<unsigned, unsigned>,
                     std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>>,
                     pair_hash>
      table_predicates_;

  // inserting column names into respective vector
  void addPredicate(
      std::unordered_map<std::pair<unsigned, unsigned>, std::pair<Xi_CW2B*, Xi_EH3*>, pair_hash>* seed_templates,
      std::pair<int32_t, unsigned> table_id_lhs_phy,
      std::pair<int32_t, unsigned> table_id_rhs_phy,
      unsigned col_lhs_id,
      unsigned col_rhs_id,
      RelAlgExecutor* ra_executor) {

    auto it_lhs = getPredicates().find(table_id_lhs_phy);
    std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>>::iterator it_col_lhs;
    if (it_lhs != getPredicates().end()) {
      it_col_lhs = std::find_if(
          it_lhs->second.begin(), it_lhs->second.end(), [&](auto const& ref) { return ref.first == col_lhs_id; });
    } else {
      std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>> table_columns;
      getPredicates()[table_id_lhs_phy] = table_columns;
      it_lhs = getPredicates().find(table_id_lhs_phy);
      it_col_lhs = it_lhs->second.end();
    }

    auto it_rhs = getPredicates().find(table_id_rhs_phy);
    std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>>::iterator it_col_rhs;
    if (it_rhs != getPredicates().end()) {
      it_col_rhs = std::find_if(
          it_rhs->second.begin(), it_rhs->second.end(), [&](auto const& ref) { return ref.first == col_rhs_id; });
    } else {
      std::vector<std::pair<unsigned, std::pair<Xi_CW2B*, Xi_EH3*>>> table_columns;
      getPredicates()[table_id_rhs_phy] = table_columns;
      it_rhs = getPredicates().find(table_id_rhs_phy);
      it_col_rhs = it_rhs->second.end();
    }

    // search for existing seeds first in RA
    if (it_col_lhs != it_lhs->second.end() && it_col_rhs == it_rhs->second.end()) {
      auto seed_lhs = it_col_lhs->second;
      it_rhs->second.emplace(it_col_rhs, col_rhs_id, seed_lhs);
    } else if (it_col_lhs == it_lhs->second.end() && it_col_rhs != it_rhs->second.end()) {
      auto seed_rhs = it_col_rhs->second;
      it_lhs->second.emplace(it_col_lhs, col_lhs_id, seed_rhs);
    } else if (it_col_lhs == it_lhs->second.end() && it_col_rhs == it_rhs->second.end()) {
      auto lhs_table_info = std::make_pair(table_id_lhs_phy.first, col_lhs_id);
      auto rhs_table_info = std::make_pair(table_id_rhs_phy.first, col_rhs_id);
      auto lhs_seed_info = seed_templates->find(lhs_table_info);
      auto rhs_seed_info = seed_templates->find(rhs_table_info);

      // generate new seeds if there is no such table_info key in either of tables
      if (lhs_seed_info != seed_templates->end() && rhs_seed_info != seed_templates->end()) {

        if ((*seed_templates)[lhs_table_info].first[0].seeds[0] != (*seed_templates)[rhs_table_info].first[0].seeds[0] ||
          (*seed_templates)[lhs_table_info].first[0].seeds[1] != (*seed_templates)[rhs_table_info].first[0].seeds[1] ||
          (*seed_templates)[lhs_table_info].second[0].seeds[0] != (*seed_templates)[rhs_table_info].second[0].seeds[0] ||
          (*seed_templates)[lhs_table_info].second[0].seeds[1] != (*seed_templates)[rhs_table_info].second[0].seeds[1]) {
          std::cout << "DIFFERENT SEEDS: " << lhs_table_info.first << " " << lhs_table_info.second
            << " : " << rhs_table_info.first << " " << rhs_table_info.second << std::endl;
        }

      } else if (lhs_seed_info == seed_templates->end() && rhs_seed_info != seed_templates->end()) {
        (*seed_templates)[lhs_table_info] = (*seed_templates)[rhs_table_info];
      } else if (lhs_seed_info != seed_templates->end() && rhs_seed_info == seed_templates->end()) {
        (*seed_templates)[rhs_table_info] = (*seed_templates)[lhs_table_info];
      } else if (lhs_seed_info == seed_templates->end() && rhs_seed_info == seed_templates->end()) {
        std::cout << "NEW SEEDS WERE GENERATED: " << lhs_table_info.first << " " << lhs_table_info.second
                  << " : " << rhs_table_info.first << " " << rhs_table_info.second << std::endl;
        auto seed = generateSeeds(ra_executor);
        (*seed_templates)[lhs_table_info] = seed;
        (*seed_templates)[rhs_table_info] = seed;
      }
      it_lhs->second.emplace(it_col_lhs, col_lhs_id, (*seed_templates)[lhs_table_info]);
      it_rhs->second.emplace(it_col_rhs, col_rhs_id, (*seed_templates)[rhs_table_info]);
    }
  }

  // generating seeds for sketches
  std::pair<Xi_CW2B*, Xi_EH3*> generateSeeds(RelAlgExecutor* ra_executor) {
    auto row_no = ra_executor->getSessionInfo().getRowNumbers();
    Xi_CW2B* xi_b_seed = new Xi_CW2B[row_no];
    Xi_EH3* xi_pm1_seed = new Xi_EH3[row_no];

    unsigned int I1, I2;

    for (unsigned i = 0; i < row_no; i++) {
      I1 = RandomGenerate();
      I2 = RandomGenerate();
      Xi_CW2B tb(I1, I2, ra_executor->getSessionInfo().getBucketSize());
      xi_b_seed[i] = tb;

      I1 = RandomGenerate();
      I2 = RandomGenerate();
      Xi_EH3 tp(I1, I2);
      xi_pm1_seed[i] = tp;
    }

    return std::make_pair(xi_b_seed, xi_pm1_seed);
  }
};

bool push_down_filter_predicates_sketch(std::vector<std::shared_ptr<RelAlgNode>>& nodes, RelAlgExecutor* ra_executor) {
  auto all_push_down_begin = timer_start();

  FilterPushDownSketch fpd;
  std::vector<std::pair<const RelAlgNode*, size_t>> table_sizes;

  srand(time(0));

  auto SKETCH_BUCKETS = ra_executor->getSessionInfo().getBucketSize();
  auto SKETCH_ROWS = ra_executor->getSessionInfo().getRowNumbers();

  auto sketch_gpu_init_begin = timer_start();
  // get these values from the executor
  //  device_type == ExecutorDeviceType::CPU;
  //  int8_t warp_size = warpSize();
  //  unsigned grid_size = gridSize(), block_size = blockSize();

  ra_executor->initializeSketchInitValues(SKETCH_ROWS, SKETCH_BUCKETS, ra_executor->getSessionInfo().getTotalThreads_PerBlock(),
          ra_executor->getSessionInfo().getTotalGPU_BlockNbr());
  int64_t sketch_gpu_init_stop = timer_stop(sketch_gpu_init_begin);
  std::cout << "time taken for sketch init for gpu: " << sketch_gpu_init_stop << std::endl;

  for (auto it_nodes = nodes.begin(); it_nodes != nodes.end(); ++it_nodes) {
    auto node_scan = std::dynamic_pointer_cast<RelScan>(*it_nodes);
    if (node_scan) {
      if (!std::dynamic_pointer_cast<RelScan>(*(std::next(it_nodes, 1))) &&
          !std::dynamic_pointer_cast<RelJoin>(*(std::next(it_nodes, 1)))) {
        continue;
      }
      auto num_tuples =
          fpd.getNumTuples(node_scan->getTableDescriptor()->tableId, ra_executor->getSessionInfo().get_catalog());
      table_sizes.emplace_back(node_scan.get(), num_tuples);
    }

    auto node_filter = std::dynamic_pointer_cast<RelFilter>(*it_nodes);
    if (node_filter) {
      const auto input = node_filter->getInput(0);
      if (dynamic_cast<const RelJoin*>(input)) {
        auto extract_hash_join_begin = timer_start();
        if (!fpd.extractHashJoinCol(nodes, node_filter->getCondition(), ra_executor)) {
          std::cout << "EXTRACT HASH JOIN COL returned false" << std::endl;
          return false;
        }
        int64_t extract_hash_join_stop = timer_stop(extract_hash_join_begin);
        std::cout << "time taken for extract hash join col: " << extract_hash_join_stop << std::endl;

        auto sketch_gen_begin = timer_start();
        auto sketchTemplates = ra_executor->getSessionInfo().getSketchTemplates();

        // create sketches based on recently created seeds
        auto predicates = fpd.getPredicates();
        for (auto it_predicate = predicates.begin(); it_predicate != predicates.end(); ++it_predicate) {
          auto cols = it_predicate->second;
          auto attribute_nbr = cols.size();
          std::pair<unsigned, int> table_info = std::make_pair(it_predicate->first.first, attribute_nbr);

          if (attribute_nbr > 1) {  // connecting relation
            Xi_CW2B** xi_b_seed = new Xi_CW2B*[attribute_nbr];
            Xi_EH3** xi_pm1_seed = new Xi_EH3*[attribute_nbr];

            // sorting the columns
            int counter = 0;
            std::vector<unsigned> columns = {};
            for (auto it_col = cols.begin(); it_col != cols.end(); ++it_col) {
              auto it = std::lower_bound(
                  columns.begin(), columns.end(), it_col->first, [](auto lhs, auto rhs) { return lhs < rhs; });
              columns.insert(it, it_col->first);  // need to maintain sorted vector for sketch insert purposes
              counter++;
            }

            // maintaining the same order of sketches as column orders
            int idx = 0;
            for (auto col = columns.begin(); col != columns.end(); col++) {
              for (auto it_col = cols.begin(); it_col != cols.end(); it_col++) {
                if (it_col->first == *col) {
                  xi_b_seed[idx] = it_col->second.first;
                  xi_pm1_seed[idx] = it_col->second.second;
                  break;
                }
              }
              idx++;
            }

            // this code section is used to create sketch templates and sketches for current RA sketches
            auto sketch_info = sketchTemplates->find(table_info);
            if (sketch_info == sketchTemplates->end()) {
              auto new_sketch = ra_executor->createAndAddSketch(
                  it_predicate->first, columns, attribute_nbr, SKETCH_BUCKETS, SKETCH_ROWS, xi_b_seed, xi_pm1_seed);
              if (ra_executor->getSessionInfo().getPreProcessingSign()) {
                std::vector<std::tuple<bool, std::vector<unsigned>, FAGMS_Sketch *>> sketch_template_vector;
                sketch_template_vector.emplace_back(std::make_tuple(false, columns, new_sketch));
                sketchTemplates->insert(std::make_pair(table_info, sketch_template_vector));
              }
            } else {
              // looking for specific sketch i.e. with the same join attributes
              bool generateNewSketch = true;
              FAGMS_Sketch* existing_sketch;
              for (auto it = sketch_info->second.begin(); it != sketch_info->second.end(); ++it) {
                auto template_columns = std::get<1>(*it);
                bool isFound = true;
                for (unsigned i = 0; i < columns.size(); i++) {
                  if (template_columns[i] != columns[i]) {
                    isFound = false;
                    break;
                  }
                }
                if (isFound) {
                  generateNewSketch = false;
                  existing_sketch = std::get<2>(*it);
                  break;
                }
              }

              if (generateNewSketch) {
                auto new_sketch = ra_executor->createAndAddSketch(
                    it_predicate->first, columns, attribute_nbr, SKETCH_BUCKETS, SKETCH_ROWS, xi_b_seed, xi_pm1_seed);
                if (ra_executor->getSessionInfo().getPreProcessingSign())
                  sketch_info->second.emplace_back(std::make_tuple(false, columns, new_sketch));
              } else {
                // sketch is found in the templates but note it is not yet clear whether we should keep it
                ra_executor->getSketches().emplace(it_predicate->first, std::make_pair(columns, existing_sketch));
                ra_executor->getSketchStatuses()[table_info] = true;
              }
            }

          } else if (attribute_nbr == 1) {  // base relation
            auto it_pair = cols.back();
            std::vector<unsigned> columns = {it_pair.first};
            auto xi_b_seed = it_pair.second.first;
            auto xi_pm1_seed = it_pair.second.second;

            // this code section is used to create sketch templates and sketches for current RA sketches
            auto sketch_info = sketchTemplates->find(table_info);
            if (sketch_info == sketchTemplates->end()) {
              auto new_sketch = ra_executor->createAndAddSketch(
                  it_predicate->first, it_pair.first, SKETCH_BUCKETS, SKETCH_ROWS, xi_b_seed, xi_pm1_seed);
              if (ra_executor->getSessionInfo().getPreProcessingSign()) {
                std::vector<std::tuple<bool, std::vector<unsigned>, FAGMS_Sketch*>> sketch_template_vector;
                sketch_template_vector.emplace_back(std::make_tuple(false, columns, new_sketch));
                sketchTemplates->insert(std::make_pair(table_info, sketch_template_vector));
              }
            } else {
              // looking for specific sketch i.e. with the same join attributes
              bool generateNewSketch = true;
              FAGMS_Sketch* existing_sketch;
              for (auto it = sketch_info->second.begin(); it != sketch_info->second.end(); ++it) {
                auto template_columns = std::get<1>(*it);
                bool isFound = true;
                for (unsigned i = 0; i < columns.size(); i++) {
                  if (template_columns[i] != columns[i]) {
                    isFound = false;
                    break;
                  }
                }
                if (isFound) {
                  generateNewSketch = false;
                  existing_sketch = std::get<2>(*it);
                  break;
                }
              }

              if (generateNewSketch) {
                auto new_sketch = ra_executor->createAndAddSketch(
                    it_predicate->first, it_pair.first, SKETCH_BUCKETS, SKETCH_ROWS, xi_b_seed, xi_pm1_seed);
                if (ra_executor->getSessionInfo().getPreProcessingSign())
                  sketch_info->second.emplace_back(std::make_tuple(false, columns, new_sketch));
              } else {
                // sketch is found in the templates but note it is not yet clear whether we should keep it
                // RA receive the pointer to the sketch
                ra_executor->getSketches().emplace(it_predicate->first, std::make_pair(columns, existing_sketch));
                ra_executor->getSketchStatuses()[table_info] = true;
              }
            }
          } else {
            std::cout << "NO JOIN PREDICATES FOUND FOR TABLE: " << table_info.first << std::endl;
          }
        }

        fpd.getPredicates().clear();

        int64_t sketch_gen_stop = timer_stop(sketch_gen_begin);
        std::cout << "time taken for init sketch generation: " << sketch_gen_stop << std::endl;

        // make table_sizes order by size ASC to deal with smaller relation first
        auto filter_evaluate_begin = timer_start();
        // this is sorted only once
        std::sort(table_sizes.begin(), table_sizes.end(), [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
        bool status = fpd.evaluateAndPushDown(nodes, ra_executor, table_sizes);
        int64_t filter_evaluate_stop = timer_stop(filter_evaluate_begin);
        std::cout << std::endl << "time taken for all push down and sketch update: " << filter_evaluate_stop << std::endl;
        if (ra_executor->getSessionInfo().getPreProcessingSign())
            ra_executor->getSessionInfo().saveSketches();  // if we need to save template sketches into a file
        int64_t all_push_down_stop = timer_stop(all_push_down_begin);
        std::cout << "time taken for entire push down logic: " << all_push_down_stop << std::endl;
        return status;
      }
    }
  }
  int64_t all_push_down_stop = timer_stop(all_push_down_begin);
  std::cout << "time taken for entire push down logic (returned false): " << all_push_down_stop << std::endl;
  return false;
}
