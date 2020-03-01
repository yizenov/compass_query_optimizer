#ifndef QUERYENGINE_FILTERPUSHDOWN_H
#define QUERYENGINE_FILTERPUSHDOWN_H

#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <algorithm>

#include "RelAlgAbstractInterpreter.h"
#include "RelAlgExecutor.h"
#include "TargetValue.h"
#include "../Catalog/Catalog.h"
#include "../Shared/sqldefs.h"
#include "../Shared/sqltypes.h"

const unsigned PUSH_DOWN_MIN_TABLE_SIZE = 100000;
const float PUSH_DOWN_MAX_SELECTIVITY = 0.03;

bool push_down_filter_predicates(std::vector<std::shared_ptr<RelAlgNode>>& nodes,
  RelAlgExecutor* ra_executor);

#endif // QUERYENGINE_FILTERPUSHDOWN_H