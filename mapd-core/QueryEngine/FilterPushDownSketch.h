#ifndef QUERYENGINE_FILTERPUSHDOWN_SKETCH_H
#define QUERYENGINE_FILTERPUSHDOWN_SKETCH_H

#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../Catalog/Catalog.h"
#include "../Shared/sqldefs.h"
#include "../Shared/sqltypes.h"
#include "RelAlgAbstractInterpreter.h"
#include "RelAlgExecutor.h"
#include "TargetValue.h"

bool push_down_filter_predicates_sketch(std::vector<std::shared_ptr<RelAlgNode>>& nodes, RelAlgExecutor* ra_executor);

#endif  // QUERYENGINE_FILTERPUSHDOWN_SKETCH_H
