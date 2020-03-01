# Join Order Benchmark changes
In several queries, MapD system outputs errors because of its query parser. Here we put the changes we had to make in order to successfully run all workload queries.

- For simplicity we replace projection with `COUNT(*)` for all queries.
- Replace nickname for `aka_title` from `at` to `att` in all queries.
- Replace `rt.role` to `rt.role_name` in all queries.

# Updated Query Selectivity Predicates
Queries that selection predicated are changed for MapD system in order to have the same selectivities as in the other three database systems.

- In queries: 1a, 1b, 1c, 1d, 5c, 8a, 8b, 22a, 22b, 22c, 28a, 28b, 28c
  - added `OR mc.note IS NULL` selection predicate.
- In queries: 4a, 4b, 4c, 12a, 12c, 14a, 14b, 14c, 18b, 22a, 22b, 22c, 22d, 26a, 26b, 28a, 28b, 28c, 33a, 33b, 33c
  - `mi_idx.info > '5.0'` selection predicate is replaced by `mi_idx.info = '5.0'`.
- In queries: 11a, 11b, 11c, 11d, 21a, 21b, 21c, 22a, 22b, 22c, 22d, 27a, 27b, 27c, 28a, 28b, 28c, 33c
  - `cn.country_code != '[pl]'` selection is replaced by `cn.country_code <> '[pl]'`.
- In queries: 11c, 11d
  - `ct.kind != 'production companies'` is replaced by `ct.kind <> 'production companies'`
  - `ct.kind IS NOT NULL` is removed.
- In queries: 13b, 13c:
  - `t.title != ''` is replaced by `t.title <> ''`.
- In query: 18b:
  - `mi.note IS NULL` is removed.
- In queries: 28a, 28b
  - `cct2.kind != 'complete+verified'` is replaced by `cct2.kind <> 'complete+verified'`.

These changes had to be made because of different selectivies in four database systems. The differences may be caused by the database systems' parsers and engines.
