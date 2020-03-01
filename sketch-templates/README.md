# Pre-build sketch templates.
Details on pre-build templates on relations in case of no selection predicates. For Join Order Benchmark we built necessary sketch templates and seeds.

- Variable `PRE_PROCESSING` is set to `0` i.e. `true` in `COMPASS_init_variables.txt`. This is when one decides to build new sketche templates with different sketch size for relations with no selection predicates.
- Templates are stored in `sketch_tempates.txt` and seeds are stored in `sketch_templates_seeds.txt`.

The templates are written in the following way:
- each template contains three consequetive rows.
- first row is `table_id`, `columns_nbr`, `status` and column indices that are separated by space.
- second row is `bucket size` and `row nbr`.
- third row is sketch values. First index value is column index. Sketch values are stored as an array.

The seeds are written in the following way:
- each seed template contains four consequetive rows.
- first row is `table_id`, `columns_nbr`, `status` and column indices that are separated by space.
- second row is `bucket size` and `row nbr`.
- third and forth rows are seed values. Each seed is a tuple of two numbers.
  - third row seeds correspond to `Xi_CW2B` values
  - fourth row seeds correspond to `Xi_EH3` values.

See `uploadSketches` and `saveSketches` methods for details in `mapd-core/Catalog/Catalog.h` file.
