# IMDB dataset changes
There are 21 relations in total in IMDB data. In order to be able create `imdb` database with the same schemas in MapD system we had to update syntax of schemas and in order to fully upload entire dataset without missing a tuple we parsed several of the csv files.

# Changes and replacements on original schema:

- Character varying -- TEXT ENCODING DICT
- Text length in MapD can be only 8 or 16 or 32 or None bits.
- In relation `role_type` attribute name `role` to `role_name`
- In relations `name` set attributes `gender` from char(1) to encoding dict(8)
- Text encoding length from 8 to 16 bits. Since many attributes values didn't fit in 8 bits. In relations:
  - `title`, `keyword` and `aka_title` set attribute `phonetic_code`.
  - `name` set attribute `name_pcode_cf`.
  - `name`, `company_name` and `char_name` set attribute `name_pcode_nf`.
  - `company_name` set attribute `name_pcode_sf`.
  - `name` and `char_name` set attribute `surname_pcode`.

# Changes on original data:
Several csv files have tuples that fail to be uploaded on MapD system because of quotes and exceeding maximum text length (32767 characters) in text fields. Missing tuples occur in the following relations:

- `aka_name` (Accepted: 901311 records, Rejected: 32 missing records)
- `aka_title` (Accepted: 361469 records, Rejected: 3 missing records)
- `cast_info` (Accepted: 36238971 records, Rejected: 5373 missing records)
- `char_name` (Accepted: 3140184 records, Rejected: 155 missing records)
- `company_name` (Accepted: 234996 records, Rejected: 1 missing records)
- `movie_companies` (Accepted: 2608799 records, Rejected: 330 missing records)
- `name` (Accepted: 4167489 records, Rejected: 2 missing records)
- `title` (Accepted: 2528292 records, Rejected: 20 missing records)
- `movie_info` (Accepted: 14783645 records, Rejected: 52075 missing records)
- `person_info` (Accepted: 2760038 records, Rejected: 203626 missing records) \
  - 7 records exceeded text length

# Parser to run:
We wrote a parser code in python. Code is saved in `dataset/file_word.py`. The ouput files of this parser gives tuples that can be fully uploaded into MapD system.
