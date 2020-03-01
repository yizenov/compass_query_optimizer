
CREATE TABLE aka_name (
	id integer NOT NULL PRIMARY KEY,
	person_id integer NOT NULL,
	name TEXT ENCODING DICT,
	imdb_index TEXT ENCODING DICT(8),
	name_pcode_cf TEXT ENCODING DICT(16),
	name_pcode_nf TEXT ENCODING DICT(16),
	surname_pcode TEXT ENCODING DICT(16),
	md5sum TEXT ENCODING DICT
);

CREATE TABLE aka_title (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	title TEXT ENCODING DICT,
	imdb_index TEXT ENCODING DICT(8),
	kind_id integer NOT NULL,
	production_year integer,
	phonetic_code TEXT ENCODING DICT(16),
	episode_of_id integer,
	season_nr integer,
	episode_nr integer,
	note TEXT ENCODING DICT,
	md5sum TEXT ENCODING DICT(32)
);

CREATE TABLE cast_info (
	id integer NOT NULL PRIMARY KEY,
	person_id integer NOT NULL,
	movie_id integer NOT NULL,
	person_role_id integer,
	note TEXT ENCODING DICT,
	nr_order integer,
	role_id integer NOT NULL
);

CREATE TABLE char_name (
	id integer NOT NULL PRIMARY KEY,
	name TEXT NOT NULL ENCODING DICT,
	imdb_index TEXT ENCODING DICT(8),
	imdb_id integer,
	name_pcode_nf TEXT ENCODING DICT(16),
	surname_pcode TEXT ENCODING DICT(16),
	md5sum TEXT ENCODING DICT(32)
);

CREATE TABLE comp_cast_type (
	id integer NOT NULL PRIMARY KEY,
	kind TEXT NOT NULL ENCODING DICT(32)
);

CREATE TABLE company_name (
	id integer NOT NULL PRIMARY KEY,
	name TEXT NOT NULL ENCODING DICT,
	country_code TEXT ENCODING DICT(8),
	imdb_id integer,
	name_pcode_nf TEXT ENCODING DICT(16),
	name_pcode_sf TEXT ENCODING DICT(16),
	md5sum TEXT ENCODING DICT(32)
);

CREATE TABLE company_type (
	id integer NOT NULL PRIMARY KEY,
	kind TEXT ENCODING DICT(32)
);

CREATE TABLE complete_cast (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer,
	subject_id integer NOT NULL,
	status_id integer NOT NULL
);

CREATE TABLE info_type (
	id integer NOT NULL PRIMARY KEY,
	info TEXT NOT NULL ENCODING DICT(32)
);

CREATE TABLE keyword (
	id integer NOT NULL PRIMARY KEY,
	keyword TEXT NOT NULL ENCODING DICT,
	phonetic_code TEXT ENCODING DICT(16)
);

CREATE TABLE kind_type (
	id integer NOT NULL PRIMARY KEY,
	kind TEXT ENCODING DICT(16)
);

CREATE TABLE link_type (
	id integer NOT NULL PRIMARY KEY,
	link TEXT NOT NULL ENCODING DICT(32)
);

CREATE TABLE movie_companies (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	company_id integer NOT NULL,
	company_type_id integer NOT NULL,
	note TEXT ENCODING DICT
);

CREATE TABLE movie_info_idx (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	info_type_id integer NOT NULL,
	info TEXT NOT NULL ENCODING DICT,
	note TEXT ENCODING DICT(8)
);

CREATE TABLE movie_keyword (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	linked_movie_id integer NOT NULL,
	link_type_id integer NOT NULL
);

CREATE TABLE name (
	id integer NOT NULL PRIMARY KEY,
	name TEXT NOT NULL ENCODING DICT,
	imdb_index TEXT ENCODING DICT(16),
	imdb_id integer,
	gender TEXT ENCODING DICT(8),
	name_pcode_cf TEXT ENCODING DICT(16),
	name_pcode_nf TEXT ENCODING DICT(16),
	surname_pcode TEXT ENCODING DICT(16),
	md5sum TEXT ENCODING DICT(32)
);

CREATE TABLE role_type (
	id integer NOT NULL PRIMARY KEY,
	role_name TEXT NOT NULL ENCODING DICT(32)
);

CREATE TABLE title (
	id integer NOT NULL PRIMARY KEY,
	title TEXT NOT NULL ENCODING DICT,
	imdb_index TEXT ENCODING DICT(8),
	kind_id integer NOT NULL,
	production_year integer,
	imdb_id integer,
	phonetic_code TEXT ENCODING DICT(16),
	episode_of_id integer,
	season_nr integer,
	episode_nr integer,
	series_years TEXT ENCODING DICT,
	md5sum TEXT ENCODING DICT(32)
);

CREATE TABLE movie_info (
	id integer NOT NULL PRIMARY KEY,
	movie_id integer NOT NULL,
	info_type_id integer NOT NULL,
	info TEXT NOT NULL ENCODING DICT,
	note TEXT ENCODING DICT
);

CREATE TABLE person_info (
	id integer NOT NULL PRIMARY KEY,
	person_id integer NOT NULL,
	info_type_id integer NOT NULL,
	info TEXT NOT NULL ENCODING DICT,
	note TEXT ENCODING DICT
);

copy aka_name from '~/imdb/aka_name.csv' with (delimiter=',', header='false');
copy aka_title from '~/imdb/aka_title.csv' with (delimiter=',', header='false');
copy cast_info from '~/imdb/cast_info.csv' with (delimiter=',', header='false');
copy char_name from '~/imdb/char_name.csv' with (delimiter=',', header='false');
copy comp_cast_type from '~/imdb/comp_cast_type.csv' with (delimiter=',', header='false');
copy company_name from '~/imdb/company_name.csv' with (delimiter=',', header='false');
copy company_type from '~/imdb/company_type.csv' with (delimiter=',', header='false');
copy complete_cast from '~/imdb/complete_cast.csv' with (delimiter=',', header='false');
copy info_type from '~/imdb/info_type.csv' with (delimiter=',', header='false');
copy keyword from '~/imdb/keyword.csv' with (delimiter=',', header='false');
copy kind_type from '~/imdb/kind_type.csv' with (delimiter=',', header='false');
copy link_type from '~/imdb/link_type.csv' with (delimiter=',', header='false');
copy movie_companies from '~/imdb/movie_companies.csv' with (delimiter=',', header='false');
copy movie_info_idx from '~/imdb/movie_info_idx.csv' with (delimiter=',', header='false');
copy movie_keyword from '~/imdb/movie_keyword.csv' with (delimiter=',', header='false');
copy movie_link from '~/imdb/movie_link.csv' with (delimiter=',', header='false');
copy name from '~/imdb/name.csv' with (delimiter=',', header='false');
copy role_type from '~/imdb/role_type.csv' with (delimiter=',', header='false');
copy title from '~/imdb/title.csv' with (delimiter=',', header='false');
copy movie_info from '~/imdb/movie_info.csv' with (delimiter=',', header='false');
copy person_info from '~/imdb/person_info.csv' with (delimiter=',', header='false');
