drop table if exists fias.Street;
drop table if exists fias.Plan;
drop table if exists fias.City_Settlement;
drop table if exists fias.Area;
drop table if exists fias.Region;

create table fias.Region (
	id int primary key generated always as identity,
	object_id bigint unique,
	region_code int,
	name varchar(250),
	type_name varchar(50)
);


create table fias.Area (
	id int primary key generated always as identity,
	object_id bigint unique,
	region_code int,
	region_id int REFERENCES fias.Region(id) DEFERRABLE INITIALLY DEFERRED ,
	name varchar(250),
	type_name varchar(50)
);

create table fias.City_Settlement (
	id int primary key generated always as identity,
	object_id bigint unique,
	region_code int,
	name varchar(250),
	type_name varchar(50),
	region_id int REFERENCES  fias.Region(id) DEFERRABLE INITIALLY DEFERRED,
	area_id int REFERENCES  fias.Area(id) DEFERRABLE INITIALLY DEFERRED,
	dop_id int REFERENCES  fias.City_Settlement(id) DEFERRABLE INITIALLY DEFERRED
);


create table fias.Plan (
	id int primary key generated always as identity,
	object_id bigint unique,
	region_code int,
	name varchar(250),
	type_name varchar(50),
	city_id int REFERENCES  fias.City_Settlement(id) DEFERRABLE INITIALLY DEFERRED
);


create table fias.Street (
	id int primary key generated always as identity,
	object_id bigint unique,
	region_code int,
	name varchar(250),
	type_name varchar(50),
	plan_id int REFERENCES  fias.Plan(id) DEFERRABLE INITIALLY DEFERRED,
	city_id int REFERENCES  fias.City_Settlement(id) DEFERRABLE INITIALLY DEFERRED
);