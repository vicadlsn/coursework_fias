drop table if exists fias.level_types;
drop table if exists fias.obj_types;

create table if not exists fias.obj_types (
    id int primary key,
    level int,
    name varchar(250),
    is_active boolean
);

create table if not exists fias.level_types (
    level int primary key,
    name varchar(250),
    short_name varchar(50),
    is_active boolean
);