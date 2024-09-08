drop table if exists fias.addr_obj;
drop table if exists fias.hierarchy;

CREATE TABLE IF NOT EXISTS fias.addr_obj (
	region_code int,
	id bigint primary key generated always as identity,
	object_id bigint unique,
	name varchar(250),
	type_name varchar(250),
	level int,
	full_address varchar,
	full_text tsvector
);

CREATE TABLE IF NOT EXISTS fias.hierarchy (
	id bigint primary key generated always as identity,
	object_id bigint unique,
	parent_object_id bigint,
	region_code varchar(4),
	path varchar
);

create or replace procedure make_full_address(
	in region integer
)
language plpgsql
as $$
begin
	WITH RECURSIVE address_hierarchy AS (
		-- Базовый запрос: начинаем с объектов, у которых есть путь
		SELECT
			h.object_id,
			h.path,
			CONCAT(a.type_name, ' ', a.name) AS full_address,
			h.parent_object_id
		FROM
			fias.addr_obj as a
		JOIN
			fias.hierarchy h ON h.object_id = a.object_id
		WHERE
			a.region_code = region
		UNION ALL
		
		-- Рекурсивный запрос: собираем полные адреса
		SELECT
			h.object_id,
			ah.path,
			CONCAT(a.type_name, ' ', a.name, ', ', ah.full_address) AS full_address,
			h.parent_object_id
		FROM
			fias.addr_obj a
		JOIN fias.hierarchy h
			ON h.object_id = a.object_id
		JOIN
			address_hierarchy ah ON h.object_id = ah.parent_object_id
	)

	UPDATE fias.addr_obj AS ao SET 
		full_address = ah.full_address,
		full_text = to_tsvector(ah.full_address)
	FROM address_hierarchy as ah
	where ah.parent_object_id = 0;
end;
$$;
