
########
authority_info
id	integer		not null	nextval('authority_info_id_seq'::regclass)
username	character varying(32)		not null
password	character varying(64)		not null
ip	character varying(32)
host	character varying(32)
port	character varying(32)
namespace	character varying(32)
database_name	character varying(32)		not null
table_name	character varying(32)		not null
power	character varying(32)		not null
########
authority_info_id_seq
integer	1	1	2147483647	1	no	1