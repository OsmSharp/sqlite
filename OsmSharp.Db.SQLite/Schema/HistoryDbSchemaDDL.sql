CREATE TABLE IF NOT EXISTS [node] 
(
	[id] INTEGER  NOT NULL PRIMARY KEY,
	[latitude] INTEGER  NULL,
	[longitude] INTEGER NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[tile] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL
); 

CREATE TABLE IF NOT EXISTS [node_tags]
(
	[node_id] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL, 
	PRIMARY KEY ([node_id],[key])
); 

CREATE TABLE IF NOT EXISTS [way] 
(
	[id] INTEGER  NOT NULL PRIMARY KEY,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL
); 

CREATE TABLE IF NOT EXISTS [way_tags] 
(
	[way_id] INTEGER NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
); 

CREATE TABLE IF NOT EXISTS [way_nodes] 
(
	[way_id] INTEGER NOT NULL,
	[node_id] INTEGER NOT NULL,
	[sequence_id] INTEGER NOT NULL, 
	PRIMARY KEY ([way_id],[node_id],[sequence_id])
); 

CREATE TABLE IF NOT EXISTS [relation] 
(
	[id] INTEGER  NOT NULL PRIMARY KEY,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL
); 

CREATE TABLE IF NOT EXISTS [relation_tags] 
(
	[relation_id] INTEGER NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL, 
	PRIMARY KEY ([relation_id],[key])
); 

CREATE TABLE IF NOT EXISTS [relation_members] 
(
	[relation_id] INTEGER NOT NULL,
	[member_type] INTEGER NOT NULL,
	[member_id] INTEGER  NOT NULL,
	[member_role] varchar(100) NULL,
	[sequence_id] INTEGER  NOT NULL
);

CREATE INDEX IF NOT EXISTS [IDX_NODE_TILE] ON [node]([tile]  ASC); 
CREATE INDEX IF NOT EXISTS [IDX_WAY_NODES_NODE] ON [way_nodes]([node_id]  ASC); 
CREATE INDEX IF NOT EXISTS [IDX_WAY_NODES_WAY_SEQUENCE] ON [way_nodes]([way_id]  ASC,[sequence_id]  ASC); 

CREATE TABLE IF NOT EXISTS [archived_node]
(
	[id] INTEGER NOT NULL,
	[latitude] INTEGER  NULL,
	[longitude] INTEGER NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[tile] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL,
	CONSTRAINT pk_archived_node PRIMARY KEY ([id], [version])
);

CREATE TABLE IF NOT EXISTS [archived_node_tags] 
(
	[node_id] INTEGER  NOT NULL,
	[node_version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [archived_way]
(
	[id] INTEGER  NOT NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL,
	CONSTRAINT pk_archived_way PRIMARY KEY ([id])
);

CREATE TABLE IF NOT EXISTS [archived_way_tags] 
(
	[way_id] INTEGER  NOT NULL,
	[way_version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [archived_way_nodes]
(
	[way_id] INTEGER  NOT NULL,
	[way_version] INTEGER  NOT NULL,
	[node_id] INTEGER  NOT NULL,
	[sequence_id] INTEGER  NOT NULL,
	CONSTRAINT pk_archived_way_nodes PRIMARY KEY ([way_id], [way_version], [sequence_id])
);

CREATE TABLE IF NOT EXISTS [archived_relation] 
(
	[id] INTEGER  NOT NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL,
	CONSTRAINT pk_archived_way PRIMARY KEY ([id], [version])
);

CREATE TABLE IF NOT EXISTS [archived_relation_tags]
(
	[relation_id] INTEGER NOT NULL,
	[relation_version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [archived_relation_members]
(
	[relation_id] INTEGER NOT NULL,
	[relation_version] INTEGER  NOT NULL,
	[member_type]INTEGER NOT NULL,
	[member_id] INTEGER  NOT NULL,
	[member_role] varchar(100) NULL,
	[sequence_id] INTEGER  NOT NULL,
	CONSTRAINT pk_relation_members_archive PRIMARY KEY ([relation_id], [relation_version], [sequence_id])
);

CREATE TABLE IF NOT EXISTS [changeset] 
(
	[id] INTEGER NOT NULL,
	[usr_id] INTEGER NULL, 
	[created_at] INTEGER NULL, 
	[min_lat] INTEGER NULL,
	[max_lat] INTEGER null, 
	[min_lon] INTEGER NULL, 
	[max_lon] INTEGER NULL,
	[closed_at] INTEGER NULL,
	CONSTRAINT [pk_changesets] PRIMARY KEY ([id])
);

CREATE TABLE IF NOT EXISTS [changeset_tags]
(
	[changeset_id] INTEGER NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [changeset_changes]
(
	[changeset_id] INTEGER NOT NULL,
	[type] INTEGER NOT NULL,
	[osm_id] INTEGER NOT NULL,
	[osm_type] INTEGER NOT NULL,
	[osm_version] INTEGER NULL NULL
);