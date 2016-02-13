CREATE TABLE IF NOT EXISTS [node] 
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
	CONSTRAINT pk_node PRIMARY KEY ([id], [version])
);

CREATE TABLE IF NOT EXISTS [node_tags] 
(
	[node_id] INTEGER  NOT NULL,
	[version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [way]
(
	[id] INTEGER  NOT NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL,
	CONSTRAINT pk_way PRIMARY KEY ([id], [version])
); 

CREATE TABLE IF NOT EXISTS [way_tags] 
(
	[way_id] INTEGER  NOT NULL,
	[version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [way_nodes]
(
	[way_id] INTEGER  NOT NULL,
	[version] INTEGER  NOT NULL,
	[node_id] INTEGER  NOT NULL,
	[sequence_id] INTEGER  NOT NULL,
	PRIMARY KEY ([way_id],[node_id],[sequence_id])
);

CREATE TABLE IF NOT EXISTS [relation] 
(
	[id] INTEGER  NOT NULL,
	[changeset_id] INTEGER NULL,
	[visible] INTEGER NULL,
	[timestamp] INTEGER NULL,
	[version] INTEGER NULL,
	[usr] varchar(100) NULL,
	[usr_id] INTEGER NULL,
	CONSTRAINT pk_way PRIMARY KEY ([id], [version])
);

CREATE TABLE IF NOT EXISTS [relation_tags]
(
	[relation_id] INTEGER NOT NULL,
	[version] INTEGER  NOT NULL,
	[key] varchar(100) NOT NULL,
	[value] varchar(500) NULL
);

CREATE TABLE IF NOT EXISTS [relation_members]
(
	[relation_id] INTEGER NOT NULL,
	[version] INTEGER  NOT NULL,
	[member_type]INTEGER NOT NULL,
	[member_id] INTEGER  NOT NULL,
	[member_role] varchar(100) NULL,
	[sequence_id] INTEGER  NOT NULL,
	CONSTRAINT pk_way PRIMARY KEY ([relation_id], [sequence_id])
);

CREATE INDEX IF NOT EXISTS [idx_node_tile] ON [node] ([tile] ASC);
CREATE INDEX IF NOT EXISTS [idx_way_nodes_node] ON [way_nodes] ([node_id] ASC);
CREATE INDEX IF NOT EXISTS [idx_way_nodes_way_sequence] ON [way_nodes] ([way_id] ASC,[sequence_id] ASC);

CREATE TABLE IF NOT EXISTS [changesets] 
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