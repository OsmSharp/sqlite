CREATE TABLE IF NOT EXISTS [node] ([id] INTEGER  NOT NULL PRIMARY KEY,[latitude] INTEGER  NULL,[longitude] INTEGER NULL,[changeset_id] INTEGER NULL,[visible] INTEGER NULL,[timestamp] INTEGER NULL,[tile] INTEGER NULL,[version] INTEGER NULL,[usr] varchar(100) NULL,[usr_id] INTEGER NULL); 
CREATE TABLE IF NOT EXISTS [node_tags] ([node_id] INTEGER  NOT NULL,[key] varchar(100) NOT NULL,[value] varchar(500) NULL, PRIMARY KEY ([node_id],[key])); 
CREATE TABLE IF NOT EXISTS [way] ([id] INTEGER  NOT NULL PRIMARY KEY,[changeset_id] INTEGER NULL,[visible] INTEGER NULL,[timestamp] INTEGER NULL,[version] INTEGER NULL,[usr] varchar(100) NULL,[usr_id] INTEGER NULL); 
CREATE TABLE IF NOT EXISTS [way_tags] ([way_id] INTEGER  NOT NULL,[key] varchar(100) NOT NULL,[value] varchar(500) NULL, PRIMARY KEY ([way_id],[key])); 
CREATE TABLE IF NOT EXISTS [way_nodes] ([way_id] INTEGER  NOT NULL,[node_id] INTEGER  NOT NULL,[sequence_id] INTEGER  NOT NULL, PRIMARY KEY ([way_id],[node_id],[sequence_id])); 
CREATE TABLE IF NOT EXISTS [relation] ([id] INTEGER  NOT NULL PRIMARY KEY,[changeset_id] INTEGER NULL,[visible] INTEGER NULL,[timestamp] INTEGER NULL,[version] INTEGER NULL,[usr] varchar(100) NULL,[usr_id] INTEGER NULL); 
CREATE TABLE IF NOT EXISTS [relation_tags] ([relation_id] INTEGER NOT NULL,[key] varchar(100) NOT NULL,[value] varchar(500) NULL, PRIMARY KEY ([relation_id],[key])); CREATE TABLE IF NOT EXISTS [relation_members] ([relation_id] INTEGER NOT NULL,[member_type]INTEGER NOT NULL,[member_id] INTEGER  NOT NULL,[member_role] varchar(100) NULL,[sequence_id] INTEGER  NOT NULL); 

CREATE INDEX IF NOT EXISTS [IDX_NODE_TILE] ON [node]([tile]  ASC); 
CREATE INDEX IF NOT EXISTS [IDX_WAY_NODES_NODE] ON [way_nodes]([node_id]  ASC); 
CREATE INDEX IF NOT EXISTS [IDX_WAY_NODES_WAY_SEQUENCE] ON [way_nodes]([way_id]  ASC,[sequence_id]  ASC); 