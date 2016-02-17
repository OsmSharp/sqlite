// The MIT License (MIT)

// Copyright (c) 2016 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

using System.Data.SQLite;
using System;
using OsmSharp.Streams;
using System.Data;

namespace OsmSharp.Db.SQLite.Streams
{
	/// <summary>
	/// An osm stream to write data to a SnapshotDb.
	/// </summary>
	public class SnapshotDbStreamTarget : OsmStreamTarget
	{
	    private SQLiteConnection _connection;
		private readonly string _connectionString;
		private SQLiteCommand _insertNodeCmd;
		private SQLiteCommand _insertNodeTagsCmd;
		private SQLiteCommand _insertWayCmd;
		private SQLiteCommand _insertWayTagsCmd;
		private SQLiteCommand _insertWayNodesCmd;
		private SQLiteCommand _insertRelationCmd;
		private SQLiteCommand _insertRelationTagsCmd;
		private SQLiteCommand _insertRelationMembersCmd;

        /// <summary>
        /// Creates a new SQLite target.
        /// </summary>
        public SnapshotDbStreamTarget(SQLiteConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Creates a new SQLite target.
        /// </summary>
        public SnapshotDbStreamTarget(string connectionString)
		{
			_connectionString = connectionString;
		}

        /// <summary>
        /// Initializes this target.
        /// </summary>
		public override void Initialize()
		{
            if (_connection == null)
            {
                _connection = new SQLiteConnection(_connectionString);
            }
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }

            Schema.Tools.SnapshotDbCreateAndDetect(_connection);

			_insertNodeCmd = _connection.CreateCommand();
			_insertNodeCmd.Transaction = _connection.BeginTransaction();
			_insertNodeCmd.CommandText = @"INSERT OR REPLACE INTO node (id,latitude,longitude,changeset_id,visible,timestamp,tile,version,usr,usr_id) VALUES (:id,:latitude,:longitude,:changeset_id,:visible,:timestamp,:tile,:version,:usr,:usr_id);";
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"latitude", DbType.Int64));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"longitude", DbType.Int64));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"changeset_id", DbType.Int64));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"visible", DbType.Int64));
            _insertNodeCmd.Parameters.Add(new SQLiteParameter(@"timestamp", DbType.Int64)); // date stored as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"tile", DbType.Int64));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"version", DbType.Int32));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"usr", DbType.String));
			_insertNodeCmd.Parameters.Add(new SQLiteParameter(@"usr_id", DbType.Int64));

			_insertNodeTagsCmd = _connection.CreateCommand();
			_insertNodeTagsCmd.Transaction = _insertNodeCmd.Transaction;
			_insertNodeTagsCmd.CommandText = @"INSERT OR REPLACE INTO node_tags (node_id,key,value) VALUES (:node_id,:key,:value);";
			_insertNodeTagsCmd.Parameters.Add(new SQLiteParameter(@"node_id", DbType.Int64));
			_insertNodeTagsCmd.Parameters.Add(new SQLiteParameter(@"key", DbType.String));
			_insertNodeTagsCmd.Parameters.Add(new SQLiteParameter(@"value", DbType.String));

			_insertWayCmd = _connection.CreateCommand();
			_insertWayCmd.Transaction = _connection.BeginTransaction();
			_insertWayCmd.CommandText = @"INSERT OR REPLACE INTO way (id,changeset_id,visible,timestamp,version,usr,usr_id) VALUES (:id,:changeset_id,:visible,:timestamp,:version,:usr,:usr_id);";
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"changeset_id", DbType.Int64));
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"visible", DbType.Int64));
            _insertWayCmd.Parameters.Add(new SQLiteParameter(@"timestamp", DbType.Int64)); // date stored as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"version", DbType.Int32));
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"usr", DbType.String));
			_insertWayCmd.Parameters.Add(new SQLiteParameter(@"usr_id", DbType.Int64));

			_insertWayTagsCmd = _connection.CreateCommand();
			_insertWayTagsCmd.Transaction = _insertWayCmd.Transaction;
			_insertWayTagsCmd.CommandText = @"INSERT OR REPLACE INTO way_tags (way_id,key,value) VALUES (:way_id,:key,:value);";
			_insertWayTagsCmd.Parameters.Add(new SQLiteParameter(@"way_id", DbType.Int64));
			_insertWayTagsCmd.Parameters.Add(new SQLiteParameter(@"key", DbType.String));
			_insertWayTagsCmd.Parameters.Add(new SQLiteParameter(@"value", DbType.String));

			_insertWayNodesCmd = _connection.CreateCommand();
			_insertWayNodesCmd.Transaction = _insertWayCmd.Transaction;
			_insertWayNodesCmd.CommandText = @"INSERT OR REPLACE INTO way_nodes (way_id,node_id,sequence_id) VALUES (:way_id,:node_id,:sequence_id);";
			_insertWayNodesCmd.Parameters.Add(new SQLiteParameter(@"way_id", DbType.Int64));
			_insertWayNodesCmd.Parameters.Add(new SQLiteParameter(@"node_id", DbType.Int64));
			_insertWayNodesCmd.Parameters.Add(new SQLiteParameter(@"sequence_id", DbType.Int64));

			_insertRelationCmd = _connection.CreateCommand();
			_insertRelationCmd.Transaction = _connection.BeginTransaction();
			_insertRelationCmd.CommandText = @"INSERT OR REPLACE INTO relation (id,changeset_id,visible,timestamp,version,usr,usr_id) VALUES (:id,:changeset_id,:visible,:timestamp,:version,:usr,:usr_id);";
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"changeset_id", DbType.Int64));
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"visible", DbType.Int64));
            _insertRelationCmd.Parameters.Add(new SQLiteParameter(@"timestamp", DbType.Int64)); // date stored as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"version", DbType.Int32));
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"usr", DbType.String));
			_insertRelationCmd.Parameters.Add(new SQLiteParameter(@"usr_id", DbType.Int64));

			_insertRelationTagsCmd = _connection.CreateCommand();
			_insertRelationTagsCmd.Transaction = _insertRelationCmd.Transaction;
			_insertRelationTagsCmd.CommandText = @"INSERT OR REPLACE INTO relation_tags (relation_id,key,value) VALUES (:relation_id,:key,:value);";
			_insertRelationTagsCmd.Parameters.Add(new SQLiteParameter(@"relation_id", DbType.Int64));
			_insertRelationTagsCmd.Parameters.Add(new SQLiteParameter(@"key", DbType.String));
			_insertRelationTagsCmd.Parameters.Add(new SQLiteParameter(@"value", DbType.String));

			_insertRelationMembersCmd = _connection.CreateCommand();
			_insertRelationMembersCmd.Transaction = _insertRelationCmd.Transaction;
			_insertRelationMembersCmd.CommandText = @"INSERT OR REPLACE INTO relation_members (relation_id,member_type,member_id,member_role,sequence_id) VALUES (:relation_id,:member_type,:member_id,:member_role,:sequence_id);";
			_insertRelationMembersCmd.Parameters.Add(new SQLiteParameter(@"relation_id", DbType.Int64));
            _insertRelationMembersCmd.Parameters.Add(new SQLiteParameter(@"member_type", DbType.Int32));
			_insertRelationMembersCmd.Parameters.Add(new SQLiteParameter(@"member_id", DbType.Int64));
            _insertRelationMembersCmd.Parameters.Add(new SQLiteParameter(@"member_role", DbType.String));
			_insertRelationMembersCmd.Parameters.Add(new SQLiteParameter(@"sequence_id", DbType.Int64));
		}

	    /// <summary>
	    /// Adds a node to this target.
	    /// </summary>
	    /// <param name="node"></param>
	    public override void AddNode(Node node)
	    {
	        if (!node.Latitude.HasValue || !node.Longitude.HasValue)
	        {
	            // cannot insert nodes without lat/lon.
	            throw new ArgumentOutOfRangeException("node", "Cannot insert nodes without lat/lon.");
	        }

            // insert the node.
	        _insertNodeCmd.Parameters[0].Value = node.Id;
	        _insertNodeCmd.Parameters[1].Value =
	            (node.Latitude.HasValue ? (int) (node.Latitude.GetValueOrDefault()*10000000.0) : (int?) null)
	                .ConvertToDBValue();
	        _insertNodeCmd.Parameters[2].Value =
                (node.Longitude.HasValue ? (int)(node.Longitude.GetValueOrDefault() * 10000000.0) : (int?)null)
	                .ConvertToDBValue();
	        _insertNodeCmd.Parameters[3].Value = node.ChangeSetId.ConvertToDBValue();
            _insertNodeCmd.Parameters[4].Value = node.Visible.ConvertToDBValue();
	        _insertNodeCmd.Parameters[5].Value = this.ConvertDateTime(node.TimeStamp);
            _insertNodeCmd.Parameters[6].Value = Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id;

            _insertNodeCmd.Parameters[7].Value = node.Version.ConvertToDBValue();
	        _insertNodeCmd.Parameters[8].Value = node.UserName;
	        _insertNodeCmd.Parameters[9].Value = node.UserId.ConvertToDBValue();
	        _insertNodeCmd.ExecuteNonQuery();

            // insert the tags.
	        if (node.Tags != null)
	        {
	            foreach (var tag in node.Tags)
	            {
	                _insertNodeTagsCmd.Parameters[0].Value = node.Id;
	                _insertNodeTagsCmd.Parameters[1].Value = tag.Key;
	                _insertNodeTagsCmd.Parameters[2].Value = tag.Value;
	                _insertNodeTagsCmd.ExecuteNonQuery();
	            }
	        }
	    }

	    /// <summary>
        /// Adds a way to this target.
        /// </summary>
		public override void AddWay(Way way)
		{
			long? id = way.Id;
			bool? visible = way.Visible;
			_insertWayCmd.Parameters[0].Value = id.ConvertToDBValue();
			_insertWayCmd.Parameters[1].Value = way.ChangeSetId.ConvertToDBValue();
            _insertWayCmd.Parameters[2].Value = visible.ConvertToDBValue();
			_insertWayCmd.Parameters[3].Value = this.ConvertDateTime(way.TimeStamp);
			_insertWayCmd.Parameters[4].Value = way.Version.ConvertToDBValue();
			_insertWayCmd.Parameters[5].Value = way.UserName;
			_insertWayCmd.Parameters[6].Value = way.UserId.ConvertToDBValue();
			_insertWayCmd.ExecuteNonQuery();
			if (way.Tags != null)
			{
				foreach (var tag in way.Tags)
				{
					var key = tag.Key;
					if (!string.IsNullOrEmpty(key))
					{
						_insertWayTagsCmd.Parameters[0].Value = id;
						_insertWayTagsCmd.Parameters[1].Value = key;
						_insertWayTagsCmd.Parameters[2].Value = tag.Value;
						_insertWayTagsCmd.ExecuteNonQuery();
					}
				}
			}
			if (way.Nodes != null)
			{
				for (int n = 0; n < way.Nodes.Length; n++)
				{
					_insertWayNodesCmd.Parameters[0].Value = id;
					_insertWayNodesCmd.Parameters[1].Value = way.Nodes[n];
					_insertWayNodesCmd.Parameters[2].Value = n;
					_insertWayNodesCmd.ExecuteNonQuery();
				}
			}
		}

        /// <summary>
        /// Adds a relation to this target.
        /// </summary>
        /// <param name="relation"></param>
		public override void AddRelation(Relation relation)
		{
			long? id = relation.Id;
			bool? visible = relation.Visible;
			_insertRelationCmd.Parameters[0].Value = id.ConvertToDBValue();
			_insertRelationCmd.Parameters[1].Value = relation.ChangeSetId.ConvertToDBValue();
            _insertRelationCmd.Parameters[2].Value = visible.ConvertToDBValue();
            _insertRelationCmd.Parameters[3].Value = this.ConvertDateTime(relation.TimeStamp);
			_insertRelationCmd.Parameters[4].Value = relation.Version.ConvertToDBValue();
			_insertRelationCmd.Parameters[5].Value = relation.UserName;
			_insertRelationCmd.Parameters[6].Value = relation.UserId.ConvertToDBValue();
			_insertRelationCmd.ExecuteNonQuery();
			if (relation.Tags != null)
			{
				foreach (var tag in relation.Tags)
				{
					_insertRelationTagsCmd.Parameters[0].Value = id;
					_insertRelationTagsCmd.Parameters[1].Value = tag.Key;
					_insertRelationTagsCmd.Parameters[2].Value = tag.Value;
					_insertRelationTagsCmd.ExecuteNonQuery();
				}
			}
			if (relation.Members != null)
			{
				for (int n = 0; n < relation.Members.Length; ++n)
				{
					var simpleRelationMember = relation.Members[n];
					_insertRelationMembersCmd.Parameters[0].Value = id;
					_insertRelationMembersCmd.Parameters[1].Value = this.ConvertMemberType(simpleRelationMember.Type);
					_insertRelationMembersCmd.Parameters[2].Value = simpleRelationMember.Id;
					_insertRelationMembersCmd.Parameters[3].Value = simpleRelationMember.Role;
					_insertRelationMembersCmd.Parameters[4].Value = n;
					_insertRelationMembersCmd.ExecuteNonQuery();
				}
			}
		}

        /// <summary>
        /// Converts the member type to long.
        /// </summary>
        private int? ConvertMemberType(OsmGeoType? memberType)
        {
            if (memberType.HasValue)
            {
                return (int)memberType.Value;
            }
            return null;
        }


        /// <summary>
        /// Converts the given datetime object to Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
        /// </summary>
        private object ConvertDateTime(DateTime? dateTime)
        {
            if (dateTime.HasValue)
            {
                return dateTime.Value.ToUnixTime();
            }
            return null;
        }

        /// <summary>
        /// Closes this target.
        /// </summary>
		public override void Close()
		{
			if (_connection != null)
			{
                if (!string.IsNullOrWhiteSpace(_connectionString))
                {
                    _connection.Close();
                    _connection.Dispose();
                }
			}
			_connection = (SQLiteConnection)null;
		}
	}
}