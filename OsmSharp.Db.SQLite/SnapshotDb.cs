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

using System;
using System.Collections.Generic;
using System.Text;
using OsmSharp.Streams;
using OsmSharp.Changesets;
using System.Data.SQLite;
using System.Data;

namespace OsmSharp.Db.SQLite
{
    /// <summary>
    /// Implements a snapshot db storing a snapshot of OSM-data in an SQL-server database.
    /// </summary>
    public class SnapshotDb : ISnapshotDb, IDisposable
    {
        private readonly string _connectionString; // Holds the connection string.
        private readonly bool _createAndDetectSchema; // Flag that indicates if the schema needs to be created if not present.

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(string connectionString)
        {
            _connectionString = connectionString;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(SQLiteConnection connection)
        {
            _connection = connection;
            _createAndDetectSchema = false;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(SQLiteConnection connection, bool createSchema)
        {
            _connection = connection;
            _createAndDetectSchema = createSchema;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDb(string connectionString, bool createSchema)
        {
            _connectionString = connectionString;
            _createAndDetectSchema = createSchema;
        }

        private SQLiteConnection _connection; // Holds the connection to the SQLServer db.

        /// <summary>
        /// Gets the connection.
        /// </summary>
        private SQLiteConnection GetConnection()
        {
            if (_connection == null)
            {
                _connection = new SQLiteConnection(_connectionString);
            }
            if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();

                Schema.Tools.SnapshotDbCreateAndDetect(_connection);
            }
            return _connection;
        }

        /// <summary>
        /// Gets the sql command.
        /// </summary>
        private SQLiteCommand GetCommand(string sql)
        {
            return new SQLiteCommand(sql, this.GetConnection());
        }

        /// <summary>
        /// Gets all the objects in the form of an osm stream source.
        /// </summary>
        public OsmStreamSource Get()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns all the objects within a given bounding box and filtered by a given filter.
        /// </summary>
        public IList<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            Schema.Tools.SnapshotDbDropSchema(this.GetConnection());
        }

        /// <summary>
        /// Adds or updates the given osm object in the db exactly as given.
        /// </summary>
        /// <remarks>
        /// - Replaces objects that already exist with the given id.
        /// </remarks>
        public void AddOrUpdate(OsmGeo osmGeo)
        {
            if (osmGeo == null) throw new ArgumentNullException();
            if (!osmGeo.Id.HasValue) throw new ArgumentException("Objects without an id cannot be added!");

            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    this.AddOrUpdateNode(osmGeo as Node);
                    break;
                case OsmGeoType.Way:
                    this.AddOrUpdateWay(osmGeo as Way);
                    break;
                case OsmGeoType.Relation:
                    this.AddOrUpdateRelation(osmGeo as Relation);
                    break;
            }
        }

        /// <summary>
        /// Adds or updates osm objects in the db exactly as they are given.
        /// </summary>
        /// <remarks>
        /// - Replaces objects that already exist with the given id.
        /// </remarks>
        public void AddOrUpdate(IEnumerable<OsmGeo> osmGeos)
        {
            foreach (var osmGeo in osmGeos)
            {
                this.AddOrUpdate(osmGeos);
            }
        }

        /// <summary>
        /// Gets an osm object of the given type and the given id.
        /// </summary>
        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    return this.GetNode(id);
                case OsmGeoType.Way:
                    return this.GetWay(id);
                case OsmGeoType.Relation:
                    return this.GetRelation(id);
            }
            return null;
        }

        /// <summary>
        /// Gets all osm objects with the given types and the given id's.
        /// </summary>
        public IList<OsmGeo> Get(IList<OsmGeoType> type, IList<long> id)
        {
            if (type == null) { throw new ArgumentNullException("type"); }
            if (id == null) { throw new ArgumentNullException("id"); }
            if (id.Count != type.Count) { throw new ArgumentException("Type and id lists need to have the same size."); }

            var result = new List<OsmGeo>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Get(type[i], id[i]));
            }
            return result;
        }

        /// <summary>
        /// Deletes the osm object with the given type, the given id without applying a changeset.
        /// </summary>
        public bool Delete(OsmGeoType type, long id)
        {
            SQLiteCommand command = null;
            switch (type)
            {
                case OsmGeoType.Node:
                    command = this.GetCommand(
                        string.Format("DELETE FROM node_tag WHERE (node_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM node WHERE (id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Way:
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_tags WHERE (way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way_nodes WHERE (way_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM way WHERE (id IN ({0})",
                            id.ToInvariantString()));
                    break;
                case OsmGeoType.Relation:
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_tags WHERE (relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation_members WHERE (relation_id IN ({0})",
                            id.ToInvariantString()));
                    command.ExecuteNonQuery();
                    command = this.GetCommand(
                        string.Format("DELETE FROM relation WHERE (id IN ({0})",
                            id.ToInvariantString()));
                    break;
            }
            return command.ExecuteNonQuery() > 0;
        }

        /// <summary>
        /// Deletes all osm objects with the given types and the given id's.
        /// </summary>
        public IList<bool> Delete(IList<OsmGeoType> type, IList<long> id)
        {
            if (type == null) { throw new ArgumentNullException("type"); }
            if (id == null) { throw new ArgumentNullException("id"); }
            if (id.Count != type.Count) { throw new ArgumentException("Type and id lists need to have the same size."); }

            var result = new List<bool>();
            for (int i = 0; i < id.Count; i++)
            {
                result.Add(this.Delete(type[i], id[i]));
            }
            return result;
        }

        /// <summary>
        /// Applies the given changeset.
        /// </summary>
        /// <param name="changeset">The changeset to apply.</param>
        /// <param name="bestEffort">When false, it's the entire changeset or nothing. When true the changeset is applied using best-effort.</param>
        /// <returns>The diff result result object containing the diff result and status information.</returns>
        public DiffResultResult ApplyChangeset(OsmChange changeset, bool bestEffort = false)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Disposes of all resources associated with this db.
        /// </summary>
        public void Dispose()
        {
            _connection.Dispose();
        }

        /// <summary>
        /// Adds or updates a node.
        /// </summary>
        private void AddOrUpdateNode(Node node)
        {
            var cmd = this.GetCommand("delete from node_tags where node_id = :node_id");
            cmd.Parameters.AddWithValue("node_id", node.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("update node set latitude=:latitude, longitude=:longitude, changeset_id=:changeset_id, " +
                "visible=:visible, timestamp=:timestamp, tile=:tile, version=:version, usr=:usr, usr_id=:usr_id " +
                "where id=:id");
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("tile", TileCalculations.xy2tile((uint)TileCalculations.lon2x(node.Longitude.Value),
                (uint)TileCalculations.lat2y(node.Latitude.Value)));
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.Parameters.AddWithValue("usr", node.UserName);
            cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);
            cmd.Parameters.AddWithValue("id", node.Id.Value);

            if (cmd.ExecuteNonQuery() == 0)
            { // oeps, node did not exist, insert.
                cmd = this.GetCommand(
                    "insert into node (id, latitude, longitude, changeset_id, visible, timestamp, tile, version, usr, usr_id) " +
                             "values(:id, :latitude, :longitude, :changeset_id, :visible, :timestamp, :tile, :version, :usr, :usr_id)");
                cmd.Parameters.AddWithValue("id", node.Id.Value);
                cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
                cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
                cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", node.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("tile", TileCalculations.xy2tile((uint)TileCalculations.lon2x(node.Longitude.Value),
                    (uint)TileCalculations.lat2y(node.Latitude.Value)));
                cmd.Parameters.AddWithValue("version", node.Version.Value);
                cmd.Parameters.AddWithValue("usr", node.UserName);
                cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);

                cmd.ExecuteNonQuery();
            }

            if (node.Tags != null)
            {
                cmd = this.GetCommand("INSERT into node_tags (node_id, key, value) VALUES (:node_id, :key, :value)");
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                cmd.Parameters.Add("key", DbType.String);
                cmd.Parameters.Add("value", DbType.String);
                foreach (var tag in node.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds or updates a way.
        /// </summary>
        private void AddOrUpdateWay(Way way)
        {
            var cmd = this.GetCommand("delete from way_tags where way_id = :way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from way_nodes where way_id = :way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update way set " +
                "changeset_id=:changeset_id, " +
                "visible=:visible, timestamp=:timestamp, " +
                "version=:version, usr=:usr, usr_id=:usr_id " +
                "where id=:id");
            cmd.Parameters.AddWithValue("id", way.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", way.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("usr", way.UserName);
            cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO way (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(:id, :changeset_id, :visible, :timestamp, :version, :usr, :usr_id)");
                cmd.Parameters.AddWithValue("id", way.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", way.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", way.Version.Value);
                cmd.Parameters.AddWithValue("usr", way.UserName);
                cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

                cmd.ExecuteNonQuery();
            }
            
            if (way.Tags != null)
            {
                cmd = this.GetCommand("INSERT into way_tags (way_id, key, value) VALUES (:way_id, :key, :value)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("key", DbType.String);
                cmd.Parameters.Add("value", DbType.String);
                foreach (var tag in way.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (way.Nodes != null)
            {
                cmd = this.GetCommand("INSERT into way_nodes (way_id, node_id, sequence_id) VALUES (:way_id, :node_id, :sequence_id)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("node_id", DbType.Int64);
                cmd.Parameters.Add("sequence_id", DbType.Int64);
                for (var i = 0; i < way.Nodes.Length; i++)
                {
                    cmd.Parameters["node_id"].Value = way.Nodes[i];
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds or updates a relation.
        /// </summary>
        private void AddOrUpdateRelation(Relation relation)
        {
            var cmd = this.GetCommand("delete from relation_tags where relation_id = :relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand("delete from relation_members where relation_id = :relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = this.GetCommand(
                "update relation set " +
                "changeset_id=:changeset_id, " +
                "visible=:visible, timestamp=:timestamp, " +
                "version=:version, usr=:usr, usr_id=:usr_id " +
                "where id=:id");
            cmd.Parameters.AddWithValue("id", relation.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", relation.Version.Value);
            cmd.Parameters.AddWithValue("usr", relation.UserName);
            cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = this.GetCommand(
                    "INSERT INTO relation (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(:id, :changeset_id, :visible, :timestamp, :version, :usr, :usr_id)");
                cmd.Parameters.AddWithValue("id", relation.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", relation.Version.Value);
                cmd.Parameters.AddWithValue("usr", relation.UserName);
                cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

                cmd.ExecuteNonQuery();
            }

            if (relation.Tags != null)
            {
                cmd = this.GetCommand("INSERT into relation_tags (relation_id, key, value) VALUES (:relation_id, :key, :value)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("key", DbType.String);
                cmd.Parameters.Add("value", DbType.String);
                foreach (var tag in relation.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (relation.Members != null)
            {
                cmd = this.GetCommand("INSERT into relation_members (relation_id, member_id, member_type, member_role, sequence_id) VALUES (:relation_id, :member_id, :member_type, :member_role, :sequence_id)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("member_id", DbType.Int64);
                cmd.Parameters.Add("member_type", DbType.Int32);
                cmd.Parameters.Add("member_role", DbType.String);
                cmd.Parameters.Add("sequence_id", DbType.Int32);
                for (var i = 0; i < relation.Members.Length; i++)
                {
                    cmd.Parameters["member_id"].Value = relation.Members[i].Id;
                    cmd.Parameters["member_role"].Value = relation.Members[i].Role;
                    cmd.Parameters["member_type"].Value = (int)relation.Members[i].Type;
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Gets the node with the given id.
        /// </summary>
        private Node GetNode(long id)
        {
            var command = this.GetCommand("SELECT * FROM node WHERE id = :id");
            command.Parameters.AddWithValue("id", id);

            Node node = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    node = reader.BuildNode();
                }
            }
            if (node != null)
            {
                command = this.GetCommand("SELECT * FROM node_tags WHERE way_id = :id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddTags(node);
                    }
                }
            }
            return node;
        }

        /// <summary>
        /// Gets the way with the given id.
        /// </summary>
        private Way GetWay(long id)
        {
            var command = this.GetCommand("SELECT * FROM way WHERE id = :id");
            command.Parameters.AddWithValue("id", id);

            Way way = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    way = reader.BuildWay();
                }
            }
            if (way != null)
            {
                command = this.GetCommand("SELECT * FROM way_nodes WHERE way_id = :id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddNodes(way);
                    }
                }

                command = this.GetCommand("SELECT * FROM way_tags WHERE way_id = :id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddTags(way);
                    }
                }
            }
            return way;
        }

        /// <summary>
        /// Gets the relation with the given id.
        /// </summary>
        private Relation GetRelation(long id)
        {
            var command = this.GetCommand("SELECT * FROM relation WHERE id = :id");
            command.Parameters.AddWithValue("id", id);

            Relation relation = null;
            using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
            {
                if (reader.Read())
                {
                    relation = reader.BuildRelation();
                }
            }
            if (relation != null)
            {
                command = this.GetCommand("SELECT * FROM relation_members WHERE relation_id = :id ORDER BY sequence_id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddMembers(relation);
                    }
                }

                command = this.GetCommand("SELECT * FROM relation_tags WHERE relation_id = :id");
                command.Parameters.AddWithValue("id", id);

                using (var reader = new DbDataReaderWrapper(command.ExecuteReader()))
                {
                    if (reader.Read())
                    {
                        reader.AddTags(relation);
                    }
                }
            }
            return relation;
        }
    }
}
