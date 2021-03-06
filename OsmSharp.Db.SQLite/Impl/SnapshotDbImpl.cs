﻿// The MIT License (MIT)

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

using System.Linq;
using System.Collections.Generic;
using OsmSharp.Db.Impl;
using OsmSharp.Db.SQLite.Schema;
using System.Data.SQLite;
using System.Data;

namespace OsmSharp.Db.SQLite.Impl
{
    /// <summary>
    /// An implementation of a snapshot db.
    /// </summary>
    class SnapshotDbImpl : ISnapshotDbImpl
    {
        private readonly string _connectionString;

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDbImpl(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Creates a snapshot db.
        /// </summary>
        public SnapshotDbImpl(SQLiteConnection connection)
        {
            _connection = connection;

            if (_connection.State == ConnectionState.Open)
            {
                if (!Schema.Tools.SnapshotDbDetect(_connection))
                {
                    throw new SQLiteException("No snapshot schema or incompatible schema detected.");
                }
            }
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

                if (!Schema.Tools.SnapshotDbDetect(_connection))
                {
                    throw new SQLiteException("No snapshot schema or incompatible schema detected.");
                }
            }
            return _connection;
        }

        /// <summary>
        /// Adds or updates osm objects in the db exactly as they are given.
        /// </summary>
        /// <remarks>
        /// - Replaces objects that already exist with the given id.
        /// </remarks>
        public void AddOrUpdate(IEnumerable<OsmGeo> osmGeos)
        {
            var connection = this.GetConnection();
            foreach (var osmGeo in osmGeos)
            {
                connection.AddOrUpdate(osmGeo);
            }
        }

        /// <summary>
        /// Clears all data.
        /// </summary>
        public void Clear()
        {
            this.GetConnection().SnapshotDbDeleteAll();
        }

        /// <summary>
        /// Deletes all objects for the given keys.
        /// </summary>
        public void Delete(IEnumerable<OsmGeoKey> keys)
        {
            var connection = this.GetConnection();

            connection.DeleteNodesById(keys.Where(x => x.Type == OsmGeoType.Node).Select(x => x.Id));
            connection.DeleteWaysById(keys.Where(x => x.Type == OsmGeoType.Way).Select(x => x.Id));
            connection.DeleteRelationsById(keys.Where(x => x.Type == OsmGeoType.Relation).Select(x => x.Id));
        }

        /// <summary>
        /// Gets all objects.
        /// </summary>
        public IEnumerable<OsmGeo> Get()
        {
            return new Streams.SnapshotDbStreamSource(this.GetConnection());
        }

        /// <summary>
        /// Gets the objects for the given keys.
        /// </summary>
        public IEnumerable<OsmGeo> Get(IEnumerable<OsmGeoKey> keys)
        {
            var connection = this.GetConnection();

            var nodes = connection.GetNodesById(keys.Where(x => x.Type == OsmGeoType.Node).Select(x => x.Id));
            var ways = connection.GetWaysById(keys.Where(x => x.Type == OsmGeoType.Way).Select(x => x.Id));
            var relations = connection.GetRelationsById(keys.Where(x => x.Type == OsmGeoType.Relation).Select(x => x.Id));

            return nodes.Cast<OsmGeo>().Concat(
                ways.Cast<OsmGeo>().Concat(
                relations.Cast<OsmGeo>()));
        }

        /// <summary>
        /// Gets all objects within the given bounding box.
        /// </summary>
        public IEnumerable<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            var connection = this.GetConnection();

            // get node ids in the bounding box.
            var nodeIdsInBox = connection.GetNodeIdsInBox(minLatitude, minLongitude, maxLatitude, maxLongitude);
            // get the way ids for the nodes in the bounding box.
            var wayIds = connection.GetWayIdsForNodes(nodeIdsInBox);
            // get the relation ids for the nodes/ways found.
            var relationIds = connection.GetRelationIdsForMembers(nodeIdsInBox, wayIds);

            // make sure the ways are complete by enumerating the missing nodes.
            var ways = connection.GetWaysById(wayIds);
            var missingNodeIds = new HashSet<long>();
            foreach(var way in ways)
            {
                if (way.Nodes != null)
                {
                    foreach(var node in way.Nodes)
                    {
                        if (!nodeIdsInBox.Contains(node) &&
                            !missingNodeIds.Contains(node))
                        {
                            missingNodeIds.Add(node);
                        }
                    }
                }
            }
            
            // get the actual nodes.
            var nodes = connection.GetNodesById(nodeIdsInBox);
            var missingNodes = connection.GetNodesById(missingNodeIds);

            // get the actual relations.
            var relations = connection.GetRelationsById(relationIds);

            return new Enumerators.MergedEnumerable(
                nodes, missingNodes, ways, relations);
        }

        /// <summary>
        /// Gets the ways for the given nodes.
        /// </summary>
        public IEnumerable<Way> GetWays(IEnumerable<long> ids)
        {
            var connection = this.GetConnection();

            var wayIds = connection.GetWayIdsForNodes(ids);
            return connection.GetWaysById(wayIds);
        }

        /// <summary>
        /// Gets the relations for the given members.
        /// </summary>
        public IEnumerable<Relation> GetRelations(IEnumerable<OsmGeoKey> keys)
        {
            var connection = this.GetConnection();

            var relationIds = connection.GetRelationIdsForMembers(keys);
            return connection.GetRelationsById(relationIds);
        }
    }
}