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

using NUnit.Framework;
using OsmSharp.Tags;
using System.Data.SQLite;

namespace OsmSharp.Db.SQLite.Test
{
    /// <summary>
    /// Contains tests for the snapshot db.
    /// </summary>
    [TestFixture]
    public class SnapshotDbTests
    {
        /// <summary>
        /// Tests adding a node.
        /// </summary>
        [Test]
        public void TestAddNode()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Node()
            {
                Id = 1,
                Latitude = 2,
                Longitude = 3,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var command = new SQLiteCommand("select * from node where id = :id", connection);
            command.Parameters.Add("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(2 * 10000000, reader.GetInt64("latitude"));
            Assert.AreEqual(3 * 10000000, reader.GetInt64("longitude"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt64("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SQLiteCommand("select * from node_tags where node_id = :id", connection);
            command.Parameters.Add("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
        }

        /// <summary>
        /// Tests adding a way.
        /// </summary>
        [Test]
        public void TestAddWay()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Way()
            {
                Id = 1,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                Nodes = new long[]
                    {
                        12,
                        23,
                        34
                    },
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var command = new SQLiteCommand("select * from way where id = :id", connection);
            command.Parameters.Add("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt64("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SQLiteCommand("select * from way_tags where way_id = :id", connection);
            command.Parameters.Add("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");

            command = new SQLiteCommand("select * from way_nodes where way_id = :id", connection);
            command.Parameters.Add("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.GetInt32("sequence_id"));
            Assert.AreEqual(12, reader.GetInt64("node_id"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32("sequence_id"));
            Assert.AreEqual(23, reader.GetInt64("node_id"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32("sequence_id"));
            Assert.AreEqual(34, reader.GetInt64("node_id"));
        }

        /// <summary>
        /// Tests adding a relation.
        /// </summary>
        [Test]
        public void TestAddRelation()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Relation()
            {
                Id = 1,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                Members = new RelationMember[]
                    {
                        new RelationMember()
                        {
                            Id = 12,
                            Role = "first",
                            Type = OsmGeoType.Node
                        },
                        new RelationMember()
                        {
                            Id = 23,
                            Role = "second",
                            Type = OsmGeoType.Way
                        },
                        new RelationMember()
                        {
                            Id = 34,
                            Role = "third",
                            Type = OsmGeoType.Relation
                        }
                    },
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var command = new SQLiteCommand("select * from relation where id = :id", connection);
            command.Parameters.Add("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt64("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SQLiteCommand("select * from relation_tags where relation_id = :id", connection);
            command.Parameters.Add("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");

            command = new SQLiteCommand("select * from relation_members where relation_id = :id", connection);
            command.Parameters.Add("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.GetInt32("sequence_id"));
            Assert.AreEqual(12, reader.GetInt64("member_id"));
            Assert.AreEqual(0, reader.GetInt64("member_type"));
            Assert.AreEqual("first", reader.GetString("member_role"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32("sequence_id"));
            Assert.AreEqual(23, reader.GetInt64("member_id"));
            Assert.AreEqual(1, reader.GetInt64("member_type"));
            Assert.AreEqual("second", reader.GetString("member_role"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32("sequence_id"));
            Assert.AreEqual(34, reader.GetInt64("member_id"));
            Assert.AreEqual(2, reader.GetInt64("member_type"));
            Assert.AreEqual("third", reader.GetString("member_role"));
        }

        /// <summary>
        /// Tests getting a node.
        /// </summary>
        [Test]
        public void TestGetNode()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Node()
            {
                Id = 1,
                Latitude = 2,
                Longitude = 3,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var node = db.GetNode(1);
            Assert.AreEqual(1, node.Id.Value);
            Assert.AreEqual(2, node.Latitude.Value);
            Assert.AreEqual(3, node.Longitude.Value);
            Assert.AreEqual(2, node.Tags.Count);
            Assert.IsTrue(node.Tags.Contains("key0", "value0"));
            Assert.IsTrue(node.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, node.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), node.TimeStamp);
            Assert.AreEqual(10, node.UserId);
            Assert.AreEqual("Ben", node.UserName);
            Assert.AreEqual(1, node.Version);
            Assert.AreEqual(true, node.Visible);
        }

        /// <summary>
        /// Tests getting a way.
        /// </summary>
        [Test]
        public void TestGetWay()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Way()
            {
                Id = 1,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                Nodes = new long[]
                    {
                        12,
                        23,
                        34
                    },
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var way = db.GetWay(1);
            Assert.AreEqual(1, way.Id.Value);
            Assert.AreEqual(2, way.Tags.Count);
            Assert.IsTrue(way.Tags.Contains("key0", "value0"));
            Assert.IsTrue(way.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, way.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), way.TimeStamp);
            Assert.AreEqual(10, way.UserId);
            Assert.AreEqual("Ben", way.UserName);
            Assert.AreEqual(1, way.Version);
            Assert.AreEqual(true, way.Visible);
            var nodes = way.Nodes;
            Assert.IsNotNull(nodes);
            Assert.AreEqual(3, nodes.Length);
            Assert.AreEqual(12, way.Nodes[0]);
            Assert.AreEqual(23, way.Nodes[1]);
            Assert.AreEqual(34, way.Nodes[2]);
        }

        /// <summary>
        /// Tests adding a relation.
        /// </summary>
        [Test]
        public void TestGetRelation()
        {
            var connection = new SQLiteConnection("Data Source=:memory:;Version=3;New=True;");
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Relation()
            {
                Id = 1,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                Members = new RelationMember[]
                    {
                        new RelationMember()
                        {
                            Id = 12,
                            Role = "first",
                            Type = OsmGeoType.Node
                        },
                        new RelationMember()
                        {
                            Id = 23,
                            Role = "second",
                            Type = OsmGeoType.Way
                        },
                        new RelationMember()
                        {
                            Id = 34,
                            Role = "third",
                            Type = OsmGeoType.Relation
                        }
                    },
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var relation = db.GetRelation(1);
            Assert.AreEqual(1, relation.Id.Value);
            Assert.AreEqual(2, relation.Tags.Count);
            Assert.IsTrue(relation.Tags.Contains("key0", "value0"));
            Assert.IsTrue(relation.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, relation.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), relation.TimeStamp);
            Assert.AreEqual(10, relation.UserId);
            Assert.AreEqual("Ben", relation.UserName);
            Assert.AreEqual(1, relation.Version);
            Assert.AreEqual(true, relation.Visible);
            var members = relation.Members;
            Assert.IsNotNull(members);
            Assert.AreEqual(3, members.Length);
            Assert.AreEqual(12, relation.Members[0].Id);
            Assert.AreEqual(OsmGeoType.Node, relation.Members[0].Type);
            Assert.AreEqual("first", relation.Members[0].Role);
            Assert.AreEqual(23, relation.Members[1].Id);
            Assert.AreEqual(OsmGeoType.Way, relation.Members[1].Type);
            Assert.AreEqual("second", relation.Members[1].Role);
            Assert.AreEqual(34, relation.Members[2].Id);
            Assert.AreEqual(OsmGeoType.Relation, relation.Members[2].Type);
            Assert.AreEqual("third", relation.Members[2].Role);
        }
    }
}