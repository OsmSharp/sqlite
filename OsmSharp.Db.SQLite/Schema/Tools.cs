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

using OsmSharp.Db.SQLite.Logging;
using System.Data.SQLite;
using System.IO;
using System.Reflection;

namespace OsmSharp.Db.SQLite.Schema
{
    /// <summary>
    /// Contains schema tools.
    /// </summary>
    public static class Tools
    {
        private static Logger _logger = new Logger("Schema.Tools");

        /// <summary>
        /// Detects a snapshot db.
        /// </summary>
        public static bool SnapshotDbDetect(SQLiteConnection connection)
        {
            //check if Simple Schema table exists
            const string sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='node';";
            object res;
            using (var cmd = new SQLiteCommand(sql, connection))
            {
                res = cmd.ExecuteScalar();
            }
            return ((long)res == 1);
        }

        /// <summary>
        /// Creates/detects the snapshot db schema.
        /// </summary>
        public static void SnapshotDbCreateAndDetect(SQLiteConnection connection)
        {
            if (!SnapshotDbDetect(connection))
            {
                _logger.Log(TraceEventType.Information,
                        "Creating snapshot database schema...");
                ExecuteSQL(connection, "SnapshotDbSchemaDDL.sql");
            }
        }

        /// <summary>
        /// Drops the snapshot schema.
        /// </summary>
        public static void SnapshotDbDropSchema(SQLiteConnection connection)
        {
            _logger.Log(TraceEventType.Information, "Dropping snapshot database schema...");
            ExecuteSQL(connection, "SnapshotDbSchemaDROP.sql");
        }

        /// <summary>
        /// Deletes all data in the snapshot schema.
        /// </summary>
        public static void SnapshotDbDeleteAll(SQLiteConnection connection)
        {
            _logger.Log(TraceEventType.Information, "Delete all data in snapshot database schema...");
            ExecuteSQL(connection, "SnapshotDbSchemaDELETE.sql");
        }

        /// <summary>
        /// Detects a history db.
        /// </summary>
        public static bool HistoryDbDetect(SQLiteConnection connection)
        {
            //check if Simple Schema table exists
            const string sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='node';";
            object res;
            using (var cmd = new SQLiteCommand(sql, connection))
            {
                res = cmd.ExecuteScalar();
            }
            return ((long)res == 1);
        }

        /// <summary>
        /// Creates/detects the history db schema.
        /// </summary>
        public static void HistoryDbCreateAndDetect(SQLiteConnection connection)
        {
            //check if Simple Schema table exists
            const string sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='node';";
            object res;
            using (var cmd = new SQLiteCommand(sql, connection))
            {
                res = cmd.ExecuteScalar();
            }
            if ((long)res == 1)
            {
                return;
            }

            _logger.Log(TraceEventType.Information,
                    "Creating history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDDL.sql");
        }

        /// <summary>
        /// Deletes all data in the history schema.
        /// </summary>
        public static void HistoryDbDeleteAll(SQLiteConnection connection)
        {
            _logger.Log(TraceEventType.Information, "Deleting all data in this database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDELETE.sql");
        }

        /// <summary>
        /// Drops the history schema.
        /// </summary>
        public static void HistoryDbDropSchema(SQLiteConnection connection)
        {
            _logger.Log(TraceEventType.Information, "Dropping history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDROP.sql");
        }

        /// <summary>
        /// Executes the sql in the given resource file.
        /// </summary>
        private static void ExecuteSQL(SQLiteConnection connection, string resourceFilename)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(
                "OsmSharp.Db.SQLite.Schema." + resourceFilename))
            {
                if (stream != null)
                {
                    using (var reader = new StreamReader(stream))
                    {
                        using (var cmd = new SQLiteCommand(string.Empty, connection))
                        {
                            cmd.CommandTimeout = 1800;
                            cmd.CommandText = reader.ReadToEnd();
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
            }
        }
    }
}