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

using OsmSharp.Db.SQLite.Streams;
using OsmSharp.Db.SQLite.Test.Functional.Properties;
using System;
using System.Data.SQLite;
using System.IO;

namespace OsmSharp.Db.SQLite.Test.Functional
{
    class Program
    {
        static void Main(string[] args)
        {
            //// enable logging.
            //OsmSharp.Logging.Log.Enable();
            //OsmSharp.Logging.Log.RegisterListener(new ConsoleTraceListener());
            OsmSharp.Logging.Logger.LogAction = (origin, level, message, parameters) =>
            {
                Console.WriteLine("{0}:{1} - {2}", origin, level, message);
            };

            // download all data.
            Staging.Download.DownloadAll();

            using (var connection = new SQLiteConnection(
                Settings.Default.ConnectionString))
            {
                connection.Open();
                Schema.Tools.HistoryDbDropSchema(connection);
            }

            var dbSource = new HistoryDbStreamSource(
                Settings.Default.ConnectionString);
            foreach(var osmGeo in dbSource)
            {
                Console.WriteLine(osmGeo.ToInvariantString());
            }

            var target = new HistoryDbStreamTarget(
                    Settings.Default.ConnectionString);
            using (var stream = File.OpenRead(@"belgium-latest.osm.pbf"))
            {
                var source = new OsmSharp.Streams.PBFOsmStreamSource(stream);
                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);
                target.RegisterSource(progress);
                target.Pull();
            }
        }
    }
}
