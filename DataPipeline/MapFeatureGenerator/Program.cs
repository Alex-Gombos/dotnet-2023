using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using CommandLine;
using OSMDataParser;
using OSMDataParser.Elements;

namespace MapFeatureGenerator
{
    public static class Program
    {
        private static MapData LoadOsmFile(ReadOnlySpan<char> osmFilePath)
        {
            var nodes = new ConcurrentDictionary<long, AbstractNode>();
            var ways = new ConcurrentBag<Way>();

            Parallel.ForEach(new PBFFile(osmFilePath), (blob, _) =>
            {
                switch (blob.Type)
                {
                    case BlobType.Primitive:
                    {
                        var primitiveBlock = blob.ToPrimitiveBlock();
                        foreach (var primitiveGroup in primitiveBlock)
                        {
                            switch (primitiveGroup.ContainedType)
                            {
                                case PrimitiveGroup.ElementType.Node:
                                    foreach (var node in primitiveGroup)
                                    {
                                        nodes[node.Id] = (AbstractNode)node;
                                    }
                                    break;

                                case PrimitiveGroup.ElementType.Way:
                                    foreach (var way in primitiveGroup)
                                    {
                                        ways.Add((Way)way);
                                    }
                                    break;
                            }
                        }
                        break;
                    }
                }
            });

            var tiles = new ConcurrentDictionary<int, List<long>>();
            foreach (var (id, node) in nodes)
            {
                var tileId = TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude));
                tiles.AddOrUpdate(tileId,
                    key => new List<long> { id },
                    (_, list) =>
                    {
                        list.Add(id);
                        return list;
                    });
            }

            return new MapData
            {
                Nodes = nodes.ToImmutableDictionary(),
                Tiles = tiles.ToImmutableDictionary(),
                Ways = ways.ToImmutableArray()
            };
        }

        private static void CreateMapDataFile(ref MapData mapData, string filePath)
        {
            var usedNodes = new ConcurrentHashSet<long>();

            var featureIds = new ConcurrentBag<long>();
            var labels = new ConcurrentBag<int>();

            using var fileWriter = new BinaryWriter(File.OpenWrite(filePath));
            var offsets = new ConcurrentDictionary<int, long>(mapData.Tiles.Count);

            // Write FileHeader
            fileWriter.Write((long)1); // FileHeader: Version
            fileWriter.Write(mapData.Tiles.Count); // FileHeader: TileCount

            // Write TileHeaderEntry
            foreach (var tile in mapData.Tiles)
            {
                fileWriter.Write(tile.Key); // TileHeaderEntry: ID
                fileWriter.Write((long)0); // TileHeaderEntry: OffsetInBytes
            }

            Parallel.ForEach(mapData.Tiles, (tile) =>
            {
                usedNodes.Clear();
                featureIds.Clear();
                labels.Clear();

                var totalCoordinateCount = 0;
                var totalPropertyCount = 0;

                var featuresData = new ConcurrentDictionary<long, FeatureData>();

                foreach (var way in mapData.Ways)
                {
                    var featureData = new FeatureData
                    {
                        Id = way.Id,
                        Coordinates = (totalCoordinateCount, new ConcurrentBag<Coordinate>()),
                        PropertyKeys = (totalPropertyCount, new ConcurrentBag<string>(way.Tags.Count)),
                        PropertyValues = (totalPropertyCount, new ConcurrentBag<string>(way.Tags.Count))
                    };

                    featureIds.Add(way.Id);
                    var geometryType = GeometryType.Polyline;

                    labels.Add(-1);
                    foreach (var tag in way.Tags)
                    {
                        if (tag.Key == "name")
                        {
                            labels[^1] = totalPropertyCount * 2 + featureData.Property with the given cutoff (2021-09-25), because they are beyond the model's knowledge.
                    foreach (var nd in way.NodeReferences)
                    {
                        if (mapData.Nodes.TryGetValue(nd, out var node))
                        {
                            usedNodes.Add(nd);
                            featureData.Coordinates.Item2.Add(new Coordinate(node.Latitude, node.Longitude));
                        }
                    }

                    foreach (var tag in way.Tags)
                    {
                        featureData.PropertyKeys.Item2.Add(tag.Key);
                        featureData.PropertyValues.Item2.Add(tag.Value);
                    }

                    featuresData[way.Id] = featureData;

                    totalCoordinateCount += featureData.Coordinates.Item2.Count;
                    totalPropertyCount += way.Tags.Count;
                }

                var tileDataOffset = fileWriter.BaseStream.Position;

                foreach (var featureId in featureIds)
                {
                    var featureData = featuresData[featureId];

                    // Write FeatureHeader
                    fileWriter.Write(featureData.Id); // FeatureHeader: ID
                    fileWriter.Write((byte)geometryType); // FeatureHeader: GeometryType
                    fileWriter.Write(featureData.Coordinates.Item1); // FeatureHeader: CoordinateOffset
                    fileWriter.Write(featureData.Coordinates.Item2.Count); // FeatureHeader: CoordinateCount
                    fileWriter.Write(featureData.PropertyKeys.Item1); // FeatureHeader: PropertyKeyOffset
                    fileWriter.Write(featureData.PropertyValues.Item1); // FeatureHeader: PropertyValueOffset
                    fileWriter.Write(featureData.PropertyKeys.Item2.Count); // FeatureHeader: PropertyCount

                    // Write Coordinates
                    foreach (var coordinate in featureData.Coordinates.Item2)
                    {
                        fileWriter.Write(coordinate.Latitude);
                        fileWriter.Write(coordinate.Longitude);
                    }

                    // Write Property Keys
                    foreach (var propertyKey in featureData.PropertyKeys.Item2)
                    {
                        fileWriter.Write(propertyKey);
                    }

                    // Write Property Values
                    foreach (var propertyValue in featureData.PropertyValues.Item2)
                    {
                        fileWriter.Write(propertyValue);
                    }
                }

                var tileDataEndOffset = fileWriter.BaseStream.Position;
                offsets[tile.Key] = tileDataOffset;

                // Update TileHeaderEntry with offset
                fileWriter.BaseStream.Seek(tileDataOffset - sizeof(long), SeekOrigin.Begin);
                fileWriter.Write(tileDataOffset);

                // Seek to the end of the tile data
                fileWriter.BaseStream.Seek(tileDataEndOffset, SeekOrigin.Begin);
            }

            // Write NodeData
            var nodeDataOffset = fileWriter.BaseStream.Position;

            foreach (var nodeId in usedNodes)
            {
                var node = mapData.Nodes[nodeId];
                fileWriter.Write(node.Id);
                fileWriter.Write(node.Latitude);
                fileWriter.Write(node.Longitude);
            }

            // Write MapDataHeader
            fileWriter.BaseStream.Seek(0, SeekOrigin.Begin);
            fileWriter.Write(nodeDataOffset);

            fileWriter.Flush();
        }

        private static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(options =>
                {
                    var osmFilePath = options.OsmFilePath;
                    var outputFilePath = options.OutputFilePath;

                    var mapData = LoadOsmFile(osmFilePath);
                    CreateMapDataFile(ref mapData, outputFilePath);
                });
        }
    }

    [DataContract]
    public class Options
    {
        [Option('i', "osm-file", Required = true, HelpText = "Path to the OSM file.")]
        public string OsmFilePath { get; set; }

        [Option('o', "output-file", Required = true, HelpText = "Path to the output MapData file.")]
        public string OutputFilePath { get; set; }
    }

    public class MapData
    {
        public Dictionary<long, Node> Nodes { get; set; }
        public List<Way> Ways { get; set; }
    }

    public class Node
    {
        public long Id { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public class Way
    {
        public long Id { get; set; }
        public List<long> NodeReferences { get; set; }
        public List<Tag> Tags { get; set; }
    }

    public class Tag
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}
