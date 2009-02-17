using System;

namespace Rhino.PersistentHashTable
{
    public class Value
    {
        public DateTime? ExpiresAt { get; set; }
        public DateTime Timestamp { get; set; }
        public string Key { get; set; }
        public ValueVersion Version { get; set; }
        public ValueVersion[] ParentVersions { get; set; }
        public byte[] Data { get; set; }
        public byte[] Sha256Hash { get; set; }
    }
}