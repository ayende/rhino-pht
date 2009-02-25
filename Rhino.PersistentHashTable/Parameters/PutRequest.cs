using System;

namespace Rhino.PersistentHashTable
{
    public class PutRequest
    {
        public string Key { get; set; }
        public ValueVersion ReplicationVersion { get; set; }

        private ValueVersion[] parentVersions;

        public ValueVersion[] ParentVersions
        {
            get { return parentVersions ?? new ValueVersion[0]; }
            set { parentVersions = value; }
        }

        public DateTime? ExpiresAt { get; set; }

        public DateTime? ReplicationTimeStamp
        {
            get;
            set;
        }

        public byte[] Bytes { get; set; }
        public bool OptimisticConcurrency { get; set; }
    }
}