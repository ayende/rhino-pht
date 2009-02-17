using System;

namespace Rhino.PersistentHashTable
{
    public class GetRequest
    {
        public string Key{ get; set;}
        public ValueVersion SpecifiedVersion { get; set; }
    }
}