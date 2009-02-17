namespace Rhino.PersistentHashTable
{
    public class PutResult
    {
        public ValueVersion Version { get; set; }
        public bool ConflictExists { get; set; }
    }
}