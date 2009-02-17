namespace Rhino.PersistentHashTable
{
    public class RemoveRequest
    {
        public string Key{ get; set;}
        private ValueVersion[] parentVersions;

        public ValueVersion[] ParentVersions
        {
            get { return parentVersions ?? new ValueVersion[0]; }
            set { parentVersions = value; }
        }

    }
}