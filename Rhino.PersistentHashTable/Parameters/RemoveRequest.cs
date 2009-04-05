namespace Rhino.PersistentHashTable
{
    public class RemoveRequest
    {
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
        public string Key{ get; set;}

        private ValueVersion[] parentVersions;

		/// <summary>
		/// Gets or sets the parent versions for this value.
		/// </summary>
		/// <value>The parent versions.</value>
        public ValueVersion[] ParentVersions
        {
            get { return parentVersions ?? new ValueVersion[0]; }
            set { parentVersions = value; }
        }

    }
}