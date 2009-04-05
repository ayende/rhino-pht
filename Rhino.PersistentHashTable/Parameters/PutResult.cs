namespace Rhino.PersistentHashTable
{
    public class PutResult
    {
		/// <summary>
		/// Gets or sets the version of the newly created value.
		/// </summary>
		/// <value>The version.</value>
        public ValueVersion Version { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether a conflict was created when setting
		/// this value.
		/// </summary>
		/// <value><c>true</c> if [conflict exists]; otherwise, <c>false</c>.</value>
        public bool ConflictExists { get; set; }
    }
}