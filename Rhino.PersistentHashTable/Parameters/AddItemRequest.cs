namespace Rhino.PersistentHashTable
{
	public class AddItemRequest
	{
		/// <summary>
		/// Gets or sets the key of the list to add the data to.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set;}

		/// <summary>
		/// Gets or sets the data to add to the list
		/// </summary>
		/// <value>The data.</value>
		public byte[] Data { get; set; }
	}
}