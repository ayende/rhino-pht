namespace Rhino.PersistentHashTable
{
	public class RemoveItemRequest
	{
		/// <summary>
		/// Gets or sets the key of the list to remove.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }

		/// <summary>
		/// Gets or sets the id for the item to remove.
		/// </summary>
		/// <value>The id.</value>
		public int Id { get; set; }
	}
}