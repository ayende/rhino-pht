using System;

namespace Rhino.PersistentHashTable
{
	public class RemoveRequest
	{
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }
		
		public ValueVersion SpecificVersion{ get; set;}
    }
}