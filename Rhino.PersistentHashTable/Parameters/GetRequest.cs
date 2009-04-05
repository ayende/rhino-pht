using System;

namespace Rhino.PersistentHashTable
{
    public class GetRequest
    {
		/// <summary>
		/// Gets or sets the key to get the value from the PHT.
		/// </summary>
		/// <value>The key.</value>
        public string Key{ get; set;}

		/// <summary>
		/// Gets or sets the specified version for this value.
		/// </summary>
		/// <value>The specified version.</value>
        public ValueVersion SpecifiedVersion { get; set; }
    }
}