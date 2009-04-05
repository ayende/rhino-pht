using System;

namespace Rhino.PersistentHashTable
{
    public class PutRequest
    {
		private ValueVersion[] parentVersions;

		/// <summary>
		/// Gets or sets the key to store this value at.
		/// </summary>
		/// <value>The key.</value>
        public string Key { get; set; }

		/// <summary>
		/// Gets or sets the replication version for this value.
		/// </summary>
		/// <value>The replication version.</value>
        public ValueVersion ReplicationVersion { get; set; }


		/// <summary>
		/// Gets or sets the parent versions for this value
		/// </summary>
		/// <value>The parent versions.</value>
        public ValueVersion[] ParentVersions
        {
            get { return parentVersions ?? new ValueVersion[0]; }
            set { parentVersions = value; }
        }

		/// <summary>
		/// Gets or sets the time this value expires at.
		/// </summary>
		/// <value>The expires at.</value>
        public DateTime? ExpiresAt { get; set; }

		/// <summary>
		/// Gets or sets the replication time stamp.
		/// </summary>
		/// <value>The replication time stamp.</value>
        public DateTime? ReplicationTimeStamp
        {
            get;
            set;
        }

		/// <summary>
		/// Gets or sets the data for this key value.
		/// </summary>
		/// <value>The bytes.</value>
        public byte[] Bytes { get; set; }

		/// <summary>
		/// Gets or sets a value indicating whether we should use optimistic concurrency when 
		/// setting this value.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if [optimistic concurrency]; otherwise, <c>false</c>.
		/// </value>
        public bool OptimisticConcurrency { get; set; }

		/// <summary>
		/// Gets or sets a value whether this value should be treated as read only.
		/// Read only values means that the <i>value</i> is read only, not that it is treated
		/// as read only by the PHT. 
		/// This means that no concurrency checks are made, and that it is safe to cache it in 
		/// many places, as long as the expiration time isn't passed.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this value should be treated as read only; otherwise, <c>false</c>.
		/// </value>
		public bool IsReadOnly { get; set; }
    }
}