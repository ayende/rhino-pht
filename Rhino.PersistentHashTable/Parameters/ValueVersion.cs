using System;

namespace Rhino.PersistentHashTable
{
    public class ValueVersion : IComparable<ValueVersion>
    {
		/// <summary>
		/// Gets or sets the instance id for this version
		/// </summary>
		/// <value>The instance id.</value>
        public Guid InstanceId { get; set; }

		/// <summary>
		/// Gets or sets the number of this version.
		/// </summary>
		/// <value>The number.</value>
        public int Number { get; set; }


        public int CompareTo(ValueVersion other)
        {
            var instanceCompared = InstanceId.CompareTo(other.InstanceId);
            if (instanceCompared == 0)
                return Number.CompareTo(other.Number);
            return instanceCompared;
        }

        public override string ToString()
        {
            return InstanceId + "/" + Number;
        }
    }
}