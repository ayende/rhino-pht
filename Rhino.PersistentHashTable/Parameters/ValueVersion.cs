using System;

namespace Rhino.PersistentHashTable
{
    public class ValueVersion : IComparable<ValueVersion>
    {
        public Guid InstanceId { get; set; }
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
            return InstanceId + "~@~" + Number;
        }
    }
}