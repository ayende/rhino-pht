using System;
using System.Linq;

namespace Rhino.DHT
{
    public static class ArrayExtension
    {
        public static T[] GetOtherElementsFromElement<T>(this T[] array , T element)
        {
            var index = Array.IndexOf(array, element);
            if (index == -1)
                return array;
            return array.Skip(index + 1).Union(array.Take(index)).ToArray();
        }

    }
}