using CommandLine;

namespace Rhino.PersistentHashTable.Util
{
    public class ExecutionOptions
    {
        [Argument(ArgumentType.Required)]
        public Actions Action;
        [Argument(ArgumentType.Required)]
        public string DatabasePath;
        [Argument(ArgumentType.AtMostOnce)]
        public string ExportPath;
        [Argument(ArgumentType.AtMostOnce)]
        public string AbsoluteExpiry;
        [Argument(ArgumentType.AtMostOnce)]
        public string RelativeExpiry;
    }
}