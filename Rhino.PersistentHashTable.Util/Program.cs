using System;
using System.IO;
using CommandLine;
using Rhino.PersistentHashTable.Actions;

namespace Rhino.PersistentHashTable.Util
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new ExecutionOptions();
            if(Parser.ParseArguments(args,options,Console.Error.WriteLine)==false)
            {
                Parser.ArgumentsUsage(typeof (ExecutionOptions));
                return;
            }

            try
            {
                switch (options.Action)
                {
                    case Actions.DumpToCsv:
                        if (Directory.Exists(options.ExportPath) == false)
                            Directory.CreateDirectory(options.ExportPath);

                        new ExportToCsv(options.DatabasePath,options.ExportPath).Execute();
                        break;
                    case Actions.ExpireData:
                        DateTime result;
                        TimeSpan expirySpan;
                        if(TimeSpan.TryParse(options.RelativeExpiry,out expirySpan))
                        {
                            result = DateTime.Now - expirySpan;
                        }
                        else if(DateTime.TryParse(options.AbsoluteExpiry,out result)==false)
                        {
                            Console.Error.WriteLine("Could not understand absolute date: '{0}' or relative '{1}'", 
                                options.AbsoluteExpiry,
                                options.RelativeExpiry);
                            return;
                        }
                        new ExpireDataFromPhtByTimestamp(options.DatabasePath, result).Excute();
                        break;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
            }
        }
    }
}
