using System;
using Xunit;

namespace Rhino.PersistentHashTable.Tests
{
    public class CleanupTests : PersistentTestBase
    {
        public class UncleanAndUnpleasant : MarshalByRefObject
        {
            private PersistentHashTable table;

            public void Start()
            {
                table = new PersistentHashTable("test.esent");
                table.Initialize();
            }
        }

        [Fact]
        public void Unloading_app_domain_will_free_esent()
        {
            var domain = AppDomain.CreateDomain("test",null,new AppDomainSetup
            {
                ApplicationBase = AppDomain.CurrentDomain.BaseDirectory
            });
            var unwrap = (UncleanAndUnpleasant)domain.CreateInstanceAndUnwrap(
                typeof(UncleanAndUnpleasant).Assembly.GetName().Name, 
                typeof(UncleanAndUnpleasant).FullName);
            unwrap.Start();

            AppDomain.Unload(domain);

            using(var table = new PersistentHashTable("test.esent"))
            {
                table.Initialize();
            }
        }
    }
}